package admin

//go:generate go tool mockgen -package mock -destination mock/service.mock.go -source service.go Service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrCacheManagerUnavailable is returned when cache manager is not available
	ErrCacheManagerUnavailable = errors.New("cache manager not available")
)

// consolidationDeleteBatchSize bounds how many (position, interval) keys go into a
// single DELETE ... IN (...) statement. Large enough that a normal island is one
// batch — in cluster mode each batch is a distributed DDL on the shared task queue,
// so fewer is better — yet small enough that the rendered query stays well under
// ClickHouse's max_query_size even for a long-overdue island of thousands of rows.
// Each batch is still keyed, preserving the out-of-order-delete safety.
const consolidationDeleteBatchSize = 5000

// PositionTracker tracks completed incremental transformations: recording
// completions, querying processed positions, gaps, coverage, consolidation, and
// period deletion against the incremental admin table.
type PositionTracker interface {
	// Position tracking (for incremental transformations)
	GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) // Returns next position for forward fill
	GetLastProcessedPosition(ctx context.Context, modelID string) (uint64, error)   // Returns position of last record
	GetFirstPosition(ctx context.Context, modelID string) (uint64, error)
	RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error

	// Coverage and gap management
	GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error)
	GetProcessedRanges(ctx context.Context, modelID string) ([]ProcessedRange, error)
	GetAllProcessedRanges(ctx context.Context, modelIDs []string) (map[string][]ProcessedRange, error)
	FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]GapInfo, error)

	// Consolidation
	ConsolidateHistoricalData(ctx context.Context, modelID string) (uint64, error)

	// Period deletion
	DeletePeriod(ctx context.Context, modelID string, startPos, endPos uint64) (uint64, error)

	// Admin table info
	GetIncrementalAdminDatabase() string
	GetIncrementalAdminTable() string
}

// ScheduledTracker tracks completed scheduled transformations against the
// scheduled admin table.
type ScheduledTracker interface {
	RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error
	GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error)
	GetAllLastScheduledExecutions(ctx context.Context, modelIDs []string) (map[string]*time.Time, error)

	// Admin table info
	GetScheduledAdminDatabase() string
	GetScheduledAdminTable() string
}

// BoundsStore caches external model bounds and provides distributed locking for
// bounds updates. (Named BoundsStore to avoid colliding with the BoundsCache type.)
type BoundsStore interface {
	GetExternalBounds(ctx context.Context, modelID string) (*BoundsCache, error)
	SetExternalBounds(ctx context.Context, cache *BoundsCache) error
	DeleteExternalBounds(ctx context.Context, modelID string) error

	// Distributed locking for bounds updates
	AcquireBoundsLock(ctx context.Context, modelID string) (BoundsLock, error)
}

// OverrideStore manages live configuration overrides stored in the cache.
type OverrideStore interface {
	GetConfigOverride(ctx context.Context, modelID string) (*ConfigOverride, error)
	GetAllConfigOverrides(ctx context.Context) ([]ConfigOverride, error)
	SetConfigOverride(ctx context.Context, override *ConfigOverride) error
	DeleteConfigOverride(ctx context.Context, modelID string) error
	DeleteAllConfigOverrides(ctx context.Context) error
	GetConfigOverrideVersion(ctx context.Context) (int64, error)
}

// Service defines the public interface for the admin service. It composes the
// capability interfaces so existing consumers keep compiling, while narrower
// dependencies can depend on just the sub-interface they need.
type Service interface {
	PositionTracker
	ScheduledTracker
	BoundsStore
	OverrideStore
}

// GapInfo represents a gap in the processed data
type GapInfo struct {
	StartPos uint64
	EndPos   uint64
}

// ProcessedRange represents a processed range from the admin table
type ProcessedRange struct {
	Position uint64 `ch:"position"`
	Interval uint64 `ch:"interval"`
}

// service manages the admin tracking table for completed transformations
type service struct {
	log logrus.FieldLogger

	client        clickhouse.ClientInterface
	cluster       string
	localSuffix   string
	adminDatabase string
	adminTable    string

	scheduledAdminDatabase string
	scheduledAdminTable    string

	cacheManager *CacheManager
}

// TableConfig represents configuration for admin tables
type TableConfig struct {
	IncrementalDatabase string
	IncrementalTable    string
	ScheduledDatabase   string
	ScheduledTable      string
}

// NewService creates a new admin table manager with type-specific admin tables
func NewService(log logrus.FieldLogger, client clickhouse.ClientInterface, cluster, localSuffix string, config TableConfig, redisClient *redis.Client) Service {
	// Leave the cache manager nil when no Redis client is provided so the
	// nil guards on cache operations take effect instead of wrapping a nil client.
	var cacheManager *CacheManager
	if redisClient != nil {
		cacheManager = NewCacheManager(redisClient)
	}

	return &service{
		log:                    log.WithField("service", "admin"),
		client:                 client,
		cluster:                cluster,
		localSuffix:            localSuffix,
		adminDatabase:          config.IncrementalDatabase,
		adminTable:             config.IncrementalTable,
		scheduledAdminDatabase: config.ScheduledDatabase,
		scheduledAdminTable:    config.ScheduledTable,
		cacheManager:           cacheManager,
	}
}

// buildTableRef creates a backtick-escaped table reference for SQL queries.
func buildTableRef(database, table string) string {
	return fmt.Sprintf("`%s`.`%s`", database, table)
}

// buildKeyedDelete renders a DELETE for rows matching (database, table) and an
// explicit key predicate. In cluster mode it targets the local replicated table
// ON CLUSTER so the delete reaches whichever shard holds the model's rows; in
// plain mode it targets the admin table directly. keyPredicate is appended to the
// WHERE clause (e.g. "(position, interval) IN (...)" or a range condition).
func (a *service) buildKeyedDelete(database, table, keyPredicate string) string {
	if a.cluster != "" {
		return fmt.Sprintf(`
			DELETE FROM `+"`%s`.`%s%s`"+` ON CLUSTER '%s'
			WHERE database = '%s' AND table = '%s'
			  AND %s
		`, a.adminDatabase, a.adminTable, a.localSuffix, a.cluster,
			database, table, keyPredicate)
	}

	return fmt.Sprintf(`
		DELETE FROM %s
		WHERE database = '%s' AND table = '%s'
		  AND %s
	`, buildTableRef(a.adminDatabase, a.adminTable),
		database, table, keyPredicate)
}

// GetIncrementalAdminDatabase returns the incremental admin database name
func (a *service) GetIncrementalAdminDatabase() string {
	return a.adminDatabase
}

// GetIncrementalAdminTable returns the incremental admin table name
func (a *service) GetIncrementalAdminTable() string {
	return a.adminTable
}

// GetScheduledAdminDatabase returns the scheduled admin database name
func (a *service) GetScheduledAdminDatabase() string {
	return a.scheduledAdminDatabase
}

// GetScheduledAdminTable returns the scheduled admin table name
func (a *service) GetScheduledAdminTable() string {
	return a.scheduledAdminTable
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
