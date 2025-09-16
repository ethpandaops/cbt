package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrInvalidModelID is returned when model ID format is invalid
	ErrInvalidModelID = errors.New("invalid model ID format: expected database.table")
	// ErrCacheManagerUnavailable is returned when cache manager is not available
	ErrCacheManagerUnavailable = errors.New("cache manager not available")
	// ErrUnsupportedTransformationType is returned for unsupported transformation types
	ErrUnsupportedTransformationType = errors.New("unsupported transformation type")
	// ErrNotImplemented is returned for functionality not yet implemented
	ErrNotImplemented = errors.New("not implemented")
)

// CompletionRecord represents a completed transformation execution
type CompletionRecord interface {
	GetDatabase() string
	GetTable() string
}

// IncrementalCompletion represents completion of an incremental transformation
type IncrementalCompletion struct {
	Database string
	Table    string
	Position uint64
	Interval uint64
}

// GetDatabase returns the database name
func (c IncrementalCompletion) GetDatabase() string { return c.Database }

// GetTable returns the table name
func (c IncrementalCompletion) GetTable() string { return c.Table }

// ScheduledCompletion represents completion of a scheduled transformation
type ScheduledCompletion struct {
	Database  string
	Table     string
	StartTime time.Time
}

// GetDatabase returns the database name
func (c ScheduledCompletion) GetDatabase() string { return c.Database }

// GetTable returns the table name
func (c ScheduledCompletion) GetTable() string { return c.Table }

// Service defines the public interface for the type-specific admin service
type Service interface {
	// Type-specific position tracking (incremental only)
	GetLastProcessedEndPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error)
	GetNextUnprocessedPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error)
	GetLastProcessedPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error)
	GetFirstPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error)

	// Type-specific completion recording
	RecordCompletion(ctx context.Context, transformationType transformation.Type, completion transformation.CompletionRecord) error

	// Gap management (incremental only)
	GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error)
	FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]GapInfo, error)

	// Consolidation (incremental only)
	ConsolidateHistoricalData(ctx context.Context, modelID string) (int, error)

	// External bounds cache (shared across types)
	GetExternalBounds(ctx context.Context, modelID string) (*BoundsCache, error)
	SetExternalBounds(ctx context.Context, cache *BoundsCache) error

	// Admin table management
	EnsureAdminTables(ctx context.Context) error
	GetAdminTableConfig(transformationType transformation.Type) (clickhouse.AdminTableConfig, error)
}

// GapInfo represents a gap in the processed data
type GapInfo struct {
	StartPos uint64
	EndPos   uint64
}

// service implements the Service interface with type-specific admin tables
type service struct {
	log          logrus.FieldLogger
	client       clickhouse.ClientInterface
	cluster      string
	localSuffix  string
	adminConfig  clickhouse.AdminConfig
	typeRegistry *transformation.TypeRegistry
	cacheManager *CacheManager
}

// NewService creates a new type-specific admin service
func NewService(log logrus.FieldLogger, client clickhouse.ClientInterface, cluster, localSuffix string, adminConfig clickhouse.AdminConfig, redisClient *redis.Client) Service {
	cacheManager := NewCacheManager(redisClient)
	typeRegistry := transformation.NewTypeRegistry()

	return &service{
		log:          log.WithField("service", "admin"),
		client:       client,
		cluster:      cluster,
		localSuffix:  localSuffix,
		adminConfig:  adminConfig,
		typeRegistry: typeRegistry,
		cacheManager: cacheManager,
	}
}

// EnsureAdminTables creates all type-specific admin tables
func (s *service) EnsureAdminTables(ctx context.Context) error {
	for _, transformationType := range s.typeRegistry.GetSupportedTypes() {
		handler, err := s.typeRegistry.GetHandler(transformationType)
		if err != nil {
			return err
		}

		adminTableConfig := s.getAdminTableConfigForType(transformationType)
		sqlStatements := handler.GetCreateTableSQL(adminTableConfig, s.cluster)

		for _, sql := range sqlStatements {
			if err := s.client.Execute(ctx, sql); err != nil {
				s.log.WithError(err).WithFields(logrus.Fields{
					"type": transformationType,
					"sql":  sql,
				}).Error("Failed to create admin table")
				return err
			}
		}

		s.log.WithFields(logrus.Fields{
			"type":     transformationType,
			"database": adminTableConfig.Database,
			"table":    adminTableConfig.Table,
		}).Info("Admin table ensured")
	}

	return nil
}

// GetAdminTableConfig returns the admin table config for a specific transformation type
func (s *service) GetAdminTableConfig(transformationType transformation.Type) (clickhouse.AdminTableConfig, error) {
	switch transformationType {
	case transformation.TypeIncremental:
		return s.adminConfig.Incremental, nil
	case transformation.TypeScheduled:
		return s.adminConfig.Scheduled, nil
	default:
		return clickhouse.AdminTableConfig{}, ErrUnsupportedTransformationType
	}
}

// getAdminTableConfigForType is an internal helper that returns the appropriate admin table config
func (s *service) getAdminTableConfigForType(transformationType transformation.Type) clickhouse.AdminTableConfig {
	config, _ := s.GetAdminTableConfig(transformationType)
	return config
}

// RecordCompletion records completion using the appropriate type handler
func (s *service) RecordCompletion(ctx context.Context, transformationType transformation.Type, completion transformation.CompletionRecord) error {
	handler, err := s.typeRegistry.GetHandler(transformationType)
	if err != nil {
		return err
	}

	adminTableConfig := s.getAdminTableConfigForType(transformationType)
	return handler.RecordCompletion(ctx, s.client, adminTableConfig, completion)
}

// GetLastProcessedEndPosition returns the end position of the last processed interval (incremental only)
func (s *service) GetLastProcessedEndPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error) {
	if transformationType != transformation.TypeIncremental {
		return 0, ErrUnsupportedTransformationType
	}

	database, table, err := s.parseModelID(modelID)
	if err != nil {
		return 0, err
	}

	adminTableConfig := s.getAdminTableConfigForType(transformationType)

	query := fmt.Sprintf(`
		SELECT COALESCE(max(position + interval), 0) as last_end_position
		FROM %s.%s FINAL
		WHERE database = '%s' AND table = '%s'
	`, adminTableConfig.Database, adminTableConfig.Table, database, table)

	var result struct {
		LastEndPosition uint64 `json:"last_end_position,string"`
	}

	if err := s.client.QueryOne(ctx, query, &result); err != nil {
		// If the table doesn't exist or has no data, return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last processed end position: %w", err)
	}

	return result.LastEndPosition, nil
}

// GetNextUnprocessedPosition returns the next position for processing (incremental only)
func (s *service) GetNextUnprocessedPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error) {
	if transformationType != transformation.TypeIncremental {
		return 0, ErrUnsupportedTransformationType
	}

	// The next unprocessed position is the same as the last processed end position
	return s.GetLastProcessedEndPosition(ctx, transformationType, modelID)
}

// GetLastProcessedPosition returns the start position of the last processed interval (incremental only)
func (s *service) GetLastProcessedPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error) {
	if transformationType != transformation.TypeIncremental {
		return 0, ErrUnsupportedTransformationType
	}

	database, table, err := s.parseModelID(modelID)
	if err != nil {
		return 0, err
	}

	adminTableConfig := s.getAdminTableConfigForType(transformationType)

	query := fmt.Sprintf(`
		SELECT COALESCE(max(position), 0) as position
		FROM %s.%s FINAL
		WHERE database = '%s' AND table = '%s'
	`, adminTableConfig.Database, adminTableConfig.Table, database, table)

	var result struct {
		Position uint64 `json:"position,string"`
	}

	if err := s.client.QueryOne(ctx, query, &result); err != nil {
		// If the table doesn't exist or has no data, return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last processed position: %w", err)
	}

	return result.Position, nil
}

// GetFirstPosition returns the first processed position (incremental only)
func (s *service) GetFirstPosition(ctx context.Context, transformationType transformation.Type, modelID string) (uint64, error) {
	if transformationType != transformation.TypeIncremental {
		return 0, ErrUnsupportedTransformationType
	}

	database, table, err := s.parseModelID(modelID)
	if err != nil {
		return 0, err
	}

	adminTableConfig := s.getAdminTableConfigForType(transformationType)

	query := fmt.Sprintf(`
		SELECT COALESCE(min(position), 0) as first_position
		FROM %s.%s FINAL
		WHERE database = '%s' AND table = '%s'
	`, adminTableConfig.Database, adminTableConfig.Table, database, table)

	var result struct {
		FirstPosition uint64 `json:"first_position,string"`
	}

	if err := s.client.QueryOne(ctx, query, &result); err != nil {
		// If the table doesn't exist or has no data, return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get first processed position: %w", err)
	}

	return result.FirstPosition, nil
}

// GetCoverage checks if a range is covered (incremental only)
func (s *service) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	// Incremental coverage checking not yet implemented
	return false, ErrNotImplemented
}

// FindGaps finds gaps in processed data (incremental only)
func (s *service) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]GapInfo, error) {
	// Incremental gap detection not yet implemented
	return nil, ErrNotImplemented
}

// ConsolidateHistoricalData consolidates historical records (incremental only)
func (s *service) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	// Incremental data consolidation not yet implemented
	return 0, ErrNotImplemented
}

// GetExternalBounds gets external model bounds from cache
func (s *service) GetExternalBounds(ctx context.Context, modelID string) (*BoundsCache, error) {
	if s.cacheManager == nil {
		return nil, ErrCacheManagerUnavailable
	}
	return s.cacheManager.GetBounds(ctx, modelID)
}

// SetExternalBounds sets external model bounds in cache
func (s *service) SetExternalBounds(ctx context.Context, cache *BoundsCache) error {
	if s.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}
	return s.cacheManager.SetBounds(ctx, cache)
}

// parseModelID splits a model ID into database and table components
func (s *service) parseModelID(modelID string) (database, table string, err error) {
	parts := strings.SplitN(modelID, ".", 2)
	if len(parts) != 2 {
		return "", "", ErrInvalidModelID
	}
	return parts[0], parts[1], nil
}
