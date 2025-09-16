package transformation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
)

var (
	// ErrNoHandlerRegistered is returned when no handler is registered for a type
	ErrNoHandlerRegistered = errors.New("no handler registered for transformation type")
	// ErrInvalidCompletionRecord is returned when completion record type is invalid
	ErrInvalidCompletionRecord = errors.New("invalid completion record type for transformation")
)

// TypeHandler defines the interface for handling different transformation types
type TypeHandler interface {
	// ShouldTrackPosition returns true if this type tracks position in admin table
	ShouldTrackPosition() bool

	// GetTemplateVariables returns type-specific template variables
	GetTemplateVariables(ctx context.Context, config *Config, task interface{}) (map[string]interface{}, error)

	// GetEnvironmentVariables returns type-specific environment variables
	GetEnvironmentVariables(ctx context.Context, config *Config, task interface{}) (map[string]string, error)

	// GetScheduleFields returns the schedule field names this type uses
	GetScheduleFields() []string

	// Admin table management
	GetAdminTableConfig() clickhouse.AdminTableConfig
	GetCreateTableSQL(adminConfig clickhouse.AdminTableConfig, cluster string) []string
	RecordCompletion(ctx context.Context, client clickhouse.ClientInterface, adminConfig clickhouse.AdminTableConfig, completion CompletionRecord) error
}

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

// TypeRegistry manages transformation type handlers
type TypeRegistry struct {
	handlers map[Type]TypeHandler
}

// NewTypeRegistry creates a new type registry with default handlers
func NewTypeRegistry() *TypeRegistry {
	registry := &TypeRegistry{
		handlers: make(map[Type]TypeHandler),
	}

	// Register default handlers
	registry.Register(TypeIncremental, &IncrementalHandler{})
	registry.Register(TypeScheduled, &ScheduledHandler{})

	return registry
}

// Register registers a handler for a specific transformation type
func (r *TypeRegistry) Register(t Type, handler TypeHandler) {
	r.handlers[t] = handler
}

// GetHandler returns the handler for a specific transformation type
func (r *TypeRegistry) GetHandler(t Type) (TypeHandler, error) {
	handler, exists := r.handlers[t]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNoHandlerRegistered, t)
	}
	return handler, nil
}

// GetSupportedTypes returns all supported transformation types
func (r *TypeRegistry) GetSupportedTypes() []Type {
	types := make([]Type, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// IncrementalHandler handles incremental transformations
type IncrementalHandler struct{}

// ShouldTrackPosition returns true for incremental transformations
func (h *IncrementalHandler) ShouldTrackPosition() bool {
	return true
}

// GetTemplateVariables returns incremental-specific template variables
func (h *IncrementalHandler) GetTemplateVariables(_ context.Context, _ *Config, _ interface{}) (map[string]interface{}, error) {
	// This will be implemented when we refactor the template system
	return map[string]interface{}{}, nil
}

// GetEnvironmentVariables returns incremental-specific environment variables
func (h *IncrementalHandler) GetEnvironmentVariables(_ context.Context, _ *Config, _ interface{}) (map[string]string, error) {
	// This will be implemented when we refactor the template system
	return map[string]string{}, nil
}

// GetScheduleFields returns the schedule fields for incremental transformations
func (h *IncrementalHandler) GetScheduleFields() []string {
	return []string{"forwardfill", "backfill"}
}

// GetAdminTableConfig returns the admin table configuration for incremental transformations
func (h *IncrementalHandler) GetAdminTableConfig() clickhouse.AdminTableConfig {
	return clickhouse.AdminTableConfig{
		Database: "admin",
		Table:    "cbt_incremental",
	}
}

// GetCreateTableSQL returns SQL for creating incremental admin tables
func (h *IncrementalHandler) GetCreateTableSQL(adminConfig clickhouse.AdminTableConfig, cluster string) []string {
	if cluster != "" {
		return h.getClusteredTableSQL(adminConfig, cluster)
	}
	return h.getSimpleTableSQL(adminConfig)
}

func (h *IncrementalHandler) getSimpleTableSQL(adminConfig clickhouse.AdminTableConfig) []string {
	return []string{
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table, position);
`, adminConfig.Database, adminConfig.Table),
	}
}

func (h *IncrementalHandler) getClusteredTableSQL(adminConfig clickhouse.AdminTableConfig, cluster string) []string {
	return []string{
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s_local ON CLUSTER '%s' (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/%s/{database}/tables/{table}/{shard}',
    '{replica}',
    updated_date_time
)
ORDER BY (database, table, position);
`, adminConfig.Database, adminConfig.Table, cluster, cluster),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER '%s' AS %s.%s_local
ENGINE = Distributed(
    '%s',
    '%s',
    '%s_local',
    cityHash64(database, table)
);
`, adminConfig.Database, adminConfig.Table, cluster, adminConfig.Database, adminConfig.Table, cluster, adminConfig.Database, adminConfig.Table),
	}
}

// RecordCompletion records completion of an incremental transformation
func (h *IncrementalHandler) RecordCompletion(ctx context.Context, client clickhouse.ClientInterface, adminConfig clickhouse.AdminTableConfig, completion CompletionRecord) error {
	record, ok := completion.(IncrementalCompletion)
	if !ok {
		return fmt.Errorf("%w: incremental", ErrInvalidCompletionRecord)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s.%s VALUES (
			now(), '%s', '%s', %d, %d
		)`,
		adminConfig.Database, adminConfig.Table,
		record.Database, record.Table, record.Position, record.Interval)

	return client.Execute(ctx, query)
}

// ScheduledHandler handles scheduled transformations
type ScheduledHandler struct{}

// ShouldTrackPosition returns false for scheduled transformations
func (h *ScheduledHandler) ShouldTrackPosition() bool {
	return false
}

// GetTemplateVariables returns scheduled-specific template variables
func (h *ScheduledHandler) GetTemplateVariables(_ context.Context, _ *Config, _ interface{}) (map[string]interface{}, error) {
	// This will be implemented when we refactor the template system
	return map[string]interface{}{}, nil
}

// GetEnvironmentVariables returns scheduled-specific environment variables
func (h *ScheduledHandler) GetEnvironmentVariables(_ context.Context, _ *Config, _ interface{}) (map[string]string, error) {
	// This will be implemented when we refactor the template system
	return map[string]string{}, nil
}

// GetScheduleFields returns the schedule fields for scheduled transformations
func (h *ScheduledHandler) GetScheduleFields() []string {
	return []string{"schedule"}
}

// GetAdminTableConfig returns the admin table configuration for scheduled transformations
func (h *ScheduledHandler) GetAdminTableConfig() clickhouse.AdminTableConfig {
	return clickhouse.AdminTableConfig{
		Database: "admin",
		Table:    "cbt_scheduled",
	}
}

// GetCreateTableSQL returns SQL for creating scheduled admin tables
func (h *ScheduledHandler) GetCreateTableSQL(adminConfig clickhouse.AdminTableConfig, cluster string) []string {
	if cluster != "" {
		return h.getClusteredTableSQL(adminConfig, cluster)
	}
	return h.getSimpleTableSQL(adminConfig)
}

func (h *ScheduledHandler) getSimpleTableSQL(adminConfig clickhouse.AdminTableConfig) []string {
	return []string{
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    start_date_time DateTime(3) COMMENT 'The start date time of the scheduled job' CODEC(DoubleDelta, ZSTD(1)),
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table);
`, adminConfig.Database, adminConfig.Table),
	}
}

func (h *ScheduledHandler) getClusteredTableSQL(adminConfig clickhouse.AdminTableConfig, cluster string) []string {
	return []string{
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s_local ON CLUSTER '%s' (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    start_date_time DateTime(3) COMMENT 'The start date time of the scheduled job' CODEC(DoubleDelta, ZSTD(1)),
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/%s/{database}/tables/{table}/{shard}',
    '{replica}',
    updated_date_time)
ORDER BY (database, table);
`, adminConfig.Database, adminConfig.Table, cluster, cluster),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER '%s' AS %s.%s_local
ENGINE = Distributed(
    '%s',
    '%s',
    '%s_local',
    cityHash64(database, table)
);
`, adminConfig.Database, adminConfig.Table, cluster, adminConfig.Database, adminConfig.Table, cluster, adminConfig.Database, adminConfig.Table),
	}
}

// RecordCompletion records completion of a scheduled transformation
func (h *ScheduledHandler) RecordCompletion(ctx context.Context, client clickhouse.ClientInterface, adminConfig clickhouse.AdminTableConfig, completion CompletionRecord) error {
	record, ok := completion.(ScheduledCompletion)
	if !ok {
		return fmt.Errorf("%w: scheduled", ErrInvalidCompletionRecord)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s.%s VALUES (
			now(), '%s', '%s', '%s'
		)`,
		adminConfig.Database, adminConfig.Table,
		record.Database, record.Table, record.StartTime.Format("2006-01-02 15:04:05.000"))

	return client.Execute(ctx, query)
}
