//go:build integration

package testutil

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/ethpandaops/cbt/pkg/models/transformation/scheduled"
)

var registerOnce sync.Once

func init() {
	registerOnce.Do(func() {
		incremental.Register()
		scheduled.Register()
	})
}

// TransformationConfig holds configuration for creating test transformations.
type TransformationConfig struct {
	Database     string
	Table        string
	SQL          string
	Dependencies []string
	MinInterval  uint64
	MaxInterval  uint64
	IntervalType string
}

// TransformationOption is a functional option for customizing test transformations.
type TransformationOption func(*TransformationConfig)

// WithDependencies sets the dependencies for the transformation.
func WithDependencies(deps ...string) TransformationOption {
	return func(cfg *TransformationConfig) {
		cfg.Dependencies = deps
	}
}

// WithInterval sets the min and max interval for the transformation.
func WithInterval(min, max uint64) TransformationOption {
	return func(cfg *TransformationConfig) {
		cfg.MinInterval = min
		cfg.MaxInterval = max
	}
}

// WithIntervalType sets the interval type for the transformation.
func WithIntervalType(intervalType string) TransformationOption {
	return func(cfg *TransformationConfig) {
		cfg.IntervalType = intervalType
	}
}

// NewTransformationSQL creates a test transformation SQL model.
// The SQL should contain the transformation logic with template variables.
func NewTransformationSQL(database, table, sql string, opts ...TransformationOption) (models.Transformation, error) {
	cfg := &TransformationConfig{
		Database:     database,
		Table:        table,
		SQL:          sql,
		MinInterval:  0,
		MaxInterval:  100,
		IntervalType: "slot",
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Build dependencies YAML
	depsYAML := ""
	if len(cfg.Dependencies) > 0 {
		depsYAML = "dependencies:\n"
		for _, dep := range cfg.Dependencies {
			depsYAML += fmt.Sprintf("  - %s\n", dep)
		}
	} else {
		depsYAML = "dependencies:\n  - source.source_table\n"
	}

	// Build the SQL file content with YAML frontmatter
	content := fmt.Sprintf(`---
type: incremental
database: %s
table: %s
interval:
  type: %s
  min: %d
  max: %d
schedules:
  forwardfill: "*/1 * * * *"
  backfill: "*/5 * * * *"
%s---
%s`, cfg.Database, cfg.Table, cfg.IntervalType, cfg.MinInterval, cfg.MaxInterval, depsYAML, cfg.SQL)

	return transformation.NewSQL([]byte(content))
}

// ExternalConfig holds configuration for creating test external models.
type ExternalConfig struct {
	Database                string
	Table                   string
	SQL                     string
	IntervalType            string
	Lag                     uint64
	IncrementalScanInterval time.Duration
	FullScanInterval        time.Duration
}

// ExternalOption is a functional option for customizing test external models.
type ExternalOption func(*ExternalConfig)

// WithCacheIntervals sets the cache scan intervals for the external model.
func WithCacheIntervals(incremental, full time.Duration) ExternalOption {
	return func(cfg *ExternalConfig) {
		cfg.IncrementalScanInterval = incremental
		cfg.FullScanInterval = full
	}
}

// NewExternalSQL creates a test external SQL model.
// The SQL should query min/max bounds from the source table.
func NewExternalSQL(database, table, sql string, opts ...ExternalOption) (models.External, error) {
	cfg := &ExternalConfig{
		Database:                database,
		Table:                   table,
		SQL:                     sql,
		IntervalType:            "slot",
		Lag:                     0,
		IncrementalScanInterval: 1 * time.Minute,
		FullScanInterval:        5 * time.Minute,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Build the SQL file content with YAML frontmatter
	content := fmt.Sprintf(`---
database: %s
table: %s
interval:
  type: %s
lag: %d
cache:
  incremental_scan_interval: %s
  full_scan_interval: %s
---
%s`, cfg.Database, cfg.Table, cfg.IntervalType, cfg.Lag,
		cfg.IncrementalScanInterval.String(), cfg.FullScanInterval.String(), cfg.SQL)

	return external.NewSQL([]byte(content))
}

// DefaultExternalBoundsSQL returns SQL that queries min/max position from a source table.
func DefaultExternalBoundsSQL(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf(`SELECT
    min(position) as min,
    max(position) as max
FROM %s.%s`, sourceDatabase, sourceTable)
}

// DefaultTransformationSQL returns SQL that copies data from source to target.
func DefaultTransformationSQL(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf(`INSERT INTO {{ .database }}.{{ .table }} (position, value)
SELECT position, value
FROM %s.%s
WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}`, sourceDatabase, sourceTable)
}

// DAG creates a DAG with the given transformations and externals.
func DAG(transformations []models.Transformation, externals []models.External) models.DAGReader {
	dag := models.NewDependencyGraph()
	_ = dag.BuildGraph(transformations, externals)
	return dag
}

// SQL Templates for realistic test scenarios inspired by xatu-cbt production models
// Note: These templates use {{ .database }} and {{ .table }} for compatibility with the
// test models service. Production models use {{ .self.database }} and {{ .self.table }}.

// EventsAggregatedSQL returns SQL for a simple aggregation transformation.
// This pattern is similar to int_storage_slot_diff.sql from xatu-cbt.
func EventsAggregatedSQL(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf(`INSERT INTO {{ .database }}.{{ .table }}
SELECT
    now() as updated_at,
    position,
    account_id,
    count() as event_count,
    sum(value) as total_value
FROM %s.%s
WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}
GROUP BY position, account_id`, sourceDatabase, sourceTable)
}

// EventsByAccountCumulativeSQL returns SQL for cumulative state transformation.
// This pattern is similar to int_storage_slot_state_by_address.sql from xatu-cbt.
// It reads its own previous state to calculate running totals across intervals.
func EventsByAccountCumulativeSQL(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf(`INSERT INTO {{ .database }}.{{ .table }}
WITH
prev_state AS (
    SELECT account_id, argMax(running_total, position) as running_total
    FROM {{ .database }}.{{ .table }} FINAL
    WHERE position < {{ .bounds.start }}
    GROUP BY account_id
),
deltas AS (
    SELECT position, account_id, total_value as delta
    FROM %s.%s FINAL
    WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}
)
SELECT
    now() as updated_at,
    d.position as position,
    d.account_id as account_id,
    d.delta as delta,
    COALESCE(p.running_total, 0) + SUM(d.delta) OVER (
        PARTITION BY d.account_id ORDER BY d.position ROWS UNBOUNDED PRECEDING
    ) as running_total
FROM deltas d
LEFT JOIN prev_state p ON d.account_id = p.account_id`, sourceDatabase, sourceTable)
}

// EventsWithNextSQL returns SQL for calculating next position per account.
// This is a simpler version of the pattern in int_storage_slot_next_touch.sql.
func EventsWithNextSQL(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf(`INSERT INTO {{ .database }}.{{ .table }}
SELECT
    now() as updated_at,
    position,
    account_id,
    leadInFrame(position) OVER (PARTITION BY account_id ORDER BY position) as next_position
FROM %s.%s FINAL
WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}`, sourceDatabase, sourceTable)
}

// MultiStatementSQL returns SQL with multiple INSERT statements.
// This tests the multi-statement execution pattern from int_storage_slot_next_touch.sql.
func MultiStatementSQL(sourceDatabase, sourceTable, helperDatabase, helperTable string) string {
	return fmt.Sprintf(`-- Statement 1: Main transformation with next_position calculation
INSERT INTO {{ .database }}.{{ .table }}
SELECT
    now() as updated_at,
    position,
    account_id,
    leadInFrame(position) OVER (PARTITION BY account_id ORDER BY position) as next_position
FROM %s.%s FINAL
WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }};

-- Statement 2: Update helper table with latest state
INSERT INTO %s.%s
SELECT account_id, max(position) as latest_position, now() as updated_at
FROM {{ .database }}.{{ .table }} FINAL
WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}
GROUP BY account_id`, sourceDatabase, sourceTable, helperDatabase, helperTable)
}

// MultiDependencyUnionSQL returns SQL that unions data from multiple sources.
// This tests the UNION pattern for transformations with multiple dependencies.
func MultiDependencyUnionSQL(source1Database, source1Table, source2Database, source2Table string) string {
	return fmt.Sprintf(`INSERT INTO {{ .database }}.{{ .table }}
SELECT
    now() as updated_at,
    position,
    account_id,
    event_count,
    total_value
FROM (
    SELECT position, account_id, event_count, total_value
    FROM %s.%s FINAL
    WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}
    UNION ALL
    SELECT position, account_id, event_count, total_value
    FROM %s.%s FINAL
    WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}
)`, source1Database, source1Table, source2Database, source2Table)
}

// ExternalBoundsWithCacheSQL returns SQL for external bounds query with cache support.
// This pattern is from canonical_beacon_block.sql with incremental/full scan logic.
func ExternalBoundsWithCacheSQL(sourceDatabase, sourceTable string) string {
	return fmt.Sprintf(`SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      min(position) as min,
    {{ end }}
    max(position) as max
FROM %s.%s
{{ if .cache.is_incremental_scan }}
WHERE position >= {{ default "0" .cache.previous_max }}
{{ end }}`, sourceDatabase, sourceTable)
}

// NetworkFilteredExternalBoundsSQL creates SQL that filters by network environment variable.
func NetworkFilteredExternalBoundsSQL(database, table string) string {
	return fmt.Sprintf(`SELECT
    min(position) as min,
    max(position) as max
FROM %s.%s
WHERE network = '{{ .env.NETWORK }}'`, database, table)
}

// AsymmetricZeroBoundsSQL creates SQL that returns a valid min but 0 for max.
// This simulates the bug where incremental scan template preserves previous_min
// but the max query returns 0 (no data in scan window).
func AsymmetricZeroBoundsSQL() string {
	return `SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
      0 as max
    {{ else }}
      min(position) as min,
      max(position) as max
    FROM {{ .self.database }}.{{ .self.table }}
    {{ end }}`
}

// MinGreaterThanMaxBoundsSQL creates SQL that returns min > max (invalid bounds).
func MinGreaterThanMaxBoundsSQL() string {
	return `SELECT
    {{ if .cache.is_incremental_scan }}
      600 as min,
      200 as max
    {{ else }}
      min(position) as min,
      max(position) as max
    FROM {{ .self.database }}.{{ .self.table }}
    {{ end }}`
}

// SlowFullScanSQL creates SQL where full scans are intentionally slower
// than incremental scans to test timing behavior.
func SlowFullScanSQL(database, table string) string {
	return fmt.Sprintf(`SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
      (
        SELECT max(position)
        FROM %s.%s
        WHERE position >= {{ default "0" .cache.previous_max }}
          AND position < {{ default "0" .cache.previous_max }} + 100
      ) as max
    {{ else }}
      min(position) as min,
      max(position) as max
    FROM (
        SELECT position, sleepEachRow(0.001) as _sleep
        FROM %s.%s
    )
    {{ end }}`, database, table, database, table)
}

// ChainConfig holds configuration for creating transformation chains.
type ChainConfig struct {
	SourceDatabase string
	SourceTable    string
	Level1Database string
	Level1Table    string
	Level2Database string
	Level2Table    string
	MinInterval    uint64
	MaxInterval    uint64
	IntervalType   string
}

// DefaultChainConfig returns a default chain configuration.
func DefaultChainConfig() ChainConfig {
	return ChainConfig{
		SourceDatabase: "source",
		SourceTable:    "events_source",
		Level1Database: "transform",
		Level1Table:    "events_aggregated",
		Level2Database: "transform",
		Level2Table:    "events_by_account",
		MinInterval:    0,
		MaxInterval:    100,
		IntervalType:   "slot",
	}
}

// NewTransformationChain creates a chain of transformations for testing nested dependencies.
// Returns: [external, level1_transform (depends on external), level2_transform (depends on level1)]
func NewTransformationChain(cfg ChainConfig) (
	external models.External,
	level1Transform models.Transformation,
	level2Transform models.Transformation,
	dag models.DAGReader,
	err error,
) {
	// Create external model for bounds
	externalSQL := DefaultExternalBoundsSQL(cfg.SourceDatabase, cfg.SourceTable)
	external, err = NewExternalSQL(cfg.SourceDatabase, cfg.SourceTable, externalSQL)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create external: %w", err)
	}

	// Create level 1 transformation (depends on external)
	level1SQL := EventsAggregatedSQL(cfg.SourceDatabase, cfg.SourceTable)
	level1Transform, err = NewTransformationSQL(
		cfg.Level1Database, cfg.Level1Table, level1SQL,
		WithDependencies(cfg.SourceDatabase+"."+cfg.SourceTable),
		WithInterval(cfg.MinInterval, cfg.MaxInterval),
		WithIntervalType(cfg.IntervalType),
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create level1 transformation: %w", err)
	}

	// Create level 2 transformation (depends on level 1 transformation)
	level2SQL := EventsByAccountCumulativeSQL(cfg.Level1Database, cfg.Level1Table)
	level2Transform, err = NewTransformationSQL(
		cfg.Level2Database, cfg.Level2Table, level2SQL,
		WithDependencies(cfg.Level1Database+"."+cfg.Level1Table),
		WithInterval(cfg.MinInterval, cfg.MaxInterval),
		WithIntervalType(cfg.IntervalType),
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create level2 transformation: %w", err)
	}

	// Build DAG
	dag = DAG(
		[]models.Transformation{level1Transform, level2Transform},
		[]models.External{external},
	)

	return external, level1Transform, level2Transform, dag, nil
}

// NewCumulativeTransformation creates a transformation that reads its own
// previous state and applies incremental deltas (cumulative state pattern).
func NewCumulativeTransformation(
	targetDatabase, targetTable string,
	sourceDatabase, sourceTable string,
	opts ...TransformationOption,
) (models.Transformation, error) {
	sql := EventsByAccountCumulativeSQL(sourceDatabase, sourceTable)
	defaultOpts := []TransformationOption{
		WithDependencies(sourceDatabase + "." + sourceTable),
		WithInterval(0, 100),
	}
	return NewTransformationSQL(targetDatabase, targetTable, sql, append(defaultOpts, opts...)...)
}

// NewMultiStatementTransformation creates a transformation with multiple
// INSERT statements (main transformation + helper table update).
func NewMultiStatementTransformation(
	targetDatabase, targetTable string,
	sourceDatabase, sourceTable string,
	helperDatabase, helperTable string,
	opts ...TransformationOption,
) (models.Transformation, error) {
	sql := MultiStatementSQL(sourceDatabase, sourceTable, helperDatabase, helperTable)
	defaultOpts := []TransformationOption{
		WithDependencies(sourceDatabase + "." + sourceTable),
		WithInterval(0, 100),
	}
	return NewTransformationSQL(targetDatabase, targetTable, sql, append(defaultOpts, opts...)...)
}

// NewMultiDependencyTransformation creates a transformation that depends on
// multiple source transformations (UNION pattern).
func NewMultiDependencyTransformation(
	targetDatabase, targetTable string,
	source1Database, source1Table string,
	source2Database, source2Table string,
	opts ...TransformationOption,
) (models.Transformation, error) {
	sql := MultiDependencyUnionSQL(source1Database, source1Table, source2Database, source2Table)
	defaultOpts := []TransformationOption{
		WithDependencies(
			source1Database+"."+source1Table,
			source2Database+"."+source2Table,
		),
		WithInterval(0, 100),
	}
	return NewTransformationSQL(targetDatabase, targetTable, sql, append(defaultOpts, opts...)...)
}

// NewWindowFunctionTransformation creates a transformation using window functions
// for testing leadInFrame/lagInFrame patterns.
func NewWindowFunctionTransformation(
	targetDatabase, targetTable string,
	sourceDatabase, sourceTable string,
	opts ...TransformationOption,
) (models.Transformation, error) {
	sql := EventsWithNextSQL(sourceDatabase, sourceTable)
	defaultOpts := []TransformationOption{
		WithDependencies(sourceDatabase + "." + sourceTable),
		WithInterval(0, 100),
	}
	return NewTransformationSQL(targetDatabase, targetTable, sql, append(defaultOpts, opts...)...)
}
