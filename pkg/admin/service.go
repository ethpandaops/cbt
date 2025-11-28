package admin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrCacheManagerUnavailable is returned when cache manager is not available
	ErrCacheManagerUnavailable = errors.New("cache manager not available")
)

// buildTableRef creates a backtick-escaped table reference for SQL queries.
func buildTableRef(database, table string) string {
	return fmt.Sprintf("`%s`.`%s`", database, table)
}

// Service defines the public interface for the admin service
type Service interface {
	// Position tracking (for incremental transformations)
	GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) // Returns next position for forward fill
	GetLastProcessedPosition(ctx context.Context, modelID string) (uint64, error)   // Returns position of last record
	GetFirstPosition(ctx context.Context, modelID string) (uint64, error)
	RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error

	// Scheduled transformation tracking
	RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error
	GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error)

	// Coverage and gap management
	GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error)
	GetProcessedRanges(ctx context.Context, modelID string) ([]ProcessedRange, error)
	FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]GapInfo, error)

	// Consolidation
	ConsolidateHistoricalData(ctx context.Context, modelID string) (int, error)

	// External bounds cache
	GetExternalBounds(ctx context.Context, modelID string) (*BoundsCache, error)
	SetExternalBounds(ctx context.Context, cache *BoundsCache) error

	// Admin table info
	GetIncrementalAdminDatabase() string
	GetIncrementalAdminTable() string
	GetScheduledAdminDatabase() string
	GetScheduledAdminTable() string
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
	cacheManager := NewCacheManager(redisClient)

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

// RecordCompletion records a completed transformation in the admin table
func (a *service) RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return err
	}

	// Log what we're recording for better debugging
	a.log.WithFields(logrus.Fields{
		"model_id":    modelID,
		"position":    position,
		"interval":    interval,
		"range_start": position,
		"range_end":   position + interval,
	}).Debug("Recording task completion in admin table")

	// Using string formatting with proper escaping
	// In production, consider using parameterized queries for better security
	query := fmt.Sprintf(`
		INSERT INTO %s (updated_date_time, database, table, position, interval)
		VALUES (now(), '%s', '%s', %d, %d)
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table, position, interval)

	if err := a.client.Execute(ctx, query); err != nil {
		return fmt.Errorf("failed to insert admin record: %w", err)
	}

	return nil
}

// GetFirstPosition returns the first processed position for a model
func (a *service) GetFirstPosition(ctx context.Context, modelID string) (uint64, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(`
		SELECT coalesce(min(position), 0) as first_pos
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table)

	var result struct {
		FirstPos uint64 `ch:"first_pos"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return 0, err
	}

	return result.FirstPos, nil
}

// GetNextUnprocessedPosition returns the next position to process for forward fill
// This is the end of the last processed range: max(position + interval)
func (a *service) GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(`
		SELECT coalesce(max(position + interval), 0) as last_end_pos
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table)

	var result struct {
		LastEndPos uint64 `ch:"last_end_pos"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return 0, err
	}

	return result.LastEndPos, nil
}

// GetLastProcessedPosition returns the position of the last processed record (max(position))
// This is useful for understanding the actual last record processed, not where to continue from
func (a *service) GetLastProcessedPosition(ctx context.Context, modelID string) (uint64, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(`
		SELECT coalesce(max(position), 0) as last_pos
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table)

	var result struct {
		LastPos uint64 `ch:"last_pos"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return 0, err
	}

	return result.LastPos, nil
}

// GetCoverage checks if a range is fully covered in the admin table
func (a *service) GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf(`
		WITH coverage AS (
			SELECT position, position + interval as end_pos
			FROM %s FINAL
			WHERE database = '%s' AND table = '%s'
			  AND position < %d
			  AND position + interval > %d
		)
		SELECT CASE
			WHEN min(position) <= %d AND max(end_pos) >= %d
			THEN 1 ELSE 0
		END as fully_covered
		FROM coverage
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table, endPos, startPos, startPos, endPos)

	var result struct {
		FullyCovered int `ch:"fully_covered"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return false, err
	}

	return result.FullyCovered == 1, nil
}

// FindGaps finds all gaps in the processed data for a model
func (a *service) FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]GapInfo, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return nil, err
	}

	// Log the gap detection parameters
	a.log.WithFields(logrus.Fields{
		"model_id": modelID,
		"min_pos":  minPos,
		"max_pos":  maxPos,
		"interval": interval,
	}).Debug("Finding gaps in processed data - scanning admin table")

	// Use a similar approach to consolidation but find gaps instead
	// This properly handles overlapping and contiguous ranges
	// Note: ClickHouse doesn't allow window functions in WHERE clauses,
	// so we calculate prev_max_end in a CTE first
	// IMPORTANT: Include intervals that start before minPos but extend into the range
	// to avoid false positive gaps
	query := fmt.Sprintf(`
		WITH ordered_rows AS (
			SELECT
				position,
				interval,
				position + interval as end_pos,
				row_number() OVER (ORDER BY position) as rn
			FROM %s FINAL
			WHERE database = '%s' AND table = '%s'
			  AND position + interval > %d
			  AND position < %d
			ORDER BY position
		),
		with_max AS (
			SELECT
				position,
				interval,
				end_pos,
				rn,
				max(end_pos) OVER (ORDER BY position ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_end_so_far
			FROM ordered_rows
		),
		with_lag AS (
			SELECT
				position,
				end_pos,
				rn,
				max_end_so_far,
				-- Get the max_end_so_far from the previous row
				if(rn > 1,
				   any(max_end_so_far) OVER (ORDER BY position ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),
				   0) as prev_max_end
			FROM with_max
		),
		gaps AS (
			SELECT
				prev_max_end as gap_start,
				position as gap_end
			FROM with_lag
			WHERE rn > 1
			  AND prev_max_end IS NOT NULL
			  AND position > prev_max_end
		)
		SELECT
			gap_start,
			gap_end
		FROM gaps
		ORDER BY gap_start DESC
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table, minPos, maxPos)

	var gapResults []struct {
		GapStart uint64 `ch:"gap_start"`
		GapEnd   uint64 `ch:"gap_end"`
	}

	err = a.client.QueryMany(ctx, query, &gapResults)
	if err != nil {
		return nil, err
	}

	gaps := make([]GapInfo, 0, len(gapResults))
	for _, result := range gapResults {
		a.log.WithFields(logrus.Fields{
			"model_id":  modelID,
			"gap_start": result.GapStart,
			"gap_end":   result.GapEnd,
			"gap_size":  result.GapEnd - result.GapStart,
		}).Debug("Found gap between consolidated ranges")

		gaps = append(gaps, GapInfo{
			StartPos: result.GapStart,
			EndPos:   result.GapEnd,
		})
	}

	// Also check for a gap at the beginning
	firstPosQuery := fmt.Sprintf(`
		SELECT min(position) as first_pos
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
		  AND position >= %d
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table, minPos)

	var firstPosResult struct {
		FirstPos *uint64 `ch:"first_pos"`
	}

	err = a.client.QueryOne(ctx, firstPosQuery, &firstPosResult)
	if err != nil {
		return nil, err
	}

	if firstPosResult.FirstPos != nil && *firstPosResult.FirstPos > minPos {
		// There's a gap at the beginning - append at end since we're processing DESC
		a.log.WithFields(logrus.Fields{
			"model_id":  modelID,
			"gap_start": minPos,
			"gap_end":   *firstPosResult.FirstPos,
			"gap_size":  *firstPosResult.FirstPos - minPos,
			"first_pos": *firstPosResult.FirstPos,
			"min_pos":   minPos,
		}).Debug("Found gap at beginning of range")

		gaps = append(gaps, GapInfo{StartPos: minPos, EndPos: *firstPosResult.FirstPos})
	}

	// Log summary of gaps found
	if len(gaps) > 0 {
		a.log.WithFields(logrus.Fields{
			"model_id":   modelID,
			"gap_count":  len(gaps),
			"total_gaps": gaps,
		}).Debug("Gap detection complete")
	}

	return gaps, nil
}

// GetProcessedRanges returns all processed ranges for a model from the admin table
// This returns the raw admin_incremental table data with no filtering or aggregation
func (a *service) GetProcessedRanges(ctx context.Context, modelID string) ([]ProcessedRange, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT position, interval
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
		ORDER BY position DESC
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table)

	var ranges []ProcessedRange

	err = a.client.QueryMany(ctx, query, &ranges)
	if err != nil {
		return nil, fmt.Errorf("failed to query processed ranges: %w", err)
	}

	a.log.WithFields(logrus.Fields{
		"model_id":    modelID,
		"range_count": len(ranges),
	}).Debug("Retrieved processed ranges from admin table")

	return ranges, nil
}

// ConsolidateHistoricalData consolidates historical admin table rows for a model
func (a *service) ConsolidateHistoricalData(ctx context.Context, modelID string) (int, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return 0, err
	}

	// Find contiguous/overlapping ranges that can be consolidated using a 3-CTE approach
	// Uses window functions to detect "islands" of contiguous/overlapping ranges
	// Each island is consolidated into a single row spanning the full range
	rangeQuery := fmt.Sprintf(`
		WITH ordered_rows AS (
			SELECT
				position,
				position + interval as end_pos,
				MAX(position + interval) OVER (
					ORDER BY position
					ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				) as prev_max_end
			FROM %s FINAL
			WHERE database = '%s' AND table = '%s'
		),
		island_groups AS (
			SELECT
				position,
				end_pos,
				SUM(CASE WHEN position > COALESCE(prev_max_end, 0) THEN 1 ELSE 0 END)
					OVER (ORDER BY position) as island_id
			FROM ordered_rows
		),
		consolidated_ranges AS (
			SELECT
				MIN(position) as start_pos,
				MAX(end_pos) as end_pos,
				COUNT(*) as row_count
			FROM island_groups
			GROUP BY island_id
			HAVING COUNT(*) > 1
		)
		SELECT
			start_pos,
			end_pos,
			row_count
		FROM consolidated_ranges
		ORDER BY row_count DESC, start_pos
		LIMIT 1
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table)

	var rangeResult struct {
		StartPos uint64 `ch:"start_pos"`
		EndPos   uint64 `ch:"end_pos"`
		RowCount int    `ch:"row_count"`
	}

	err = a.client.QueryOne(ctx, rangeQuery, &rangeResult)
	if err != nil {
		return 0, fmt.Errorf("failed to find contiguous ranges: %w", err)
	}

	// Check if we actually got valid results (not zeros/nulls)
	// If all fields are zero, it means no ranges were found
	if rangeResult.RowCount == 0 || (rangeResult.StartPos == 0 && rangeResult.EndPos == 0) {
		return 0, nil // No valid ranges to consolidate
	}

	// Don't consolidate a single row - it would just create a duplicate
	if rangeResult.RowCount == 1 {
		return 0, nil // Single row doesn't need consolidation
	}

	// Log what we're about to consolidate for debugging
	a.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"row_count": rangeResult.RowCount,
		"start_pos": rangeResult.StartPos,
		"end_pos":   rangeResult.EndPos,
		"interval":  rangeResult.EndPos - rangeResult.StartPos,
	}).Debug("Consolidating admin table rows")

	// Capture timestamp for the consolidated row before insertion
	// This allows us to delete all older rows using timestamp comparison
	consolidationTime := time.Now().UTC()
	consolidatedInterval := rangeResult.EndPos - rangeResult.StartPos

	// Insert the consolidated row FIRST to avoid gaps
	// Use explicit timestamp to ensure we can reliably delete older rows
	insertQuery := fmt.Sprintf(`
		INSERT INTO `+"`%s`.`%s`"+` (updated_date_time, database, table, position, interval)
		VALUES ('%s', '%s', '%s', %d, %d)
	`, a.adminDatabase, a.adminTable, consolidationTime.Format("2006-01-02 15:04:05.000"),
		database, table, rangeResult.StartPos, consolidatedInterval)

	if err := a.client.Execute(ctx, insertQuery); err != nil {
		return 0, fmt.Errorf("failed to insert consolidated row: %w", err)
	}

	// Delete old rows using interval-based exclusion
	// This is more robust than timestamp-based deletion because:
	// 1. No dependency on clock precision or synchronization across cluster nodes
	// 2. Explicitly excludes the consolidated row by its unique characteristic (larger interval)
	// 3. Works reliably regardless of ReplacingMergeTree merge timing
	// 4. Semantically clear: "delete all rows in range except the consolidated one"
	//
	// The consolidated row has interval = (end_pos - start_pos), which is larger than
	// any individual row's interval in the consolidated range
	var deleteQuery string
	if a.cluster != "" {
		// Cluster mode: use local suffix and ON CLUSTER
		deleteQuery = fmt.Sprintf(`
			DELETE FROM `+"`%s`.`%s%s`"+` ON CLUSTER '%s'
			WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
			  AND interval != %d
		`, a.adminDatabase, a.adminTable, a.localSuffix, a.cluster,
			database, table, rangeResult.StartPos, rangeResult.EndPos,
			consolidatedInterval)
	} else {
		// Non-cluster mode: simple DELETE
		deleteQuery = fmt.Sprintf(`
			DELETE FROM `+"`%s`.`%s`"+`
			WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
			  AND interval != %d
		`, a.adminDatabase, a.adminTable,
			database, table, rangeResult.StartPos, rangeResult.EndPos,
			consolidatedInterval)
	}

	if err := a.client.Execute(ctx, deleteQuery); err != nil {
		// Log error but don't fail - the consolidated row will eventually replace the old ones
		// due to ReplacingMergeTree behavior
		return rangeResult.RowCount, fmt.Errorf("consolidated row inserted but failed to delete old rows: %w", err)
	}

	return rangeResult.RowCount, nil
}

// GetExternalBounds retrieves cached external model bounds
func (a *service) GetExternalBounds(ctx context.Context, modelID string) (*BoundsCache, error) {
	if a.cacheManager == nil {
		return nil, nil
	}
	return a.cacheManager.GetBounds(ctx, modelID)
}

// SetExternalBounds stores external model bounds in cache
func (a *service) SetExternalBounds(ctx context.Context, cache *BoundsCache) error {
	if a.cacheManager == nil {
		return ErrCacheManagerUnavailable
	}
	return a.cacheManager.SetBounds(ctx, cache)
}

// RecordScheduledCompletion records a completed scheduled transformation
func (a *service) RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return err
	}

	a.log.WithFields(logrus.Fields{
		"model_id":        modelID,
		"start_date_time": startDateTime,
	}).Debug("Recording scheduled task completion in admin table")

	query := fmt.Sprintf(`
		INSERT INTO %s (updated_date_time, database, table, start_date_time)
		VALUES (now(), '%s', '%s', '%s')
	`, buildTableRef(a.scheduledAdminDatabase, a.scheduledAdminTable), database, table, startDateTime.Format("2006-01-02 15:04:05.000"))

	return a.client.Execute(ctx, query)
}

// GetLastScheduledExecution returns the last execution time for a scheduled transformation
func (a *service) GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT max(start_date_time) as last_execution
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
	`, buildTableRef(a.scheduledAdminDatabase, a.scheduledAdminTable), database, table)

	var result struct {
		LastExecution *time.Time `ch:"last_execution"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return nil, err
	}

	if result.LastExecution == nil || result.LastExecution.IsZero() {
		return nil, nil
	}

	return result.LastExecution, nil
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
