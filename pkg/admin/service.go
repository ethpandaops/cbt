package admin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrInvalidModelID is returned when model ID format is invalid
	ErrInvalidModelID = errors.New("invalid model ID format: expected database.table")
	// ErrCacheManagerUnavailable is returned when cache manager is not available
	ErrCacheManagerUnavailable = errors.New("cache manager not available")
)

// Service defines the public interface for the admin service
type Service interface {
	// Position tracking (for incremental transformations)
	GetLastProcessedEndPosition(ctx context.Context, modelID string) (uint64, error) // Returns end of last processed range
	GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error)  // Returns next position for forward fill
	GetLastProcessedPosition(ctx context.Context, modelID string) (uint64, error)    // Returns position of last record
	GetFirstPosition(ctx context.Context, modelID string) (uint64, error)
	RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error

	// Scheduled transformation tracking
	RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error
	GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error)

	// Coverage and gap management
	GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error)
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
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return ErrInvalidModelID
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
		INSERT INTO `+"`%s`.`%s`"+` (updated_date_time, database, table, position, interval)
		VALUES (now(), '%s', '%s', %d, %d)
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], position, interval)

	return a.client.Execute(ctx, query)
}

// GetFirstPosition returns the first processed position for a model
func (a *service) GetFirstPosition(ctx context.Context, modelID string) (uint64, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return 0, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		SELECT coalesce(min(position), 0) as first_pos
		FROM `+"`%s`.`%s`"+` FINAL
		WHERE database = '%s' AND table = '%s'
	`, a.adminDatabase, a.adminTable, parts[0], parts[1])

	var result struct {
		FirstPos uint64 `json:"first_pos,string"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return result.FirstPos, nil
}

// GetLastProcessedEndPosition returns the end of the last processed range (max(position + interval))
// This is the position where forward processing should continue from
func (a *service) GetLastProcessedEndPosition(ctx context.Context, modelID string) (uint64, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return 0, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		SELECT coalesce(max(position + interval), 0) as last_end_pos
		FROM `+"`%s`.`%s`"+` FINAL
		WHERE database = '%s' AND table = '%s'
	`, a.adminDatabase, a.adminTable, parts[0], parts[1])

	var result struct {
		LastEndPos uint64 `json:"last_end_pos,string"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return result.LastEndPos, nil
}

// GetNextUnprocessedPosition returns the next position to process for forward fill
// This is equivalent to GetLastProcessedEndPosition but with clearer naming
func (a *service) GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) {
	return a.GetLastProcessedEndPosition(ctx, modelID)
}

// GetLastProcessedPosition returns the position of the last processed record (max(position))
// This is useful for understanding the actual last record processed, not where to continue from
func (a *service) GetLastProcessedPosition(ctx context.Context, modelID string) (uint64, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return 0, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		SELECT coalesce(max(position), 0) as last_pos
		FROM `+"`%s`.`%s`"+` FINAL
		WHERE database = '%s' AND table = '%s'
	`, a.adminDatabase, a.adminTable, parts[0], parts[1])

	var result struct {
		LastPos uint64 `json:"last_pos,string"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return result.LastPos, nil
}

// GetCoverage checks if a range is fully covered in the admin table
func (a *service) GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return false, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		WITH coverage AS (
			SELECT position, position + interval as end_pos
			FROM `+"`%s`.`%s`"+` FINAL
			WHERE database = '%s' AND table = '%s'
			  AND position < %d
			  AND position + interval > %d
		)
		SELECT CASE 
			WHEN min(position) <= %d AND max(end_pos) >= %d 
			THEN 1 ELSE 0 
		END as fully_covered
		FROM coverage
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], endPos, startPos, startPos, endPos)

	var result struct {
		FullyCovered int `json:"fully_covered"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	return result.FullyCovered == 1, nil
}

// FindGaps finds all gaps in the processed data for a model
func (a *service) FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]GapInfo, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return nil, ErrInvalidModelID
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
	query := fmt.Sprintf(`
		WITH ordered_rows AS (
			SELECT
				position,
				interval,
				position + interval as end_pos,
				row_number() OVER (ORDER BY position) as rn
			FROM `+"`%s`.`%s`"+` FINAL
			WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
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
			  AND position - prev_max_end >= %d
		)
		SELECT 
			gap_start,
			gap_end
		FROM gaps
		ORDER BY gap_start DESC
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], minPos, maxPos, interval)

	var gapResults []struct {
		GapStart uint64 `json:"gap_start,string"`
		GapEnd   uint64 `json:"gap_end,string"`
	}

	err := a.client.QueryMany(ctx, query, &gapResults)
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
		FROM `+"`%s`.`%s`"+` FINAL
		WHERE database = '%s' AND table = '%s'
		  AND position >= %d
	`, a.adminDatabase, a.adminTable, parts[0], parts[1], minPos)

	var firstPosResult struct {
		FirstPos *uint64 `json:"first_pos,string"`
	}

	err = a.client.QueryOne(ctx, firstPosQuery, &firstPosResult)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
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

// ConsolidateHistoricalData consolidates historical admin table rows for a model
func (a *service) ConsolidateHistoricalData(ctx context.Context, modelID string) (int, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return 0, ErrInvalidModelID
	}

	database := parts[0]
	table := parts[1]

	// Find contiguous/overlapping ranges that can be consolidated
	// This merges both overlapping and contiguous ranges into single consolidated ranges
	// Uses window functions compatible with ClickHouse v25
	rangeQuery := fmt.Sprintf(`
		WITH ordered_rows AS (
			SELECT
				position,
				interval,
				position + interval as end_pos,
				row_number() OVER (ORDER BY position) as rn
			FROM `+"`%s`.`%s`"+` FINAL
			WHERE database = '%s' AND table = '%s'
			ORDER BY position
		),
		with_lag AS (
			SELECT 
				position,
				interval,
				end_pos,
				rn,
				any(end_pos) OVER (ORDER BY position ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_end
			FROM ordered_rows
		),
		with_max AS (
			SELECT
				position,
				interval,
				end_pos,
				rn,
				prev_end,
				max(end_pos) OVER (ORDER BY position ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_end_so_far
			FROM with_lag
		),
		with_groups AS (
			SELECT
				position,
				interval,
				end_pos,
				CASE 
					WHEN rn = 1 THEN 1
					WHEN position > any(max_end_so_far) OVER (ORDER BY position ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) THEN 1
					ELSE 0
				END as new_group
			FROM with_max
		),
		with_group_ids AS (
			SELECT
				position,
				interval,
				end_pos,
				sum(new_group) OVER (ORDER BY position) as group_id
			FROM with_groups
		),
		consolidated_ranges AS (
			SELECT
				group_id,
				MIN(position) as start_pos,
				MAX(end_pos) as end_pos,
				COUNT(*) as row_count
			FROM with_group_ids
			GROUP BY group_id
		)
		SELECT
			start_pos,
			end_pos,
			row_count
		FROM consolidated_ranges
		WHERE row_count > 1
		ORDER BY row_count DESC, start_pos
		LIMIT 1
	`, a.adminDatabase, a.adminTable, database, table)

	var rangeResult struct {
		StartPos uint64 `json:"start_pos,string"`
		EndPos   uint64 `json:"end_pos,string"`
		RowCount int    `json:"row_count,string"`
	}

	err := a.client.QueryOne(ctx, rangeQuery, &rangeResult)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil // No contiguous ranges found
		}
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

	// Insert the consolidated row FIRST to avoid gaps
	consolidatedInterval := rangeResult.EndPos - rangeResult.StartPos
	insertQuery := fmt.Sprintf(`
		INSERT INTO `+"`%s`.`%s`"+` (updated_date_time, database, table, position, interval)
		VALUES (now(), '%s', '%s', %d, %d)
	`, a.adminDatabase, a.adminTable, database, table, rangeResult.StartPos, consolidatedInterval)

	if err := a.client.Execute(ctx, insertQuery); err != nil {
		return 0, fmt.Errorf("failed to insert consolidated row: %w", err)
	}

	// Delete old rows EXCEPT the consolidated row we just inserted
	// This ensures we don't create gaps and don't delete our new row
	// Use lazy DELETE and handle cluster mode if configured
	var deleteQuery string
	if a.cluster != "" {
		// Cluster mode: use local suffix and ON CLUSTER
		deleteQuery = fmt.Sprintf(`
			DELETE FROM `+"`%s`.`%s`%s"+` ON CLUSTER '%s'
			WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
			  AND NOT (position = %d AND interval = %d)
		`, a.adminDatabase, a.adminTable, a.localSuffix, a.cluster,
			database, table, rangeResult.StartPos, rangeResult.EndPos,
			rangeResult.StartPos, consolidatedInterval)
	} else {
		// Non-cluster mode: simple DELETE
		deleteQuery = fmt.Sprintf(`
			DELETE FROM `+"`%s`.`%s`"+`
			WHERE database = '%s' AND table = '%s'
			  AND position >= %d AND position < %d
			  AND NOT (position = %d AND interval = %d)
		`, a.adminDatabase, a.adminTable,
			database, table, rangeResult.StartPos, rangeResult.EndPos,
			rangeResult.StartPos, consolidatedInterval)
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
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return ErrInvalidModelID
	}

	a.log.WithFields(logrus.Fields{
		"model_id":        modelID,
		"start_date_time": startDateTime,
	}).Debug("Recording scheduled task completion in admin table")

	query := fmt.Sprintf(`
		INSERT INTO `+"`%s`.`%s`"+` (updated_date_time, database, table, start_date_time)
		VALUES (now(), '%s', '%s', '%s')
	`, a.scheduledAdminDatabase, a.scheduledAdminTable, parts[0], parts[1], startDateTime.Format("2006-01-02 15:04:05.000"))

	return a.client.Execute(ctx, query)
}

// GetLastScheduledExecution returns the last execution time for a scheduled transformation
func (a *service) GetLastScheduledExecution(ctx context.Context, modelID string) (*time.Time, error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return nil, ErrInvalidModelID
	}

	query := fmt.Sprintf(`
		SELECT max(start_date_time) as last_execution
		FROM `+"`%s`.`%s`"+` FINAL
		WHERE database = '%s' AND table = '%s'
	`, a.scheduledAdminDatabase, a.scheduledAdminTable, parts[0], parts[1])

	var result struct {
		LastExecution *time.Time `json:"last_execution"`
	}

	err := a.client.QueryOne(ctx, query, &result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return result.LastExecution, nil
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
