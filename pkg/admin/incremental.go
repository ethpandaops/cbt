package admin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/sirupsen/logrus"
)

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
		return 0, fmt.Errorf("failed to query first position: %w", err)
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
		return 0, fmt.Errorf("failed to query next unprocessed position: %w", err)
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
		return 0, fmt.Errorf("failed to query last processed position: %w", err)
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
		FullyCovered uint8 `ch:"fully_covered"`
	}

	err = a.client.QueryOne(ctx, query, &result)
	if err != nil {
		return false, fmt.Errorf("failed to query coverage: %w", err)
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
		return nil, fmt.Errorf("failed to query gaps: %w", err)
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
		return nil, fmt.Errorf("failed to query first position for gaps: %w", err)
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

// GetAllProcessedRanges returns processed ranges for multiple models in a single query.
// This avoids the N+1 query problem when listing coverage for all models.
func (a *service) GetAllProcessedRanges(ctx context.Context, modelIDs []string) (map[string][]ProcessedRange, error) {
	if len(modelIDs) == 0 {
		return make(map[string][]ProcessedRange), nil
	}

	// Build the WHERE IN clause with (database, table) tuples
	tuples := make([]string, 0, len(modelIDs))

	for _, id := range modelIDs {
		database, table, err := modelid.Parse(id)
		if err != nil {
			return nil, err
		}

		tuples = append(tuples, fmt.Sprintf("('%s', '%s')", database, table))
	}

	query := fmt.Sprintf(`
		SELECT database, table, position, interval
		FROM %s FINAL
		WHERE (database, table) IN (%s)
		ORDER BY database, table, position DESC
	`, buildTableRef(a.adminDatabase, a.adminTable), strings.Join(tuples, ", "))

	var rows []struct {
		Database string `ch:"database"`
		Table    string `ch:"table"`
		Position uint64 `ch:"position"`
		Interval uint64 `ch:"interval"`
	}

	if err := a.client.QueryMany(ctx, query, &rows); err != nil {
		return nil, fmt.Errorf("failed to batch query processed ranges: %w", err)
	}

	result := make(map[string][]ProcessedRange, len(modelIDs))
	for _, row := range rows {
		id := modelid.Format(row.Database, row.Table)
		result[id] = append(result[id], ProcessedRange{
			Position: row.Position,
			Interval: row.Interval,
		})
	}

	a.log.WithFields(logrus.Fields{
		"model_count": len(modelIDs),
		"total_rows":  len(rows),
	}).Debug("Batch retrieved processed ranges from admin table")

	return result, nil
}

// ConsolidateHistoricalData consolidates historical admin table rows for a model
func (a *service) ConsolidateHistoricalData(ctx context.Context, modelID string) (uint64, error) {
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
		RowCount uint64 `ch:"row_count"`
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

	// Fetch the exact granular rows that make up this island BEFORE inserting the
	// consolidated row. We delete precisely these keys afterwards — never a range —
	// so a delete can only ever reference rows that existed at scan time. This is
	// what makes consolidation safe against late or out-of-order deletes: an older
	// delete can never clip a consolidated row written by a later run, because that
	// newer row's (position, interval) was never in any earlier delete's key set.
	membersQuery := fmt.Sprintf(`
		SELECT position, interval
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
		  AND position >= %d AND position < %d
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table,
		rangeResult.StartPos, rangeResult.EndPos)

	var members []ProcessedRange
	if err := a.client.QueryMany(ctx, membersQuery, &members); err != nil {
		return 0, fmt.Errorf("failed to fetch island members for consolidation: %w", err)
	}

	// Guard against the island shrinking between the range scan and this fetch
	// (e.g. a concurrent delete). Nothing meaningful to merge if fewer than 2 rows.
	if len(members) < 2 {
		return 0, nil
	}

	return a.writeConsolidatedRows(ctx, modelID, database, table, members)
}

// writeConsolidatedRows inserts a single consolidated row spanning the members'
// full range, then deletes the granular member rows by their exact keys in
// batches. members must contain at least two rows. It returns the number of
// member rows the consolidation accounted for.
func (a *service) writeConsolidatedRows(ctx context.Context, modelID, database, table string, members []ProcessedRange) (uint64, error) {
	// Derive the consolidated span from the rows we actually read, so the consolidated
	// row always covers exactly the rows we are about to delete — even if the range
	// scan observed a slightly different (e.g. partial) view of the island.
	startPos := members[0].Position

	var endPos uint64

	for _, m := range members {
		if m.Position < startPos {
			startPos = m.Position
		}

		if end := m.Position + m.Interval; end > endPos {
			endPos = end
		}
	}

	consolidatedInterval := endPos - startPos

	// Insert the consolidated row FIRST so coverage is never lost if the delete fails.
	consolidationTime := time.Now().UTC()
	insertQuery := fmt.Sprintf(`
		INSERT INTO `+"`%s`.`%s`"+` (updated_date_time, database, table, position, interval)
		VALUES ('%s', '%s', '%s', %d, %d)
	`, a.adminDatabase, a.adminTable, consolidationTime.Format("2006-01-02 15:04:05.000"),
		database, table, startPos, consolidatedInterval)

	if err := a.client.Execute(ctx, insertQuery); err != nil {
		return 0, fmt.Errorf("failed to insert consolidated row: %w", err)
	}

	// Delete the granular rows by their exact (position, interval) keys, excluding the
	// consolidated row's own key (in case a member already matched it). Deleting by
	// explicit identity — rather than "everything in range except this interval" —
	// is what prevents a stale or out-of-order delete from clipping a newer
	// consolidated row.
	tuples := make([]string, 0, len(members))

	for _, m := range members {
		if m.Position == startPos && m.Interval == consolidatedInterval {
			continue
		}

		tuples = append(tuples, fmt.Sprintf("(%d, %d)", m.Position, m.Interval))
	}

	// Unreachable when the member fetch uses FINAL: dedup guarantees at most one row
	// can match the consolidated key, so len(tuples) >= len(members)-1 >= 1 past the
	// "< 2" guard in the caller. Kept as a guard in case the fetch ever becomes non-FINAL.
	if len(tuples) == 0 {
		return uint64(len(members)), nil
	}

	if err := a.deleteMemberBatches(ctx, modelID, database, table, tuples); err != nil {
		// The consolidated row is already in place; any rows not yet removed are
		// harmless over-coverage that the next run will clean up.
		return uint64(len(members)), err
	}

	return uint64(len(members)), nil
}

// deleteMemberBatches deletes the given (position, interval) key tuples in
// batches so a long-overdue island never produces a single IN-list that exceeds
// max_query_size. Each batch is still keyed, so the out-of-order delete safety
// holds per batch.
func (a *service) deleteMemberBatches(ctx context.Context, modelID, database, table string, tuples []string) error {
	batches := (len(tuples) + consolidationDeleteBatchSize - 1) / consolidationDeleteBatchSize
	if batches > 1 {
		a.log.WithFields(logrus.Fields{
			"model_id": modelID,
			"rows":     len(tuples),
			"batches":  batches,
		}).Info("Consolidating large island across batched deletes")
	}

	for start := 0; start < len(tuples); start += consolidationDeleteBatchSize {
		end := min(start+consolidationDeleteBatchSize, len(tuples))

		keyPredicate := fmt.Sprintf("(position, interval) IN (%s)", strings.Join(tuples[start:end], ", "))
		if err := a.client.Execute(ctx, a.buildKeyedDelete(database, table, keyPredicate)); err != nil {
			return fmt.Errorf("consolidated row inserted but failed to delete old rows: %w", err)
		}
	}

	return nil
}

// DeletePeriod removes tracking rows that overlap [startPos, endPos) and re-inserts
// remainder rows to preserve coverage outside the deleted window.
// Returns the number of original overlapping rows deleted.
func (a *service) DeletePeriod(ctx context.Context, modelID string, startPos, endPos uint64) (uint64, error) {
	database, table, err := modelid.Parse(modelID)
	if err != nil {
		return 0, err
	}

	// 1. Query overlapping rows
	selectQuery := fmt.Sprintf(`
		SELECT position, interval
		FROM %s FINAL
		WHERE database = '%s' AND table = '%s'
		  AND position + interval > %d
		  AND position < %d
		ORDER BY position
	`, buildTableRef(a.adminDatabase, a.adminTable), database, table, startPos, endPos)

	var overlapping []ProcessedRange

	if err := a.client.QueryMany(ctx, selectQuery, &overlapping); err != nil {
		return 0, fmt.Errorf("failed to query overlapping rows: %w", err)
	}

	if len(overlapping) == 0 {
		return 0, nil
	}

	// 2. Compute remainder rows
	type remainder struct {
		position uint64
		interval uint64
	}

	remainders := make([]remainder, 0, len(overlapping)*2)

	for _, row := range overlapping {
		rowEnd := row.Position + row.Interval

		// Left remainder: portion before the deletion window
		if row.Position < startPos {
			remainders = append(remainders, remainder{
				position: row.Position,
				interval: startPos - row.Position,
			})
		}

		// Right remainder: portion after the deletion window
		if rowEnd > endPos {
			remainders = append(remainders, remainder{
				position: endPos,
				interval: rowEnd - endPos,
			})
		}
	}

	deletedCount := uint64(len(overlapping))

	// 3. Delete all overlapping rows
	keyPredicate := fmt.Sprintf("position + interval > %d AND position < %d", startPos, endPos)
	deleteQuery := a.buildKeyedDelete(database, table, keyPredicate)

	if err := a.client.Execute(ctx, deleteQuery); err != nil {
		return 0, fmt.Errorf("failed to delete overlapping rows: %w", err)
	}

	// 4. Insert remainder rows
	for _, r := range remainders {
		insertQuery := fmt.Sprintf(`
			INSERT INTO %s (updated_date_time, database, table, position, interval)
			VALUES (now(), '%s', '%s', %d, %d)
		`, buildTableRef(a.adminDatabase, a.adminTable), database, table, r.position, r.interval)

		if err := a.client.Execute(ctx, insertQuery); err != nil {
			return deletedCount, fmt.Errorf(
				"deleted rows but failed to insert remainder (%d, %d): %w",
				r.position, r.interval, err,
			)
		}
	}

	a.log.WithFields(logrus.Fields{
		"model_id":       modelID,
		"start_pos":      startPos,
		"end_pos":        endPos,
		"deleted_rows":   deletedCount,
		"remainder_rows": len(remainders),
	}).Info("Deleted period from admin table")

	return deletedCount, nil
}
