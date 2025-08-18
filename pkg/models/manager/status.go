package manager

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
)

// GetModelStatuses returns status information for all models
func (m *Manager) GetModelStatuses(ctx context.Context) (statusMap map[string]ModelStatus, allModels map[string]models.ModelConfig, err error) {
	// Query the admin table for last run information
	query := fmt.Sprintf(`
		SELECT 
			database,
			table,
			min(position) as first_position,
			max(position) as last_position,
			max(position + interval) as next_position,
			max(updated_date_time) as last_run,
			count(DISTINCT position) as total_runs
		FROM %s.%s FINAL
		GROUP BY database, table
		ORDER BY database, table
	`, m.adminManager.GetAdminDatabase(), m.adminManager.GetAdminTable())

	var results []ModelStatus
	err = m.chClient.QueryMany(ctx, query, &results)
	if err != nil {
		// Admin table might not exist yet if no models have run
		m.logger.WithError(err).Debug("Failed to query admin table (might not exist yet)")
	}

	// Also discover all models to show ones that haven't run yet
	allModels, err = LoadModels(m.logger, m.pathConfig)
	if err != nil {
		return nil, nil, err
	}

	// Build status map
	statusMap = make(map[string]ModelStatus)
	for _, status := range results {
		modelID := fmt.Sprintf("%s.%s", status.Database, status.Table)
		statusMap[modelID] = status
	}

	return statusMap, allModels, nil
}

// GetGapInformation returns gap information for all models
func (m *Manager) GetGapInformation(ctx context.Context, allModels map[string]models.ModelConfig) map[string]GapInfo {
	gapMap := make(map[string]GapInfo)
	modelIDs := make([]string, 0, len(allModels))

	// Collect model IDs to avoid copying structs
	for modelID := range allModels {
		modelIDs = append(modelIDs, modelID)
	}

	for _, modelID := range modelIDs {
		modelConfig := allModels[modelID]
		if modelConfig.External {
			// External models don't have gaps to track
			continue
		}

		// Get the last position
		lastPos, err := m.adminManager.GetLastPosition(ctx, modelID)
		if err != nil || lastPos == 0 {
			continue // Model hasn't run yet
		}

		info := m.calculateModelGaps(ctx, modelID)
		if info != nil {
			gapMap[modelID] = *info
		}
	}

	return gapMap
}

func (m *Manager) calculateModelGaps(ctx context.Context, modelID string) *GapInfo {
	// Query for gaps - get all positions to calculate coverage
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return nil
	}

	result := m.queryIntervals(ctx, parts[0], parts[1])
	if result == nil {
		return nil
	}

	// Convert json.Number positions to uint64
	positions := make([]uint64, 0, len(result.Positions))
	for _, pos := range result.Positions {
		val, err := pos.Float64()
		if err != nil {
			continue
		}
		positions = append(positions, uint64(val))
	}

	// Calculate gaps
	gapCount, oldestGap := findGaps(positions, result.IntervalSize)

	// Calculate expected intervals and coverage
	totalExpected := calculateExpectedIntervals(result.MinPos, result.MaxPos, result.IntervalSize)
	coverage := calculateCoverage(result.TotalIntervals, totalExpected)

	return &GapInfo{
		GapCount:      gapCount,
		OldestGap:     oldestGap,
		Coverage:      coverage,
		TotalExpected: totalExpected,
	}
}

func (m *Manager) queryIntervals(ctx context.Context, database, table string) *IntervalResult {
	query := fmt.Sprintf(`
		WITH intervals AS (
			SELECT DISTINCT
				position,
				interval
			FROM %s.%s FINAL
			WHERE database = '%s' AND table = '%s'
			ORDER BY position
		)
		SELECT 
			count() as total_intervals,
			min(position) as min_pos,
			max(position + interval) as max_pos,
			groupArray(position) as positions,
			any(interval) as interval_size
		FROM intervals
	`, m.adminManager.GetAdminDatabase(), m.adminManager.GetAdminTable(), database, table)

	var result IntervalResult
	err := m.chClient.QueryOne(ctx, query, &result)
	if err != nil || result.TotalIntervals == 0 {
		return nil
	}

	return &result
}

func findGaps(positions []uint64, intervalSize uint64) (gapCount int, oldestGap uint64) {
	// Sort positions
	sort.Slice(positions, func(i, j int) bool {
		return positions[i] < positions[j]
	})

	// Find gaps between consecutive positions
	for i := 1; i < len(positions); i++ {
		expectedPos := positions[i-1] + intervalSize
		if positions[i] > expectedPos {
			// There's a gap
			for pos := expectedPos; pos < positions[i]; pos += intervalSize {
				gapCount++
				if oldestGap == 0 || pos < oldestGap {
					oldestGap = pos
				}
			}
		}
	}

	return gapCount, oldestGap
}

func calculateExpectedIntervals(minPos, maxPos, intervalSize uint64) int {
	if maxPos <= minPos || intervalSize == 0 {
		return 0
	}

	// Calculate the difference safely
	diff := maxPos - minPos
	result := diff / intervalSize

	// Check for potential overflow when converting to int
	if result > math.MaxInt32 {
		return math.MaxInt32
	}

	// Safe conversion after overflow check
	return int(result)
}

func calculateCoverage(totalIntervals uint64, totalExpected int) float64 {
	if totalExpected <= 0 {
		return 0.0
	}

	coverage := float64(totalIntervals) / float64(totalExpected)
	if coverage > 1.0 {
		coverage = 1.0 // Cap at 100%
	}

	return coverage
}

// FormatRunTimes formats last run and next run times
func FormatRunTimes(lastRunStr string, modelConfig *models.ModelConfig) (lastRun, nextRun string) {
	lastRun = lastRunStr
	var lastRunTime time.Time

	if t, err := time.Parse("2006-01-02 15:04:05", lastRun); err == nil {
		lastRunTime = t
		// Calculate time ago
		ago := time.Since(t)
		switch {
		case ago < time.Hour:
			lastRun = fmt.Sprintf("%dm ago", int(ago.Minutes()))
		case ago < 24*time.Hour:
			lastRun = fmt.Sprintf("%dh ago", int(ago.Hours()))
		default:
			lastRun = fmt.Sprintf("%dd ago", int(ago.Hours()/24))
		}
	}

	// Calculate next run based on schedule
	nextRun = "-"
	if modelConfig.Schedule != "" && !modelConfig.External {
		nextRun = calculateNextRun(modelConfig.Schedule, lastRunTime)
	}

	return lastRun, nextRun
}

func calculateNextRun(schedule string, lastRunTime time.Time) string {
	if !strings.HasPrefix(schedule, "@every ") {
		return "-"
	}

	durationStr := strings.TrimPrefix(schedule, "@every ")
	duration, err := time.ParseDuration(durationStr)
	if err != nil || lastRunTime.IsZero() {
		return "-"
	}

	nextRunTime := lastRunTime.Add(duration)
	timeUntil := time.Until(nextRunTime)

	if timeUntil <= 0 {
		return "due"
	}

	if timeUntil > time.Minute {
		return fmt.Sprintf("in %dm", int(timeUntil.Minutes()))
	}
	return fmt.Sprintf("in %ds", int(timeUntil.Seconds()))
}

// FormatPositions formats position values as raw numbers
func FormatPositions(firstPosition, lastPosition, nextPosition uint64) (firstPosStr, lastPosStr, nextPosStr string) {
	firstPosStr = fmt.Sprintf("%d", firstPosition)
	lastPosStr = fmt.Sprintf("%d", lastPosition)
	nextPosStr = fmt.Sprintf("%d", nextPosition)
	return firstPosStr, lastPosStr, nextPosStr
}

// CreateAdminTableManager creates an admin table manager
func CreateAdminTableManager(chClient clickhouse.ClientInterface, cluster, localSuffix, adminDatabase, adminTable string) *clickhouse.AdminTableManager {
	return clickhouse.NewAdminTableManager(chClient, cluster, localSuffix, adminDatabase, adminTable)
}
