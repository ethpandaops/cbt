//go:build integration

package admin

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDatabase = "admin"
	testTable    = "cbt_incremental"
	testSchedDB  = "admin"
	testSchedTbl = "cbt_scheduled"
)

// createAdminTables creates the required admin tables for testing.
func createAdminTables(t *testing.T, client clickhouse.ClientInterface) {
	t.Helper()

	ctx := context.Background()

	// Create admin database
	err := client.Execute(ctx, "CREATE DATABASE IF NOT EXISTS admin")
	require.NoError(t, err)

	// Create incremental admin table
	incrementalTable := `
		CREATE TABLE IF NOT EXISTS admin.cbt_incremental (
			updated_date_time DateTime(3),
			database LowCardinality(String),
			table LowCardinality(String),
			position UInt64,
			interval UInt64
		)
		ENGINE = ReplacingMergeTree(updated_date_time)
		ORDER BY (database, table, position, interval)
	`
	err = client.Execute(ctx, incrementalTable)
	require.NoError(t, err)

	// Create scheduled admin table
	scheduledTable := `
		CREATE TABLE IF NOT EXISTS admin.cbt_scheduled (
			updated_date_time DateTime(3),
			database LowCardinality(String),
			table LowCardinality(String),
			start_date_time DateTime(3)
		)
		ENGINE = ReplacingMergeTree(updated_date_time)
		ORDER BY (database, table)
	`
	err = client.Execute(ctx, scheduledTable)
	require.NoError(t, err)
}

func setupIntegrationService(t *testing.T) (Service, clickhouse.ClientInterface) {
	t.Helper()

	conn := testutil.NewClickHouseContainer(t)
	redisConn := testutil.NewRedisContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          conn.URL,
		QueryTimeout: 30 * time.Second,
	}
	cfg.SetDefaults()

	client, err := clickhouse.NewClient(logger, cfg)
	require.NoError(t, err)

	err = client.Start()
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := client.Stop(); err != nil {
			t.Logf("failed to stop client: %v", err)
		}
	})

	// Create admin tables
	createAdminTables(t, client)

	// Create the service
	tableConfig := TableConfig{
		IncrementalDatabase: testDatabase,
		IncrementalTable:    testTable,
		ScheduledDatabase:   testSchedDB,
		ScheduledTable:      testSchedTbl,
	}
	svc := NewService(
		logger.WithField("test", "admin"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	return svc, client
}

func TestIntegration_RecordCompletion(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Record a completion
	err := svc.RecordCompletion(ctx, "test_db.test_table", 100, 50)
	require.NoError(t, err)

	// Verify it was recorded
	nextPos, err := svc.GetNextUnprocessedPosition(ctx, "test_db.test_table")
	require.NoError(t, err)
	assert.Equal(t, uint64(150), nextPos) // position + interval = 100 + 50
}

func TestIntegration_GetNextUnprocessedPosition(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Initially should be 0
	nextPos, err := svc.GetNextUnprocessedPosition(ctx, "test_db.new_table")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), nextPos)

	// Record some completions
	err = svc.RecordCompletion(ctx, "test_db.new_table", 0, 100)
	require.NoError(t, err)

	err = svc.RecordCompletion(ctx, "test_db.new_table", 100, 100)
	require.NoError(t, err)

	// Next unprocessed should be 200
	nextPos, err = svc.GetNextUnprocessedPosition(ctx, "test_db.new_table")
	require.NoError(t, err)
	assert.Equal(t, uint64(200), nextPos)
}

func TestIntegration_GetFirstPosition(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Record completions with different starting positions
	err := svc.RecordCompletion(ctx, "test_db.first_pos_table", 500, 100)
	require.NoError(t, err)

	err = svc.RecordCompletion(ctx, "test_db.first_pos_table", 1000, 100)
	require.NoError(t, err)

	// First position should be 500
	firstPos, err := svc.GetFirstPosition(ctx, "test_db.first_pos_table")
	require.NoError(t, err)
	assert.Equal(t, uint64(500), firstPos)
}

func TestIntegration_GetLastProcessedPosition(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Record completions
	err := svc.RecordCompletion(ctx, "test_db.last_pos_table", 100, 50)
	require.NoError(t, err)

	err = svc.RecordCompletion(ctx, "test_db.last_pos_table", 500, 50)
	require.NoError(t, err)

	// Last processed position should be 500 (max position)
	lastPos, err := svc.GetLastProcessedPosition(ctx, "test_db.last_pos_table")
	require.NoError(t, err)
	assert.Equal(t, uint64(500), lastPos)
}

func TestIntegration_FindGaps(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Create data with gaps:
	// Range 0-100, skip 100-200 (gap), Range 200-300
	err := svc.RecordCompletion(ctx, "test_db.gaps_table", 0, 100)
	require.NoError(t, err)

	err = svc.RecordCompletion(ctx, "test_db.gaps_table", 200, 100)
	require.NoError(t, err)

	// Find gaps
	gaps, err := svc.FindGaps(ctx, "test_db.gaps_table", 0, 300, 100)
	require.NoError(t, err)

	// Should find gap from 100-200
	require.Len(t, gaps, 1)
	assert.Equal(t, uint64(100), gaps[0].StartPos)
	assert.Equal(t, uint64(200), gaps[0].EndPos)
}

func TestIntegration_GetCoverage(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Record a completion covering 100-200
	err := svc.RecordCompletion(ctx, "test_db.coverage_table", 100, 100)
	require.NoError(t, err)

	// Check coverage for covered range
	covered, err := svc.GetCoverage(ctx, "test_db.coverage_table", 100, 200)
	require.NoError(t, err)
	assert.True(t, covered)

	// Check coverage for uncovered range
	covered, err = svc.GetCoverage(ctx, "test_db.coverage_table", 0, 50)
	require.NoError(t, err)
	assert.False(t, covered)
}

func TestIntegration_GetProcessedRanges(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Record multiple completions
	err := svc.RecordCompletion(ctx, "test_db.ranges_table", 0, 100)
	require.NoError(t, err)

	err = svc.RecordCompletion(ctx, "test_db.ranges_table", 100, 100)
	require.NoError(t, err)

	err = svc.RecordCompletion(ctx, "test_db.ranges_table", 200, 100)
	require.NoError(t, err)

	// Get processed ranges
	ranges, err := svc.GetProcessedRanges(ctx, "test_db.ranges_table")
	require.NoError(t, err)

	// Should have 3 ranges ordered by position DESC
	require.Len(t, ranges, 3)
	assert.Equal(t, uint64(200), ranges[0].Position)
	assert.Equal(t, uint64(100), ranges[1].Position)
	assert.Equal(t, uint64(0), ranges[2].Position)
}

func TestIntegration_RecordScheduledCompletion(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	startTime := time.Now().UTC().Truncate(time.Millisecond)

	// Record a scheduled completion
	err := svc.RecordScheduledCompletion(ctx, "test_db.scheduled_table", startTime)
	require.NoError(t, err)

	// Verify it was recorded
	lastExec, err := svc.GetLastScheduledExecution(ctx, "test_db.scheduled_table")
	require.NoError(t, err)
	require.NotNil(t, lastExec)
	assert.Equal(t, startTime.UTC(), lastExec.UTC())
}

func TestIntegration_GetLastScheduledExecution_NoRecords(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Get last execution for non-existent model
	lastExec, err := svc.GetLastScheduledExecution(ctx, "test_db.nonexistent_table")
	require.NoError(t, err)
	assert.Nil(t, lastExec)
}

func TestIntegration_BoundsCacheWithRealRedis(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	now := time.Now().UTC()

	// Set bounds
	cache := &BoundsCache{
		ModelID:             "test_db.bounds_table",
		Min:                 100,
		Max:                 1000,
		LastIncrementalScan: now,
		LastFullScan:        now,
		PreviousMin:         50,
		PreviousMax:         500,
		InitialScanComplete: true,
		UpdatedAt:           now,
	}

	err := svc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	// Get bounds
	retrieved, err := svc.GetExternalBounds(ctx, "test_db.bounds_table")
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, "test_db.bounds_table", retrieved.ModelID)
	assert.Equal(t, uint64(100), retrieved.Min)
	assert.Equal(t, uint64(1000), retrieved.Max)
	assert.True(t, retrieved.InitialScanComplete)
}

func TestIntegration_BoundsLockWithRealRedis(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Acquire lock
	lock, err := svc.AcquireBoundsLock(ctx, "test_db.lock_table")
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Release lock
	err = lock.Unlock(ctx)
	require.NoError(t, err)
}

func TestIntegration_ConsolidateHistoricalData(t *testing.T) {
	svc, _ := setupIntegrationService(t)
	ctx := context.Background()

	// Record many contiguous completions that can be consolidated
	for i := uint64(0); i < 10; i++ {
		err := svc.RecordCompletion(ctx, "test_db.consolidate_table", i*100, 100)
		require.NoError(t, err)
	}

	// Consolidate
	rowCount, err := svc.ConsolidateHistoricalData(ctx, "test_db.consolidate_table")
	require.NoError(t, err)

	// Should have consolidated multiple rows
	assert.Greater(t, rowCount, uint64(1))
}

func TestIntegration_AdminTableInfo(t *testing.T) {
	svc, _ := setupIntegrationService(t)

	assert.Equal(t, testDatabase, svc.GetIncrementalAdminDatabase())
	assert.Equal(t, testTable, svc.GetIncrementalAdminTable())
	assert.Equal(t, testSchedDB, svc.GetScheduledAdminDatabase())
	assert.Equal(t, testSchedTbl, svc.GetScheduledAdminTable())
}
