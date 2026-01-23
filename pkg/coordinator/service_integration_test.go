//go:build integration

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSourceDB    = "source"
	testSourceTable = "source_table"
	testTargetDB    = "target"
	testTargetTable = "target_table"
)

// testValidator implements the validation.Validator interface for testing.
type testValidator struct {
	validateResult      validation.Result
	validRangeMin       uint64
	validRangeMax       uint64
	startPosition       uint64
	validateErr         error
	validRangeErr       error
	startPositionErr    error
	validateCalled      bool
	validRangeCalled    bool
	startPositionCalled bool
}

func (v *testValidator) ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (validation.Result, error) {
	v.validateCalled = true
	return v.validateResult, v.validateErr
}

func (v *testValidator) GetValidRange(ctx context.Context, modelID string, semantics validation.RangeSemantics) (minPos, maxPos uint64, err error) {
	v.validRangeCalled = true
	return v.validRangeMin, v.validRangeMax, v.validRangeErr
}

func (v *testValidator) GetStartPosition(ctx context.Context, modelID string) (uint64, error) {
	v.startPositionCalled = true
	return v.startPosition, v.startPositionErr
}

// setupIntegrationCoordinator creates a coordinator service with real ClickHouse and Redis.
func setupIntegrationCoordinator(t *testing.T) (Service, admin.Service, *testutil.RedisConnection, *testValidator, clickhouse.ClientInterface) {
	t.Helper()

	chConn := testutil.NewClickHouseContainer(t)
	redisConn := testutil.NewRedisContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          chConn.URL,
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
	testutil.CreateAdminTables(t, client)

	// Create source and target tables
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 1000, 1)
	testutil.CreateTargetTable(t, client, testTargetDB, testTargetTable)

	// Create admin service
	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "coordinator"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create external model for bounds
	externalSQL := testutil.DefaultExternalBoundsSQL(testSourceDB, testSourceTable)
	externalModel, err := testutil.NewTestExternalSQL(testSourceDB, testSourceTable, externalSQL)
	require.NoError(t, err)

	// Create transformation
	transformSQL := testutil.DefaultTransformationSQL(testSourceDB, testSourceTable)
	transformModel, err := testutil.NewTestTransformationSQL(
		testTargetDB, testTargetTable, transformSQL,
		testutil.WithDependencies(testSourceDB+"."+testSourceTable),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	// Build DAG
	dag := testutil.TestDAG([]models.Transformation{transformModel}, []models.External{externalModel})

	// Create test validator
	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
		validRangeMin:  0,
		validRangeMax:  1000,
		startPosition:  0,
	}

	// Create coordinator service
	svc, err := NewService(
		logger.WithField("test", "coordinator"),
		redisConn.Options,
		dag,
		adminSvc,
		validator,
	)
	require.NoError(t, err)

	ctx := context.Background()
	err = svc.Start(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := svc.Stop(); err != nil {
			t.Logf("failed to stop coordinator: %v", err)
		}
	})

	return svc, adminSvc, redisConn, validator, client
}

func TestIntegration_ProcessForward_InitialPosition(t *testing.T) {
	_, adminSvc, _, validator, client := setupIntegrationCoordinator(t)
	ctx := context.Background()

	// Create transformation for testing
	transformSQL := testutil.DefaultTransformationSQL(testSourceDB, testSourceTable)
	transformModel, err := testutil.NewTestTransformationSQL(
		testTargetDB, testTargetTable, transformSQL,
		testutil.WithDependencies(testSourceDB+"."+testSourceTable),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	// Setup external bounds in cache (simulating external scan)
	cache := &admin.BoundsCache{
		ModelID:             testSourceDB + "." + testSourceTable,
		Min:                 0,
		Max:                 1000,
		InitialScanComplete: true,
		UpdatedAt:           time.Now().UTC(),
	}
	err = adminSvc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	// Verify initially no position recorded
	nextPos, err := adminSvc.GetNextUnprocessedPosition(ctx, testTargetDB+"."+testTargetTable)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), nextPos)

	// Validator should be configured for initial position calculation
	assert.Equal(t, uint64(0), validator.startPosition)

	// Verify transformation is valid
	config := transformModel.GetConfig()
	assert.Equal(t, testTargetDB, config.Database)
	assert.Equal(t, testTargetTable, config.Table)

	// Clean up
	_ = client
}

func TestIntegration_ProcessForward_NextPosition(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testTargetDB + "." + testTargetTable

	// Record some completions
	err := adminSvc.RecordCompletion(ctx, modelID, 0, 100)
	require.NoError(t, err)

	err = adminSvc.RecordCompletion(ctx, modelID, 100, 100)
	require.NoError(t, err)

	// Next unprocessed should be 200
	nextPos, err := adminSvc.GetNextUnprocessedPosition(ctx, modelID)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), nextPos)
}

func TestIntegration_ProcessBack_FindsGaps(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testTargetDB + "." + testTargetTable

	// Record completions with a gap: 0-100, (missing 100-200), 200-300
	err := adminSvc.RecordCompletion(ctx, modelID, 0, 100)
	require.NoError(t, err)

	err = adminSvc.RecordCompletion(ctx, modelID, 200, 100)
	require.NoError(t, err)

	// Find gaps
	gaps, err := adminSvc.FindGaps(ctx, modelID, 0, 300, 100)
	require.NoError(t, err)

	// Should find gap from 100-200
	require.Len(t, gaps, 1)
	assert.Equal(t, uint64(100), gaps[0].StartPos)
	assert.Equal(t, uint64(200), gaps[0].EndPos)
}

func TestIntegration_ProcessBack_FillsFromEnd(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testTargetDB + "." + testTargetTable

	// Record completions with multiple gaps
	err := adminSvc.RecordCompletion(ctx, modelID, 0, 100)
	require.NoError(t, err)

	// Leave gap 100-300

	err = adminSvc.RecordCompletion(ctx, modelID, 300, 100)
	require.NoError(t, err)

	// Leave gap 400-600

	err = adminSvc.RecordCompletion(ctx, modelID, 600, 100)
	require.NoError(t, err)

	// Find gaps - should be ordered by position DESC (most recent first)
	gaps, err := adminSvc.FindGaps(ctx, modelID, 0, 700, 100)
	require.NoError(t, err)

	// Should find 2 gaps
	require.Len(t, gaps, 2)

	// Gaps should be ordered DESC (most recent first)
	// First gap should be 400-600
	assert.Equal(t, uint64(400), gaps[0].StartPos)
	assert.Equal(t, uint64(600), gaps[0].EndPos)

	// Second gap should be 100-300
	assert.Equal(t, uint64(100), gaps[1].StartPos)
	assert.Equal(t, uint64(300), gaps[1].EndPos)
}

func TestIntegration_EnqueueExternalScan_Incremental(t *testing.T) {
	svc, _, _, _, _ := setupIntegrationCoordinator(t)

	// Process external scan
	svc.ProcessExternalScan(testSourceDB+"."+testSourceTable, "incremental")

	// This should not error - task will be enqueued to Redis
	// We can't easily verify the task was enqueued without consuming it,
	// but we can verify the call doesn't panic
}

func TestIntegration_EnqueueExternalScan_Full(t *testing.T) {
	svc, _, _, _, _ := setupIntegrationCoordinator(t)

	// Process full external scan
	svc.ProcessExternalScan(testSourceDB+"."+testSourceTable, "full")

	// This should not error - task will be enqueued to Redis
}

func TestIntegration_GetCoverage(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testTargetDB + "." + testTargetTable

	// Record a completion covering 100-200
	err := adminSvc.RecordCompletion(ctx, modelID, 100, 100)
	require.NoError(t, err)

	// Check coverage for covered range
	covered, err := adminSvc.GetCoverage(ctx, modelID, 100, 200)
	require.NoError(t, err)
	assert.True(t, covered)

	// Check coverage for uncovered range
	covered, err = adminSvc.GetCoverage(ctx, modelID, 0, 50)
	require.NoError(t, err)
	assert.False(t, covered)
}

func TestIntegration_BoundsCache(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// Set bounds
	now := time.Now().UTC()
	cache := &admin.BoundsCache{
		ModelID:             modelID,
		Min:                 100,
		Max:                 1000,
		LastIncrementalScan: now,
		LastFullScan:        now,
		PreviousMin:         50,
		PreviousMax:         500,
		InitialScanComplete: true,
		UpdatedAt:           now,
	}

	err := adminSvc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	// Get bounds
	retrieved, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, modelID, retrieved.ModelID)
	assert.Equal(t, uint64(100), retrieved.Min)
	assert.Equal(t, uint64(1000), retrieved.Max)
	assert.True(t, retrieved.InitialScanComplete)
}

func TestIntegration_BoundsLock(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// Acquire lock
	lock, err := adminSvc.AcquireBoundsLock(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Release lock
	err = lock.Unlock(ctx)
	require.NoError(t, err)
}

func TestIntegration_ConsolidateHistoricalData(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testTargetDB + "." + testTargetTable

	// Record many contiguous completions that can be consolidated
	for i := uint64(0); i < 10; i++ {
		err := adminSvc.RecordCompletion(ctx, modelID, i*100, 100)
		require.NoError(t, err)
	}

	// Consolidate
	rowCount, err := adminSvc.ConsolidateHistoricalData(ctx, modelID)
	require.NoError(t, err)

	// Should have consolidated multiple rows
	assert.Greater(t, rowCount, uint64(1))
}

func TestIntegration_ProcessedRanges(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)
	ctx := context.Background()

	modelID := testTargetDB + "." + testTargetTable

	// Record some completions
	err := adminSvc.RecordCompletion(ctx, modelID, 0, 100)
	require.NoError(t, err)

	err = adminSvc.RecordCompletion(ctx, modelID, 100, 100)
	require.NoError(t, err)

	err = adminSvc.RecordCompletion(ctx, modelID, 200, 100)
	require.NoError(t, err)

	// Get processed ranges
	ranges, err := adminSvc.GetProcessedRanges(ctx, modelID)
	require.NoError(t, err)

	// Should have 3 ranges ordered by position DESC
	require.Len(t, ranges, 3)
	assert.Equal(t, uint64(200), ranges[0].Position)
	assert.Equal(t, uint64(100), ranges[1].Position)
	assert.Equal(t, uint64(0), ranges[2].Position)
}

func TestIntegration_AdminTableInfo(t *testing.T) {
	_, adminSvc, _, _, _ := setupIntegrationCoordinator(t)

	assert.Equal(t, "admin", adminSvc.GetIncrementalAdminDatabase())
	assert.Equal(t, "cbt_incremental", adminSvc.GetIncrementalAdminTable())
	assert.Equal(t, "admin", adminSvc.GetScheduledAdminDatabase())
	assert.Equal(t, "cbt_scheduled", adminSvc.GetScheduledAdminTable())
}

func TestIntegration_ServiceLifecycle(t *testing.T) {
	chConn := testutil.NewClickHouseContainer(t)
	redisConn := testutil.NewRedisContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          chConn.URL,
		QueryTimeout: 30 * time.Second,
	}
	cfg.SetDefaults()

	client, err := clickhouse.NewClient(logger, cfg)
	require.NoError(t, err)

	err = client.Start()
	require.NoError(t, err)
	defer func() { _ = client.Stop() }()

	testutil.CreateAdminTables(t, client)

	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "coordinator"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Empty DAG for this test
	dag := testutil.TestDAG(nil, nil)

	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
	}

	// Create service
	svc, err := NewService(
		logger.WithField("test", "coordinator"),
		redisConn.Options,
		dag,
		adminSvc,
		validator,
	)
	require.NoError(t, err)

	// Start service
	ctx := context.Background()
	err = svc.Start(ctx)
	require.NoError(t, err)

	// Stop service
	err = svc.Stop()
	require.NoError(t, err)
}

func TestIntegration_DetermineIntervalForGap(t *testing.T) {
	tests := []struct {
		name        string
		gapSize     uint64
		minInterval uint64
		maxInterval uint64
		expected    uint64
	}{
		{
			name:        "gap_smaller_than_max",
			gapSize:     50,
			minInterval: 10,
			maxInterval: 100,
			expected:    50,
		},
		{
			name:        "gap_larger_than_max",
			gapSize:     200,
			minInterval: 10,
			maxInterval: 100,
			expected:    100,
		},
		{
			name:        "gap_smaller_than_min",
			gapSize:     5,
			minInterval: 10,
			maxInterval: 100,
			expected:    10,
		},
		{
			name:        "gap_equals_max",
			gapSize:     100,
			minInterval: 10,
			maxInterval: 100,
			expected:    100,
		},
		{
			name:        "min_interval_zero",
			gapSize:     50,
			minInterval: 0,
			maxInterval: 100,
			expected:    50,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := determineIntervalForGap(tc.gapSize, tc.minInterval, tc.maxInterval)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// ============================================================================
// Enhanced Integration Tests with Real-World Patterns
// ============================================================================

const (
	testEventsSourceDB  = "source"
	testEventsSource    = "events_source"
	testAggregatedDB    = "transform"
	testAggregatedTable = "events_aggregated"
	testByAccountDB     = "transform"
	testByAccountTable  = "events_by_account"
)

// setupChainedCoordinator creates a coordinator with a transformation chain for testing.
func setupChainedCoordinator(t *testing.T) (Service, admin.Service, *testutil.RedisConnection, *testValidator, clickhouse.ClientInterface) {
	t.Helper()

	chConn := testutil.NewClickHouseContainer(t)
	redisConn := testutil.NewRedisContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          chConn.URL,
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
	testutil.CreateAdminTables(t, client)

	// Create events source table
	testutil.CreateEventsSourceTable(t, client, testEventsSourceDB, testEventsSource, 500, 1)

	// Create aggregated and by_account tables
	testutil.CreateEventsAggregatedTable(t, client, testAggregatedDB, testAggregatedTable)
	testutil.CreateEventsByAccountTable(t, client, testByAccountDB, testByAccountTable)

	// Create admin service
	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "coordinator"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create transformation chain
	chainCfg := testutil.DefaultTestChainConfig()
	external, level1, level2, dag, err := testutil.NewTestTransformationChain(chainCfg)
	require.NoError(t, err)

	// Set up external bounds
	cache := &admin.BoundsCache{
		ModelID:             external.GetID(),
		Min:                 0,
		Max:                 500,
		InitialScanComplete: true,
		UpdatedAt:           time.Now().UTC(),
	}
	err = adminSvc.SetExternalBounds(context.Background(), cache)
	require.NoError(t, err)

	// Create test validator
	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
		validRangeMin:  0,
		validRangeMax:  500,
		startPosition:  0,
	}

	// Create coordinator service with the chain DAG
	svc, err := NewService(
		logger.WithField("test", "coordinator"),
		redisConn.Options,
		dag,
		adminSvc,
		validator,
	)
	require.NoError(t, err)

	ctx := context.Background()
	err = svc.Start(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := svc.Stop(); err != nil {
			t.Logf("failed to stop coordinator: %v", err)
		}
	})

	// Store models for reference
	_ = level1
	_ = level2

	return svc, adminSvc, redisConn, validator, client
}

func TestIntegration_ProcessForward_ChainedTransformations(t *testing.T) {
	_, adminSvc, _, _, _ := setupChainedCoordinator(t)
	ctx := context.Background()

	level1ID := testAggregatedDB + "." + testAggregatedTable
	level2ID := testByAccountDB + "." + testByAccountTable

	// Record completions for level 1 (events_aggregated)
	for position := uint64(0); position < 300; position += 100 {
		err := adminSvc.RecordCompletion(ctx, level1ID, position, 100)
		require.NoError(t, err)
	}

	// Verify level 1 progress
	nextPosLevel1, err := adminSvc.GetNextUnprocessedPosition(ctx, level1ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(300), nextPosLevel1)

	// Level 2 should start from 0 (no completions yet)
	nextPosLevel2, err := adminSvc.GetNextUnprocessedPosition(ctx, level2ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), nextPosLevel2)

	// Record completions for level 2 (events_by_account)
	for position := uint64(0); position < 200; position += 100 {
		err := adminSvc.RecordCompletion(ctx, level2ID, position, 100)
		require.NoError(t, err)
	}

	// Verify level 2 progress
	nextPosLevel2, err = adminSvc.GetNextUnprocessedPosition(ctx, level2ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), nextPosLevel2)
}

func TestIntegration_ProcessBack_NestedDependencies(t *testing.T) {
	_, adminSvc, _, _, _ := setupChainedCoordinator(t)
	ctx := context.Background()

	level1ID := testAggregatedDB + "." + testAggregatedTable
	level2ID := testByAccountDB + "." + testByAccountTable

	// Record completions for level 1 with a gap: 0-100, (missing 100-200), 200-300
	err := adminSvc.RecordCompletion(ctx, level1ID, 0, 100)
	require.NoError(t, err)

	err = adminSvc.RecordCompletion(ctx, level1ID, 200, 100)
	require.NoError(t, err)

	// Find gaps in level 1
	gaps, err := adminSvc.FindGaps(ctx, level1ID, 0, 300, 100)
	require.NoError(t, err)
	require.Len(t, gaps, 1)
	assert.Equal(t, uint64(100), gaps[0].StartPos)
	assert.Equal(t, uint64(200), gaps[0].EndPos)

	// Record completions for level 2 that depend on level 1
	err = adminSvc.RecordCompletion(ctx, level2ID, 0, 100)
	require.NoError(t, err)

	// Verify level 2 next position considers the dependency gap
	nextPosLevel2, err := adminSvc.GetNextUnprocessedPosition(ctx, level2ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), nextPosLevel2)
}

func TestIntegration_DAG_TransformationDependencies(t *testing.T) {
	chConn := testutil.NewClickHouseContainer(t)
	redisConn := testutil.NewRedisContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          chConn.URL,
		QueryTimeout: 30 * time.Second,
	}
	cfg.SetDefaults()

	client, err := clickhouse.NewClient(logger, cfg)
	require.NoError(t, err)

	err = client.Start()
	require.NoError(t, err)
	defer func() { _ = client.Stop() }()

	testutil.CreateAdminTables(t, client)

	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "coordinator"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create transformation chain
	chainCfg := testutil.DefaultTestChainConfig()
	external, level1, level2, dag, err := testutil.NewTestTransformationChain(chainCfg)
	require.NoError(t, err)

	// Verify DAG structure
	// Level 1 should depend on external
	level1Deps := dag.GetDependencies(level1.GetID())
	assert.Len(t, level1Deps, 1)
	assert.Equal(t, external.GetID(), level1Deps[0])

	// Level 2 should depend on level 1
	level2Deps := dag.GetDependencies(level2.GetID())
	assert.Len(t, level2Deps, 1)
	assert.Equal(t, level1.GetID(), level2Deps[0])

	// Level 1 should be dependent of external
	externalDependents := dag.GetDependents(external.GetID())
	assert.Contains(t, externalDependents, level1.GetID())

	// Level 2 should be dependent of level 1
	level1Dependents := dag.GetDependents(level1.GetID())
	assert.Contains(t, level1Dependents, level2.GetID())

	// Verify transitive dependencies
	allLevel2Deps := dag.GetAllDependencies(level2.GetID())
	assert.Len(t, allLevel2Deps, 2) // Both level1 and external
	assert.Contains(t, allLevel2Deps, level1.GetID())
	assert.Contains(t, allLevel2Deps, external.GetID())

	// Verify path exists from external to level 2
	assert.True(t, dag.IsPathBetween(external.GetID(), level2.GetID()))

	// Cleanup
	_ = adminSvc
}

func TestIntegration_ProcessForward_MultiLevelChainBounds(t *testing.T) {
	_, adminSvc, _, _, _ := setupChainedCoordinator(t)
	ctx := context.Background()

	level1ID := testAggregatedDB + "." + testAggregatedTable
	level2ID := testByAccountDB + "." + testByAccountTable

	// Test that level 2 cannot process beyond what level 1 has completed
	// Simulate level 1 completing only up to position 200
	for position := uint64(0); position < 200; position += 100 {
		err := adminSvc.RecordCompletion(ctx, level1ID, position, 100)
		require.NoError(t, err)
	}

	// Record level 2 completion up to its available range
	err := adminSvc.RecordCompletion(ctx, level2ID, 0, 100)
	require.NoError(t, err)

	// Verify next unprocessed for level 2
	nextPos, err := adminSvc.GetNextUnprocessedPosition(ctx, level2ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), nextPos)

	// Verify coverage check
	covered, err := adminSvc.GetCoverage(ctx, level1ID, 0, 200)
	require.NoError(t, err)
	assert.True(t, covered)

	// Beyond level 1's completion is not covered
	covered, err = adminSvc.GetCoverage(ctx, level1ID, 200, 300)
	require.NoError(t, err)
	assert.False(t, covered)
}

func TestIntegration_ConsolidateHistoricalData_Chain(t *testing.T) {
	_, adminSvc, _, _, _ := setupChainedCoordinator(t)
	ctx := context.Background()

	level1ID := testAggregatedDB + "." + testAggregatedTable

	// Record many contiguous completions
	for i := uint64(0); i < 20; i++ {
		err := adminSvc.RecordCompletion(ctx, level1ID, i*100, 100)
		require.NoError(t, err)
	}

	// Consolidate
	rowCount, err := adminSvc.ConsolidateHistoricalData(ctx, level1ID)
	require.NoError(t, err)
	assert.Greater(t, rowCount, uint64(1), "Should have consolidated multiple rows")

	// Verify data integrity after consolidation
	ranges, err := adminSvc.GetProcessedRanges(ctx, level1ID)
	require.NoError(t, err)

	// The ranges should still cover the full processed area
	// After consolidation, we should have fewer rows but same coverage
	totalCovered := uint64(0)
	for _, r := range ranges {
		totalCovered += r.Interval
	}
	assert.GreaterOrEqual(t, totalCovered, uint64(2000), "Coverage should be maintained after consolidation")
}

func TestIntegration_BoundsCache_WithChainedDependencies(t *testing.T) {
	_, adminSvc, _, _, _ := setupChainedCoordinator(t)
	ctx := context.Background()

	externalID := testEventsSourceDB + "." + testEventsSource

	// Update external bounds (simulating new data arrival)
	now := time.Now().UTC()
	cache := &admin.BoundsCache{
		ModelID:             externalID,
		Min:                 0,
		Max:                 1000, // Extended from 500 to 1000
		LastIncrementalScan: now,
		LastFullScan:        now,
		PreviousMin:         0,
		PreviousMax:         500,
		InitialScanComplete: true,
		UpdatedAt:           now,
	}

	err := adminSvc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	// Retrieve and verify
	retrieved, err := adminSvc.GetExternalBounds(ctx, externalID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, uint64(0), retrieved.Min)
	assert.Equal(t, uint64(1000), retrieved.Max)
	assert.Equal(t, uint64(500), retrieved.PreviousMax)
	assert.True(t, retrieved.InitialScanComplete)
}
