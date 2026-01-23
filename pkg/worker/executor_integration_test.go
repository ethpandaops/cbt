//go:build integration

package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/tasks"
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

// setupIntegrationExecutor creates a ModelExecutor with real ClickHouse and Redis.
func setupIntegrationExecutor(t *testing.T) (*ModelExecutor, clickhouse.ClientInterface, admin.Service, *testutil.RedisConnection) {
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

	// Create the admin service
	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "worker"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create test tables
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 1000, 1)
	testutil.CreateTargetTable(t, client, testTargetDB, testTargetTable)

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

	// Create models service mock
	modelsService := &testModelsService{
		dag:             dag,
		transformation:  transformModel,
		externalModel:   externalModel,
		clickhouseURL:   chConn.URL,
		clickhouseDB:    "test_db",
		logger:          logger,
		externalModelID: testSourceDB + "." + testSourceTable,
		globalEnv:       make(map[string]string),
		clickhouseCfg:   cfg,
	}

	executor := NewModelExecutor(
		logger.WithField("test", "executor"),
		client,
		modelsService,
		adminSvc,
	)

	return executor, client, adminSvc, redisConn
}

// testModelsService is a simple models.Service implementation for testing.
type testModelsService struct {
	dag             models.DAGReader
	transformation  models.Transformation
	externalModel   models.External
	clickhouseURL   string
	clickhouseDB    string
	logger          *logrus.Logger
	externalModelID string
	globalEnv       map[string]string
	clickhouseCfg   *clickhouse.Config
}

func (s *testModelsService) Start() error { return nil }
func (s *testModelsService) Stop() error  { return nil }
func (s *testModelsService) GetDAG() models.DAGReader {
	return s.dag
}

func (s *testModelsService) RenderTransformation(model models.Transformation, position, interval uint64, _ time.Time) (string, error) {
	config := model.GetConfig()
	sql := model.GetValue()

	// Simple template replacement for testing
	sql = replaceTemplateVar(sql, "{{ .database }}", config.Database)
	sql = replaceTemplateVar(sql, "{{ .table }}", config.Table)
	sql = replaceTemplateVar(sql, "{{ .bounds.start }}", uintToStr(position))
	sql = replaceTemplateVar(sql, "{{ .bounds.end }}", uintToStr(position+interval))

	return sql, nil
}

func (s *testModelsService) RenderExternal(model models.External, cacheState map[string]interface{}) (string, error) {
	// Use real template engine to render external models with sprig functions
	engine := models.NewTemplateEngine(s.clickhouseCfg, s.dag.(*models.DependencyGraph), s.globalEnv)
	return engine.RenderExternal(model, cacheState)
}

func (s *testModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	return &[]string{}, nil
}

// replaceTemplateVar performs simple string replacement.
func replaceTemplateVar(s, old, new string) string {
	for {
		idx := indexOf(s, old)
		if idx < 0 {
			return s
		}
		s = s[:idx] + new + s[idx+len(old):]
	}
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func uintToStr(n uint64) string {
	if n == 0 {
		return "0"
	}
	var result []byte
	for n > 0 {
		result = append([]byte{byte('0' + n%10)}, result...)
		n /= 10
	}
	return string(result)
}

func TestIntegration_ExecuteSQL_SimpleTransformation(t *testing.T) {
	executor, client, _, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	// Get transformation from the test models service
	modelsService := executor.models.(*testModelsService)
	transformation := modelsService.transformation

	// Create task context
	taskCtx := &tasks.TaskContext{
		Transformation: transformation,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}

	// Execute the transformation
	err := executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Verify rows were inserted into target table
	rowCount := testutil.GetRowCount(t, client, testTargetDB, testTargetTable)
	assert.Equal(t, uint64(100), rowCount)
}

func TestIntegration_ExecuteSQL_MultipleIntervals(t *testing.T) {
	executor, client, _, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelsService := executor.models.(*testModelsService)
	transformation := modelsService.transformation

	// Execute multiple intervals
	for position := uint64(0); position < 300; position += 100 {
		taskCtx := &tasks.TaskContext{
			Transformation: transformation,
			Position:       position,
			Interval:       100,
			StartTime:      time.Now(),
			WorkerID:       "test-worker",
		}

		err := executor.Execute(ctx, taskCtx)
		require.NoError(t, err)
	}

	// Verify all rows were inserted
	rowCount := testutil.GetRowCount(t, client, testTargetDB, testTargetTable)
	assert.Equal(t, uint64(300), rowCount)
}

func TestIntegration_ExecuteSQL_TableNotExists(t *testing.T) {
	executor, _, _, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	// Create a transformation pointing to non-existent table
	transformSQL := `INSERT INTO nonexistent.table SELECT 1`
	transformModel, err := testutil.NewTestTransformationSQL(
		"nonexistent", "table", transformSQL,
		testutil.WithDependencies(testSourceDB+"."+testSourceTable),
	)
	require.NoError(t, err)

	taskCtx := &tasks.TaskContext{
		Transformation: transformModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}

	// Execute should fail with table not exists error
	err = executor.Execute(ctx, taskCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table does not exist")
}

func TestIntegration_Validate_TableExists(t *testing.T) {
	executor, _, _, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelsService := executor.models.(*testModelsService)
	transformation := modelsService.transformation

	taskCtx := &tasks.TaskContext{
		Transformation: transformation,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}

	// Validate should pass for existing table
	err := executor.Validate(ctx, taskCtx)
	require.NoError(t, err)
}

func TestIntegration_UpdateBounds_FullScan(t *testing.T) {
	executor, _, adminSvc, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// Execute full scan
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	// Verify bounds were cached
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Source table has positions 0-999
	assert.Equal(t, uint64(0), cache.Min)
	assert.Equal(t, uint64(999), cache.Max)
	assert.True(t, cache.InitialScanComplete)
}

func TestIntegration_UpdateBounds_IncrementalScan(t *testing.T) {
	executor, client, adminSvc, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// First do a full scan
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	// Add more data to source table
	insertSQL := `INSERT INTO ` + testSourceDB + `.` + testSourceTable + ` (position, value)
		SELECT number + 1000, concat('test_value_', toString(number + 1000))
		FROM numbers(500)`
	err = client.Execute(ctx, insertSQL)
	require.NoError(t, err)

	// Execute incremental scan
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	// Verify bounds were updated
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Should now include new data (positions up to 1499)
	assert.Equal(t, uint64(0), cache.Min)
	assert.Equal(t, uint64(1499), cache.Max)
}

func TestIntegration_UpdateBounds_ZeroProtection(t *testing.T) {
	executor, client, adminSvc, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// First do a full scan to populate cache
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	// Get initial bounds
	initialCache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, initialCache)

	// Truncate source table (simulates empty source)
	testutil.TruncateTable(t, client, testSourceDB, testSourceTable)

	// Incremental scan with empty source should preserve existing bounds
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	// Verify bounds were preserved (zero protection)
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	assert.Equal(t, initialCache.Min, cache.Min)
	assert.Equal(t, initialCache.Max, cache.Max)
}

func TestIntegration_UpdateBounds_DistributedLock(t *testing.T) {
	_, _, adminSvc, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// Test that distributed lock can be acquired and released
	lock, err := adminSvc.AcquireBoundsLock(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Release the lock
	err = lock.Unlock(ctx)
	require.NoError(t, err)

	// Should be able to acquire again
	lock2, err := adminSvc.AcquireBoundsLock(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, lock2)

	err = lock2.Unlock(ctx)
	require.NoError(t, err)
}

func TestIntegration_UpdateBounds_IncrementalWithoutInitialScan(t *testing.T) {
	executor, _, adminSvc, _ := setupIntegrationExecutor(t)
	ctx := context.Background()

	modelID := testSourceDB + "." + testSourceTable

	// Try incremental scan without initial full scan - should be skipped
	err := executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	// Verify no cache was created (incremental skipped without initial scan)
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	assert.Nil(t, cache)
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
	testWithNextDB      = "transform"
	testWithNextTable   = "events_with_next"
	testHelperDB        = "transform"
	testHelperTable     = "helper_latest_state"
)

// setupEventsIntegrationExecutor creates an executor with realistic events-based tables.
func setupEventsIntegrationExecutor(t *testing.T) (*ModelExecutor, clickhouse.ClientInterface, admin.Service, *testutil.RedisConnection) {
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

	// Create admin service
	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "worker"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create events source table with 500 positions (5 accounts each = 2500 rows)
	testutil.CreateEventsSourceTable(t, client, testEventsSourceDB, testEventsSource, 500, 1)

	// Create external model
	externalSQL := testutil.DefaultExternalBoundsSQL(testEventsSourceDB, testEventsSource)
	externalModel, err := testutil.NewTestExternalSQL(testEventsSourceDB, testEventsSource, externalSQL)
	require.NoError(t, err)

	// Create empty DAG (will be configured per-test)
	dag := testutil.TestDAG(nil, []models.External{externalModel})

	modelsService := &testModelsService{
		dag:             dag,
		externalModel:   externalModel,
		clickhouseURL:   chConn.URL,
		clickhouseDB:    "test_db",
		logger:          logger,
		externalModelID: testEventsSourceDB + "." + testEventsSource,
		globalEnv:       make(map[string]string),
		clickhouseCfg:   cfg,
	}

	executor := NewModelExecutor(
		logger.WithField("test", "executor"),
		client,
		modelsService,
		adminSvc,
	)

	return executor, client, adminSvc, redisConn
}

func TestIntegration_ExecuteSQL_SimpleAggregation(t *testing.T) {
	executor, client, _, _ := setupEventsIntegrationExecutor(t)
	ctx := context.Background()

	// Create aggregated table
	testutil.CreateEventsAggregatedTable(t, client, testAggregatedDB, testAggregatedTable)

	// Create transformation
	transformSQL := testutil.EventsAggregatedSQL(testEventsSourceDB, testEventsSource)
	transformModel, err := testutil.NewTestTransformationSQL(
		testAggregatedDB, testAggregatedTable, transformSQL,
		testutil.WithDependencies(testEventsSourceDB+"."+testEventsSource),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	// Execute for position 0-100
	taskCtx := &tasks.TaskContext{
		Transformation: transformModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}

	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Verify aggregated rows: 100 positions * 5 accounts = 500 rows
	rowCount := testutil.GetAggregatedRowCount(t, client, testAggregatedDB, testAggregatedTable)
	assert.Equal(t, uint64(500), rowCount)

	// Verify distinct accounts
	accountCount := testutil.GetDistinctAccountCount(t, client, testAggregatedDB, testAggregatedTable)
	assert.Equal(t, uint64(5), accountCount)
}

func TestIntegration_ExecuteSQL_CumulativeState(t *testing.T) {
	executor, client, _, _ := setupEventsIntegrationExecutor(t)
	ctx := context.Background()

	// Create aggregated table first (as dependency)
	testutil.CreateEventsAggregatedTable(t, client, testAggregatedDB, testAggregatedTable)

	// Populate aggregated table with some data first
	aggregateSQL := testutil.EventsAggregatedSQL(testEventsSourceDB, testEventsSource)
	aggregateModel, err := testutil.NewTestTransformationSQL(
		testAggregatedDB, testAggregatedTable, aggregateSQL,
		testutil.WithDependencies(testEventsSourceDB+"."+testEventsSource),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	// Execute aggregation for positions 0-200
	for position := uint64(0); position < 200; position += 100 {
		taskCtx := &tasks.TaskContext{
			Transformation: aggregateModel,
			Position:       position,
			Interval:       100,
			StartTime:      time.Now(),
			WorkerID:       "test-worker",
		}
		err = executor.Execute(ctx, taskCtx)
		require.NoError(t, err)
	}

	// Create cumulative state table
	testutil.CreateEventsByAccountTable(t, client, testByAccountDB, testByAccountTable)

	// Create cumulative transformation
	cumulativeModel, err := testutil.NewTestCumulativeTransformation(
		testByAccountDB, testByAccountTable,
		testAggregatedDB, testAggregatedTable,
		testutil.WithDependencies(testAggregatedDB+"."+testAggregatedTable),
	)
	require.NoError(t, err)

	// Execute cumulative transformation for first interval (0-100)
	taskCtx := &tasks.TaskContext{
		Transformation: cumulativeModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}
	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Verify rows were created
	rowCount := testutil.GetAggregatedRowCount(t, client, testByAccountDB, testByAccountTable)
	assert.Greater(t, rowCount, uint64(0))

	// Get running total for account_0 at position 99
	runningTotal1 := testutil.GetRunningTotal(t, client, testByAccountDB, testByAccountTable, "account_0", 99)

	// Execute cumulative transformation for second interval (100-200)
	taskCtx = &tasks.TaskContext{
		Transformation: cumulativeModel,
		Position:       100,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}
	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Get running total for account_0 at position 199 (should be higher due to cumulative)
	runningTotal2 := testutil.GetRunningTotal(t, client, testByAccountDB, testByAccountTable, "account_0", 199)

	// Cumulative state should be maintained across intervals
	assert.GreaterOrEqual(t, runningTotal2, runningTotal1, "Running total should accumulate across intervals")
}

func TestIntegration_ExecuteSQL_WindowFunctions(t *testing.T) {
	executor, client, _, _ := setupEventsIntegrationExecutor(t)
	ctx := context.Background()

	// Create aggregated table first
	testutil.CreateEventsAggregatedTable(t, client, testAggregatedDB, testAggregatedTable)

	// Populate aggregated table
	aggregateSQL := testutil.EventsAggregatedSQL(testEventsSourceDB, testEventsSource)
	aggregateModel, err := testutil.NewTestTransformationSQL(
		testAggregatedDB, testAggregatedTable, aggregateSQL,
		testutil.WithDependencies(testEventsSourceDB+"."+testEventsSource),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	taskCtx := &tasks.TaskContext{
		Transformation: aggregateModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}
	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Create events_with_next table
	testutil.CreateEventsWithNextTable(t, client, testWithNextDB, testWithNextTable)

	// Create window function transformation
	windowModel, err := testutil.NewTestWindowFunctionTransformation(
		testWithNextDB, testWithNextTable,
		testAggregatedDB, testAggregatedTable,
		testutil.WithDependencies(testAggregatedDB+"."+testAggregatedTable),
	)
	require.NoError(t, err)

	// Execute window function transformation
	taskCtx = &tasks.TaskContext{
		Transformation: windowModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}
	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Verify rows were created with next_position values
	rowCount := testutil.GetAggregatedRowCount(t, client, testWithNextDB, testWithNextTable)
	assert.Greater(t, rowCount, uint64(0))
}

func TestIntegration_ExecuteSQL_MultiStatement(t *testing.T) {
	executor, client, _, _ := setupEventsIntegrationExecutor(t)
	ctx := context.Background()

	// Create aggregated table first
	testutil.CreateEventsAggregatedTable(t, client, testAggregatedDB, testAggregatedTable)

	// Populate aggregated table
	aggregateSQL := testutil.EventsAggregatedSQL(testEventsSourceDB, testEventsSource)
	aggregateModel, err := testutil.NewTestTransformationSQL(
		testAggregatedDB, testAggregatedTable, aggregateSQL,
		testutil.WithDependencies(testEventsSourceDB+"."+testEventsSource),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	taskCtx := &tasks.TaskContext{
		Transformation: aggregateModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}
	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Create target and helper tables
	testutil.CreateEventsWithNextTable(t, client, testWithNextDB, testWithNextTable)
	testutil.CreateHelperLatestStateTable(t, client, testHelperDB, testHelperTable)

	// Create multi-statement transformation
	multiModel, err := testutil.NewTestMultiStatementTransformation(
		testWithNextDB, testWithNextTable,
		testAggregatedDB, testAggregatedTable,
		testHelperDB, testHelperTable,
		testutil.WithDependencies(testAggregatedDB+"."+testAggregatedTable),
	)
	require.NoError(t, err)

	// Execute multi-statement transformation
	taskCtx = &tasks.TaskContext{
		Transformation: multiModel,
		Position:       0,
		Interval:       100,
		StartTime:      time.Now(),
		WorkerID:       "test-worker",
	}
	err = executor.Execute(ctx, taskCtx)
	require.NoError(t, err)

	// Verify main table was populated
	mainRowCount := testutil.GetAggregatedRowCount(t, client, testWithNextDB, testWithNextTable)
	assert.Greater(t, mainRowCount, uint64(0))

	// Verify helper table was also populated (by second statement)
	helperRowCount := testutil.GetRowCount(t, client, testHelperDB, testHelperTable)
	assert.Greater(t, helperRowCount, uint64(0))
}

func TestIntegration_ExecuteSQL_MultipleIntervalsWithCumulativeState(t *testing.T) {
	executor, client, _, _ := setupEventsIntegrationExecutor(t)
	ctx := context.Background()

	// Create aggregated table
	testutil.CreateEventsAggregatedTable(t, client, testAggregatedDB, testAggregatedTable)

	// Populate aggregated table with 300 positions worth of data
	aggregateSQL := testutil.EventsAggregatedSQL(testEventsSourceDB, testEventsSource)
	aggregateModel, err := testutil.NewTestTransformationSQL(
		testAggregatedDB, testAggregatedTable, aggregateSQL,
		testutil.WithDependencies(testEventsSourceDB+"."+testEventsSource),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	for position := uint64(0); position < 300; position += 100 {
		taskCtx := &tasks.TaskContext{
			Transformation: aggregateModel,
			Position:       position,
			Interval:       100,
			StartTime:      time.Now(),
			WorkerID:       "test-worker",
		}
		err = executor.Execute(ctx, taskCtx)
		require.NoError(t, err)
	}

	// Create cumulative state table
	testutil.CreateEventsByAccountTable(t, client, testByAccountDB, testByAccountTable)

	// Create cumulative transformation
	cumulativeModel, err := testutil.NewTestCumulativeTransformation(
		testByAccountDB, testByAccountTable,
		testAggregatedDB, testAggregatedTable,
		testutil.WithDependencies(testAggregatedDB+"."+testAggregatedTable),
	)
	require.NoError(t, err)

	// Execute cumulative transformation for 3 intervals
	var runningTotals []int64
	for position := uint64(0); position < 300; position += 100 {
		taskCtx := &tasks.TaskContext{
			Transformation: cumulativeModel,
			Position:       position,
			Interval:       100,
			StartTime:      time.Now(),
			WorkerID:       "test-worker",
		}
		err = executor.Execute(ctx, taskCtx)
		require.NoError(t, err)

		// Track running total at end of each interval
		rt := testutil.GetRunningTotal(t, client, testByAccountDB, testByAccountTable, "account_0", position+99)
		runningTotals = append(runningTotals, rt)
	}

	// Verify running totals are monotonically non-decreasing
	// (cumulative state is maintained across intervals)
	for i := 1; i < len(runningTotals); i++ {
		assert.GreaterOrEqual(t, runningTotals[i], runningTotals[i-1],
			"Running total should be maintained across intervals")
	}
}

// ============================================================================
// External Model Bounds Integration Tests
// ============================================================================

const (
	testNetworkEventsDB    = "source"
	testNetworkEventsTable = "network_events"
	testSlowSourceDB       = "source"
	testSlowSourceTable    = "slow_source"
)

// setupExternalBoundsExecutor creates an executor configured for external bounds testing.
func setupExternalBoundsExecutor(
	t *testing.T,
	database, table string,
	externalSQL string,
	env map[string]string,
	incrementalInterval, fullInterval time.Duration,
) (*ModelExecutor, clickhouse.ClientInterface, admin.Service, *testutil.RedisConnection) {
	t.Helper()

	chConn := testutil.NewClickHouseContainer(t)
	redisConn := testutil.NewRedisContainer(t)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	cfg := &clickhouse.Config{
		URL:          chConn.URL,
		QueryTimeout: 60 * time.Second,
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

	// Create admin service
	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "worker"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create external model with custom SQL and intervals
	externalModel, err := testutil.NewTestExternalSQLWithEnv(
		database, table, externalSQL, env,
		testutil.WithCacheIntervals(incrementalInterval, fullInterval),
	)
	require.NoError(t, err)

	// Build DAG with external model
	dag := testutil.TestDAG(nil, []models.External{externalModel})

	modelsService := &testModelsService{
		dag:             dag,
		externalModel:   externalModel,
		clickhouseURL:   chConn.URL,
		clickhouseDB:    "test_db",
		logger:          logger,
		externalModelID: database + "." + table,
		globalEnv:       env,
		clickhouseCfg:   cfg,
	}

	executor := NewModelExecutor(
		logger.WithField("test", "executor"),
		client,
		modelsService,
		adminSvc,
	)

	return executor, client, adminSvc, redisConn
}

func TestIntegration_UpdateBounds_SlowFullScan(t *testing.T) {
	// Create table with enough rows to make full scan noticeably slower
	externalSQL := testutil.SlowFullScanSQL(testSlowSourceDB, testSlowSourceTable)

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSlowSourceDB, testSlowSourceTable,
		externalSQL,
		map[string]string{},
		100*time.Millisecond, // incremental scan interval
		500*time.Millisecond, // full scan interval
	)
	ctx := context.Background()

	// Create slow source table with 500 rows (enough for measurable sleep delay)
	testutil.CreateSlowSourceTable(t, client, testSlowSourceDB, testSlowSourceTable, 500)

	modelID := testSlowSourceDB + "." + testSlowSourceTable

	// Time the full scan
	fullScanStart := time.Now()
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)
	fullScanDuration := time.Since(fullScanStart)

	// Verify initial bounds were set
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)
	assert.Equal(t, uint64(0), cache.Min)
	assert.Equal(t, uint64(499), cache.Max)
	assert.True(t, cache.InitialScanComplete)

	// Time the incremental scan (should be faster since it only scans a subset)
	incrementalStart := time.Now()
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)
	incrementalDuration := time.Since(incrementalStart)

	// Incremental scan should be faster than full scan
	// Note: we can't guarantee exact timing, but incremental should be notably faster
	// because it only queries a subset of data without the sleepEachRow overhead
	t.Logf("Full scan duration: %v, Incremental scan duration: %v", fullScanDuration, incrementalDuration)

	// Verify bounds are still correct after incremental scan
	cache, err = adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)
	assert.Equal(t, uint64(0), cache.Min, "Min should remain unchanged")
	assert.Equal(t, uint64(499), cache.Max, "Max should remain unchanged")
}

func TestIntegration_UpdateBounds_ConcurrentNeverCorrupts(t *testing.T) {
	// Use simple bounds SQL for this test
	externalSQL := testutil.DefaultExternalBoundsSQL(testSourceDB, testSourceTable)

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSourceDB, testSourceTable,
		externalSQL,
		map[string]string{},
		50*time.Millisecond,  // fast incremental interval
		200*time.Millisecond, // moderate full interval
	)
	ctx := context.Background()

	// Create source table with 1000 positions
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 1000, 1)

	modelID := testSourceDB + "." + testSourceTable

	// Initialize with full scan
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	// Get initial bounds
	initialCache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, initialCache)
	initialMin := initialCache.Min
	initialMax := initialCache.Max

	// Track errors and bounds violations
	var errorCount atomic.Int32
	var violations atomic.Int32
	var wg sync.WaitGroup

	// Duration for concurrent test
	testDuration := 3 * time.Second
	ctx, cancel := context.WithTimeout(ctx, testDuration)
	defer cancel()

	// Full scan goroutines
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := executor.UpdateBounds(ctx, modelID, ScanTypeFull); err != nil {
						if ctx.Err() == nil { // Don't count context cancellation as error
							errorCount.Add(1)
						}
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()
	}

	// Incremental scan goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := executor.UpdateBounds(ctx, modelID, ScanTypeIncremental); err != nil {
						if ctx.Err() == nil {
							errorCount.Add(1)
						}
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
		}()
	}

	// Bounds reader goroutine that continuously verifies bounds integrity
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				cache, err := adminSvc.GetExternalBounds(ctx, modelID)
				if err == nil && cache != nil {
					// Verify bounds integrity: min <= max
					if cache.Min > cache.Max {
						violations.Add(1)
						t.Errorf("Bounds corruption: min (%d) > max (%d)", cache.Min, cache.Max)
					}
					// Verify bounds never decrease (monotonically increasing)
					if cache.Min < initialMin {
						violations.Add(1)
						t.Errorf("Min decreased: %d < initial %d", cache.Min, initialMin)
					}
					if cache.Max < initialMax {
						violations.Add(1)
						t.Errorf("Max decreased: %d < initial %d", cache.Max, initialMax)
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify no corruption occurred
	assert.Equal(t, int32(0), violations.Load(), "No bounds violations should occur")

	// Final verification
	finalCache, err := adminSvc.GetExternalBounds(context.Background(), modelID)
	require.NoError(t, err)
	require.NotNil(t, finalCache)

	assert.True(t, finalCache.Min <= finalCache.Max, "Final min should be <= max")
	assert.True(t, finalCache.Min >= initialMin, "Final min should be >= initial min")
	assert.True(t, finalCache.Max >= initialMax, "Final max should be >= initial max")
}

func TestIntegration_UpdateBounds_ConditionalSQL(t *testing.T) {
	// Use conditional SQL that uses is_incremental_scan for different behavior
	externalSQL := testutil.ExternalBoundsWithCacheSQL(testSourceDB, testSourceTable)

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSourceDB, testSourceTable,
		externalSQL,
		map[string]string{},
		100*time.Millisecond,
		500*time.Millisecond,
	)
	ctx := context.Background()

	// Create source table
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 500, 1)

	modelID := testSourceDB + "." + testSourceTable

	// Full scan should query min() from data
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Full scan should have queried actual min from data
	assert.Equal(t, uint64(0), cache.Min, "Full scan should get min from data")
	assert.Equal(t, uint64(499), cache.Max, "Full scan should get max from data")
	assert.True(t, cache.InitialScanComplete)

	// Add more data
	insertSQL := `INSERT INTO ` + testSourceDB + `.` + testSourceTable + ` (position, value)
		SELECT number + 500, concat('test_value_', toString(number + 500))
		FROM numbers(200)`
	err = client.Execute(ctx, insertSQL)
	require.NoError(t, err)

	// Incremental scan should use previous_min (not query it again)
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	cache, err = adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Min should still be 0 (from previous_min), max should be updated
	assert.Equal(t, uint64(0), cache.Min, "Incremental scan should use previous_min")
	assert.Equal(t, uint64(699), cache.Max, "Incremental scan should update max")
}

func TestIntegration_UpdateBounds_EnvironmentVariables(t *testing.T) {
	// Use SQL that filters by network environment variable
	externalSQL := testutil.NetworkFilteredExternalBoundsSQL(testNetworkEventsDB, testNetworkEventsTable)

	env := map[string]string{
		"NETWORK": "mainnet",
	}

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testNetworkEventsDB, testNetworkEventsTable,
		externalSQL,
		env,
		100*time.Millisecond,
		500*time.Millisecond,
	)
	ctx := context.Background()

	// Create network events table with multiple networks
	networks := []string{"mainnet", "sepolia", "holesky"}
	testutil.CreateNetworkEventsTable(t, client, testNetworkEventsDB, testNetworkEventsTable, networks, 100)

	modelID := testNetworkEventsDB + "." + testNetworkEventsTable

	// Run full scan - should only see mainnet data
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Bounds should match only mainnet data (positions 0-99)
	assert.Equal(t, uint64(0), cache.Min, "Min should be 0 for mainnet")
	assert.Equal(t, uint64(99), cache.Max, "Max should be 99 for mainnet (100 positions)")

	// Verify mainnet has 100 rows
	mainnetRows := testutil.GetNetworkRowCount(t, client, testNetworkEventsDB, testNetworkEventsTable, "mainnet")
	assert.Equal(t, uint64(100), mainnetRows)
}

func TestIntegration_UpdateBounds_ZeroProtectionEnhanced(t *testing.T) {
	// Use simple bounds SQL for this test
	externalSQL := testutil.DefaultExternalBoundsSQL(testSourceDB, testSourceTable)

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSourceDB, testSourceTable,
		externalSQL,
		map[string]string{},
		100*time.Millisecond,
		500*time.Millisecond,
	)
	ctx := context.Background()

	// Create source table with data
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 500, 1)

	modelID := testSourceDB + "." + testSourceTable

	// Initial full scan to populate cache
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	initialCache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, initialCache)

	assert.Equal(t, uint64(0), initialCache.Min)
	assert.Equal(t, uint64(499), initialCache.Max)

	// Truncate table (simulates temporary data unavailability)
	testutil.TruncateTable(t, client, testSourceDB, testSourceTable)

	// Incremental scan with empty source should preserve existing bounds
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Bounds should be preserved (zero protection)
	assert.Equal(t, initialCache.Min, cache.Min, "Min should be preserved on zero result")
	assert.Equal(t, initialCache.Max, cache.Max, "Max should be preserved on zero result")

	// Full scan with no data should also protect bounds
	err = executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	cache, err = adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Bounds should still be preserved
	assert.Equal(t, initialCache.Min, cache.Min, "Full scan should also protect bounds")
	assert.Equal(t, initialCache.Max, cache.Max, "Full scan should also protect bounds")
}

func TestIntegration_UpdateBounds_InitialScanCompleteFlow(t *testing.T) {
	// Use simple bounds SQL for this test
	externalSQL := testutil.DefaultExternalBoundsSQL(testSourceDB, testSourceTable)

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSourceDB, testSourceTable,
		externalSQL,
		map[string]string{},
		100*time.Millisecond,
		500*time.Millisecond,
	)
	ctx := context.Background()

	// Create source table
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 500, 1)

	modelID := testSourceDB + "." + testSourceTable

	// Verify no cache exists initially
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	assert.Nil(t, cache, "No cache should exist initially")

	// Try incremental scan without initial full scan - should be skipped
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	// Verify no cache was created (incremental skipped without initial scan)
	cache, err = adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	assert.Nil(t, cache, "Incremental scan without initial scan should not create cache")

	// Run full scan - should set InitialScanComplete = true
	err = executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	cache, err = adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache, "Cache should exist after full scan")
	assert.True(t, cache.InitialScanComplete, "InitialScanComplete should be true")
	assert.Equal(t, uint64(0), cache.Min)
	assert.Equal(t, uint64(499), cache.Max)

	// Add more data
	insertSQL := `INSERT INTO ` + testSourceDB + `.` + testSourceTable + ` (position, value)
		SELECT number + 500, concat('test_value_', toString(number + 500))
		FROM numbers(100)`
	err = client.Execute(ctx, insertSQL)
	require.NoError(t, err)

	// Now incremental scan should succeed
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	cache, err = adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Min should be preserved from initial scan, max should be updated
	assert.Equal(t, uint64(0), cache.Min, "Min should be preserved")
	assert.Equal(t, uint64(599), cache.Max, "Max should be updated by incremental scan")
}

func TestIntegration_UpdateBounds_AsymmetricZeroProtection(t *testing.T) {
	// Use SQL that returns valid min but 0 for max during incremental scan
	externalSQL := testutil.AsymmetricZeroBoundsSQL()

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSourceDB, testSourceTable,
		externalSQL,
		map[string]string{},
		100*time.Millisecond,
		500*time.Millisecond,
	)
	ctx := context.Background()

	// Create source table with data
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 500, 1)

	modelID := testSourceDB + "." + testSourceTable

	// Initial full scan to populate cache with real bounds
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	initialCache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, initialCache)

	assert.Equal(t, uint64(0), initialCache.Min)
	assert.Equal(t, uint64(499), initialCache.Max)

	// Now run incremental scan - SQL will return previous_min but 0 for max
	// This simulates the bug where scan window has no data
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	// Bounds should be PRESERVED, not corrupted to (0, 0)
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	assert.Equal(t, uint64(0), cache.Min, "Min should be preserved")
	assert.Equal(t, uint64(499), cache.Max, "Max should be preserved, NOT corrupted to 0")
}

func TestIntegration_UpdateBounds_MinGreaterThanMaxProtection(t *testing.T) {
	// Use SQL that returns min > max during incremental scan
	externalSQL := testutil.MinGreaterThanMaxBoundsSQL()

	executor, client, adminSvc, _ := setupExternalBoundsExecutor(
		t,
		testSourceDB, testSourceTable,
		externalSQL,
		map[string]string{},
		100*time.Millisecond,
		500*time.Millisecond,
	)
	ctx := context.Background()

	// Create source table with data
	testutil.CreateSourceTable(t, client, testSourceDB, testSourceTable, 500, 1)

	modelID := testSourceDB + "." + testSourceTable

	// Initial full scan to populate cache with real bounds
	err := executor.UpdateBounds(ctx, modelID, ScanTypeFull)
	require.NoError(t, err)

	initialCache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, initialCache)

	assert.Equal(t, uint64(0), initialCache.Min)
	assert.Equal(t, uint64(499), initialCache.Max)

	// Now run incremental scan - SQL will return min=600, max=200 (invalid)
	err = executor.UpdateBounds(ctx, modelID, ScanTypeIncremental)
	require.NoError(t, err)

	// Bounds should be PRESERVED, not set to invalid (600, 200)
	cache, err := adminSvc.GetExternalBounds(ctx, modelID)
	require.NoError(t, err)
	require.NotNil(t, cache)

	assert.Equal(t, uint64(0), cache.Min, "Min should be preserved")
	assert.Equal(t, uint64(499), cache.Max, "Max should be preserved, NOT set to invalid value")
}
