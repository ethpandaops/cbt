//go:build integration

package tasks

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
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

// testExecutor implements the Executor interface for testing.
type testExecutor struct {
	executeFunc      func(ctx context.Context, taskCtx interface{}) error
	validateFunc     func(ctx context.Context, taskCtx interface{}) error
	updateBoundsFunc func(ctx context.Context, modelID, scanType string) error
	executeCalled    bool
	validateCalled   bool
	updateCalled     bool
	lastTaskCtx      interface{}
}

func (e *testExecutor) Execute(ctx context.Context, taskCtx interface{}) error {
	e.executeCalled = true
	e.lastTaskCtx = taskCtx
	if e.executeFunc != nil {
		return e.executeFunc(ctx, taskCtx)
	}
	return nil
}

func (e *testExecutor) Validate(ctx context.Context, taskCtx interface{}) error {
	e.validateCalled = true
	if e.validateFunc != nil {
		return e.validateFunc(ctx, taskCtx)
	}
	return nil
}

func (e *testExecutor) UpdateBounds(ctx context.Context, modelID, scanType string) error {
	e.updateCalled = true
	if e.updateBoundsFunc != nil {
		return e.updateBoundsFunc(ctx, modelID, scanType)
	}
	return nil
}

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

// setupIntegrationTaskHandler creates a TaskHandler with real ClickHouse and Redis.
func setupIntegrationTaskHandler(t *testing.T) (*TaskHandler, clickhouse.ClientInterface, admin.Service, *testutil.RedisConnection, *testExecutor) {
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
		logger.WithField("test", "tasks"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create test transformation
	transformSQL := testutil.DefaultTransformationSQL(testSourceDB, testSourceTable)
	transformModel, err := testutil.NewTestTransformationSQL(
		testTargetDB, testTargetTable, transformSQL,
		testutil.WithDependencies(testSourceDB+"."+testSourceTable),
		testutil.WithInterval(0, 100),
	)
	require.NoError(t, err)

	// Create test validator
	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
		validRangeMin:  0,
		validRangeMax:  1000,
	}

	// Create test executor
	executor := &testExecutor{}

	// Create task handler
	handler := NewTaskHandler(
		logger.WithField("test", "handler"),
		client,
		adminSvc,
		validator,
		executor,
		[]models.Transformation{transformModel},
	)

	return handler, client, adminSvc, redisConn, executor
}

func TestIntegration_HandleTransformation_Incremental(t *testing.T) {
	handler, _, adminSvc, _, executor := setupIntegrationTaskHandler(t)
	ctx := context.Background()

	// Create incremental payload
	payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    testTargetDB + "." + testTargetTable,
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)

	// Handle the transformation
	err = handler.HandleTransformation(ctx, task)
	require.NoError(t, err)

	// Verify executor was called
	assert.True(t, executor.executeCalled)

	// Verify task context was passed correctly
	taskCtx, ok := executor.lastTaskCtx.(*TaskContext)
	require.True(t, ok)
	assert.Equal(t, uint64(0), taskCtx.Position)
	assert.Equal(t, uint64(100), taskCtx.Interval)

	// Verify completion was recorded in admin table
	nextPos, err := adminSvc.GetNextUnprocessedPosition(ctx, payload.ModelID)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), nextPos)
}

func TestIntegration_HandleTransformation_Scheduled(t *testing.T) {
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
		_ = client.Stop()
	})

	// Create admin tables
	testutil.CreateAdminTables(t, client)
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
		logger.WithField("test", "tasks"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create scheduled transformation
	scheduledSQL := `---
type: scheduled
database: ` + testTargetDB + `
table: ` + testTargetTable + `
schedule: "*/1 * * * *"
dependencies:
  - ` + testSourceDB + `.` + testSourceTable + `
---
INSERT INTO {{ .database }}.{{ .table }} (position, value) VALUES (1, 'scheduled_test')`

	scheduledModel, err := createScheduledTransformation(scheduledSQL)
	require.NoError(t, err)

	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
	}
	executor := &testExecutor{}

	handler := NewTaskHandler(
		logger.WithField("test", "handler"),
		client,
		adminSvc,
		validator,
		executor,
		[]models.Transformation{scheduledModel},
	)

	ctx := context.Background()

	execTime := time.Now().UTC().Truncate(time.Millisecond)
	payload := ScheduledTaskPayload{
		Type:          TaskTypeScheduled,
		ModelID:       testTargetDB + "." + testTargetTable,
		ExecutionTime: execTime,
		EnqueuedAt:    time.Now(),
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)

	err = handler.HandleTransformation(ctx, task)
	require.NoError(t, err)

	assert.True(t, executor.executeCalled)

	// Verify scheduled completion was recorded
	lastExec, err := adminSvc.GetLastScheduledExecution(ctx, payload.ModelID)
	require.NoError(t, err)
	require.NotNil(t, lastExec)
	assert.Equal(t, execTime.UTC(), lastExec.UTC())
}

func TestIntegration_HandleTransformation_DependencyValidation(t *testing.T) {
	handler, _, _, _, executor := setupIntegrationTaskHandler(t)

	// Override validator to fail dependency check
	failingValidator := &testValidator{
		validateResult: validation.Result{CanProcess: false, Errors: []error{ErrDependenciesNotSatisfied}},
	}
	handler.validator = failingValidator

	ctx := context.Background()

	payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    testTargetDB + "." + testTargetTable,
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)

	err = handler.HandleTransformation(ctx, task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dependencies not satisfied")

	// Executor should NOT have been called
	assert.False(t, executor.executeCalled)
}

func TestIntegration_HandleTransformation_ModelNotFound(t *testing.T) {
	handler, _, _, _, _ := setupIntegrationTaskHandler(t)
	ctx := context.Background()

	payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    "nonexistent.table",
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)

	err = handler.HandleTransformation(ctx, task)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrModelConfigNotFound)
}

func TestIntegration_HandleExternalScan_Full(t *testing.T) {
	handler, _, _, _, executor := setupIntegrationTaskHandler(t)
	ctx := context.Background()

	executor.updateBoundsFunc = func(ctx context.Context, modelID, scanType string) error {
		assert.Equal(t, testSourceDB+"."+testSourceTable, modelID)
		assert.Equal(t, "full", scanType)
		return nil
	}

	payload := map[string]string{
		"model_id":  testSourceDB + "." + testSourceTable,
		"scan_type": "full",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask("external:full", payloadBytes)

	err = handler.HandleExternalScan(ctx, task)
	require.NoError(t, err)

	assert.True(t, executor.updateCalled)
}

func TestIntegration_HandleExternalScan_Incremental(t *testing.T) {
	handler, _, _, _, executor := setupIntegrationTaskHandler(t)
	ctx := context.Background()

	executor.updateBoundsFunc = func(ctx context.Context, modelID, scanType string) error {
		assert.Equal(t, testSourceDB+"."+testSourceTable, modelID)
		assert.Equal(t, "incremental", scanType)
		return nil
	}

	payload := map[string]string{
		"model_id":  testSourceDB + "." + testSourceTable,
		"scan_type": "incremental",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask("external:incremental", payloadBytes)

	err = handler.HandleExternalScan(ctx, task)
	require.NoError(t, err)

	assert.True(t, executor.updateCalled)
}

func TestIntegration_HandleExternalScan_MissingModelID(t *testing.T) {
	handler, _, _, _, _ := setupIntegrationTaskHandler(t)
	ctx := context.Background()

	payload := map[string]string{
		"scan_type": "full",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask("external:full", payloadBytes)

	err = handler.HandleExternalScan(ctx, task)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrModelIDNotFound)
}

func TestIntegration_HandleExternalScan_MissingScanType(t *testing.T) {
	handler, _, _, _, _ := setupIntegrationTaskHandler(t)
	ctx := context.Background()

	payload := map[string]string{
		"model_id": testSourceDB + "." + testSourceTable,
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask("external:full", payloadBytes)

	err = handler.HandleExternalScan(ctx, task)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrScanTypeNotFound)
}

func TestIntegration_Routes(t *testing.T) {
	handler, _, _, _, _ := setupIntegrationTaskHandler(t)

	routes := handler.Routes()

	assert.Contains(t, routes, TypeModelTransformation)
	assert.Contains(t, routes, "external:incremental")
	assert.Contains(t, routes, "external:full")
}

// createScheduledTransformation creates a scheduled transformation for testing.
func createScheduledTransformation(content string) (models.Transformation, error) {
	return models.NewTransformation([]byte(content), "test.sql")
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

// testChainExecutor implements the Executor interface for chain testing.
type testChainExecutor struct {
	executeFunc       func(ctx context.Context, taskCtx interface{}) error
	validateFunc      func(ctx context.Context, taskCtx interface{}) error
	updateBoundsFunc  func(ctx context.Context, modelID, scanType string) error
	executedModels    []string
	executedPositions []uint64
	mu                sync.Mutex
}

func (e *testChainExecutor) Execute(ctx context.Context, taskCtx interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tc, ok := taskCtx.(*TaskContext)
	if ok {
		e.executedModels = append(e.executedModels, tc.Transformation.GetID())
		e.executedPositions = append(e.executedPositions, tc.Position)
	}

	if e.executeFunc != nil {
		return e.executeFunc(ctx, taskCtx)
	}
	return nil
}

func (e *testChainExecutor) Validate(ctx context.Context, taskCtx interface{}) error {
	if e.validateFunc != nil {
		return e.validateFunc(ctx, taskCtx)
	}
	return nil
}

func (e *testChainExecutor) UpdateBounds(ctx context.Context, modelID, scanType string) error {
	if e.updateBoundsFunc != nil {
		return e.updateBoundsFunc(ctx, modelID, scanType)
	}
	return nil
}

func (e *testChainExecutor) GetExecutedModels() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]string, len(e.executedModels))
	copy(result, e.executedModels)
	return result
}

func (e *testChainExecutor) GetExecutedPositions() []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]uint64, len(e.executedPositions))
	copy(result, e.executedPositions)
	return result
}

// setupChainTaskHandler creates a TaskHandler configured for chain testing.
func setupChainTaskHandler(t *testing.T) (*TaskHandler, admin.Service, *testChainExecutor, []models.Transformation) {
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
		_ = client.Stop()
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
		logger.WithField("test", "tasks"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create transformation chain
	chainCfg := testutil.DefaultTestChainConfig()
	external, level1, level2, _, err := testutil.NewTestTransformationChain(chainCfg)
	require.NoError(t, err)

	transformations := []models.Transformation{level1, level2}

	// Build DAG with chain
	dag := testutil.TestDAG(transformations, []models.External{external})

	// Create validator that accepts all
	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
		validRangeMin:  0,
		validRangeMax:  500,
	}

	// Create chain executor
	chainExecutor := &testChainExecutor{}

	handler := NewTaskHandler(
		logger.WithField("test", "handler"),
		client,
		adminSvc,
		validator,
		chainExecutor,
		transformations,
	)

	// The DAG is used by the validator, not directly by the handler
	_ = dag

	return handler, adminSvc, chainExecutor, transformations
}

func TestIntegration_HandleTransformation_NestedChain(t *testing.T) {
	handler, adminSvc, chainExecutor, transformations := setupChainTaskHandler(t)
	ctx := context.Background()

	// Set up external bounds in cache
	cache := &admin.BoundsCache{
		ModelID:             testEventsSourceDB + "." + testEventsSource,
		Min:                 0,
		Max:                 500,
		InitialScanComplete: true,
		UpdatedAt:           time.Now().UTC(),
	}
	err := adminSvc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	// Execute level 1 transformation (events_aggregated)
	level1ID := transformations[0].GetID()
	level1Payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    level1ID,
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(level1Payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)
	err = handler.HandleTransformation(ctx, task)
	require.NoError(t, err)

	// Verify level 1 was executed
	executedModels := chainExecutor.GetExecutedModels()
	require.Len(t, executedModels, 1)
	assert.Equal(t, level1ID, executedModels[0])

	// Record completion for level 1
	err = adminSvc.RecordCompletion(ctx, level1ID, 0, 100)
	require.NoError(t, err)

	// Execute level 2 transformation (events_by_account)
	level2ID := transformations[1].GetID()
	level2Payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    level2ID,
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err = json.Marshal(level2Payload)
	require.NoError(t, err)

	task = asynq.NewTask(TypeModelTransformation, payloadBytes)
	err = handler.HandleTransformation(ctx, task)
	require.NoError(t, err)

	// Verify level 2 was executed
	executedModels = chainExecutor.GetExecutedModels()
	require.Len(t, executedModels, 2)
	assert.Equal(t, level2ID, executedModels[1])

	// Verify both at position 0
	positions := chainExecutor.GetExecutedPositions()
	assert.Equal(t, uint64(0), positions[0])
	assert.Equal(t, uint64(0), positions[1])
}

func TestIntegration_HandleTransformation_ChainDependencyValidation(t *testing.T) {
	handler, adminSvc, chainExecutor, transformations := setupChainTaskHandler(t)
	ctx := context.Background()

	// Set up external bounds
	cache := &admin.BoundsCache{
		ModelID:             testEventsSourceDB + "." + testEventsSource,
		Min:                 0,
		Max:                 500,
		InitialScanComplete: true,
		UpdatedAt:           time.Now().UTC(),
	}
	err := adminSvc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	// Override validator to fail when dependency not satisfied
	failingValidator := &testValidator{
		validateResult: validation.Result{
			CanProcess: false,
			Errors:     []error{ErrDependenciesNotSatisfied},
		},
	}
	handler.validator = failingValidator

	// Try to execute level 2 without level 1 completion
	level2ID := transformations[1].GetID()
	level2Payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    level2ID,
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(level2Payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)
	err = handler.HandleTransformation(ctx, task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dependencies not satisfied")

	// Executor should NOT have been called
	executedModels := chainExecutor.GetExecutedModels()
	assert.Empty(t, executedModels)
}

func TestIntegration_HandleTransformation_MultipleDependencies(t *testing.T) {
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
		_ = client.Stop()
	})

	testutil.CreateAdminTables(t, client)

	// Create two source tables for multi-dependency testing
	testutil.CreateEventsAggregatedTable(t, client, "source1", "events_a")
	testutil.CreateEventsAggregatedTable(t, client, "source2", "events_b")
	testutil.CreateEventsAggregatedTable(t, client, "target", "merged_events")

	// Create admin service
	tableConfig := admin.TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}
	adminSvc := admin.NewService(
		logger.WithField("test", "tasks"),
		client,
		"",
		"",
		tableConfig,
		redisConn.Client,
	)

	// Create multi-dependency transformation (UNION pattern)
	multiDepModel, err := testutil.NewTestMultiDependencyTransformation(
		"target", "merged_events",
		"source1", "events_a",
		"source2", "events_b",
	)
	require.NoError(t, err)

	// Create externals for both sources
	external1, err := testutil.NewTestExternalSQL("source1", "events_a",
		testutil.DefaultExternalBoundsSQL("source1", "events_a"))
	require.NoError(t, err)

	external2, err := testutil.NewTestExternalSQL("source2", "events_b",
		testutil.DefaultExternalBoundsSQL("source2", "events_b"))
	require.NoError(t, err)

	// Build DAG with multi-dependency
	dag := testutil.TestDAG(
		[]models.Transformation{multiDepModel},
		[]models.External{external1, external2},
	)

	// Validator that checks both dependencies
	validator := &testValidator{
		validateResult: validation.Result{CanProcess: true},
		validRangeMin:  0,
		validRangeMax:  100,
	}

	executor := &testChainExecutor{}

	handler := NewTaskHandler(
		logger.WithField("test", "handler"),
		client,
		adminSvc,
		validator,
		executor,
		[]models.Transformation{multiDepModel},
	)

	// The DAG is used by the validator, not directly by the handler
	_ = dag

	ctx := context.Background()

	// Execute the multi-dependency transformation
	payload := IncrementalTaskPayload{
		Type:       TaskTypeIncremental,
		ModelID:    multiDepModel.GetID(),
		Position:   0,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, payloadBytes)
	err = handler.HandleTransformation(ctx, task)
	require.NoError(t, err)

	// Verify executor was called
	executedModels := executor.GetExecutedModels()
	require.Len(t, executedModels, 1)
	assert.Equal(t, multiDepModel.GetID(), executedModels[0])
}

func TestIntegration_HandleTransformation_ChainProgressTracking(t *testing.T) {
	handler, adminSvc, chainExecutor, transformations := setupChainTaskHandler(t)
	ctx := context.Background()

	// Set up external bounds
	cache := &admin.BoundsCache{
		ModelID:             testEventsSourceDB + "." + testEventsSource,
		Min:                 0,
		Max:                 500,
		InitialScanComplete: true,
		UpdatedAt:           time.Now().UTC(),
	}
	err := adminSvc.SetExternalBounds(ctx, cache)
	require.NoError(t, err)

	level1ID := transformations[0].GetID()

	// Execute level 1 for multiple intervals
	for position := uint64(0); position < 300; position += 100 {
		payload := IncrementalTaskPayload{
			Type:       TaskTypeIncremental,
			ModelID:    level1ID,
			Position:   position,
			Interval:   100,
			Direction:  DirectionForward,
			EnqueuedAt: time.Now(),
		}

		payloadBytes, err := json.Marshal(payload)
		require.NoError(t, err)

		task := asynq.NewTask(TypeModelTransformation, payloadBytes)
		err = handler.HandleTransformation(ctx, task)
		require.NoError(t, err)

		// Record completion
		err = adminSvc.RecordCompletion(ctx, level1ID, position, 100)
		require.NoError(t, err)
	}

	// Verify all positions were executed
	positions := chainExecutor.GetExecutedPositions()
	assert.Len(t, positions, 3)
	assert.Equal(t, uint64(0), positions[0])
	assert.Equal(t, uint64(100), positions[1])
	assert.Equal(t, uint64(200), positions[2])

	// Verify next unprocessed position
	nextPos, err := adminSvc.GetNextUnprocessedPosition(ctx, level1ID)
	require.NoError(t, err)
	assert.Equal(t, uint64(300), nextPos)
}
