package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors used across the handler unit tests.
var (
	errExecute      = errors.New("execute failed")
	errValidateDeps = errors.New("dependency validation failed")
	errRecord       = errors.New("record completion failed")
)

// fakeExecutor is a configurable Executor for unit testing the handler.
type fakeExecutor struct {
	executeErr      error
	updateBoundsErr error
	executeCalled   bool
	updateCalled    bool
	lastTaskCtx     *ExecutionContext
	lastUpdateModel string
	lastUpdateScan  string
	validateErr     error
	validateCalled  bool
}

func (e *fakeExecutor) Execute(_ context.Context, taskCtx *ExecutionContext) error {
	e.executeCalled = true
	e.lastTaskCtx = taskCtx
	return e.executeErr
}

func (e *fakeExecutor) Validate(_ context.Context, _ *ExecutionContext) error {
	e.validateCalled = true
	return e.validateErr
}

func (e *fakeExecutor) UpdateBounds(_ context.Context, modelID, scanType string) error {
	e.updateCalled = true
	e.lastUpdateModel = modelID
	e.lastUpdateScan = scanType
	return e.updateBoundsErr
}

// fakeHandlerValidator implements validation.Validator with configurable results.
type fakeHandlerValidator struct {
	result validation.Result
	err    error
	called bool
}

func (v *fakeHandlerValidator) ValidateDependencies(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
	v.called = true
	return v.result, v.err
}

func (v *fakeHandlerValidator) GetValidRange(_ context.Context, _ string, _ validation.RangeSemantics) (minPos, maxPos uint64, err error) {
	return 0, 0, nil
}

func (v *fakeHandlerValidator) GetStartPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

// recordingHandler is a transformation.Handler whose RecordCompletion result is configurable.
type recordingHandler struct {
	testutil.FakeHandler
	recordErr    error
	recordCalled bool
}

func (h *recordingHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	h.recordCalled = true
	return h.recordErr
}

func newTestLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

// incrementalTransformation builds a FakeTransformation configured as an incremental
// model "db.model" with the given handler.
func incrementalTransformation(handler transformation.Handler) *testutil.FakeTransformation {
	return &testutil.FakeTransformation{
		ID:      "db.model",
		Type:    transformation.TypeSQL,
		Config:  transformation.Config{Type: transformation.TypeIncremental, Database: "db", Table: "model"},
		Handler: handler,
	}
}

func TestGetWorkerID(t *testing.T) {
	id := getWorkerID()
	assert.NotEmpty(t, id)
}

func TestGetWorkerID_HostnameError(t *testing.T) {
	original := hostnameFn
	t.Cleanup(func() { hostnameFn = original })

	hostnameFn = func() (string, error) { return "", errExecute }

	assert.Equal(t, "worker-unknown", getWorkerID())
}

func TestNewHandler(t *testing.T) {
	log := newTestLogger()
	ch := &testutil.FakeClickHouseClient{}
	adminSvc := &adminfake.FakeAdminService{}
	validator := &fakeHandlerValidator{}
	executor := &fakeExecutor{}

	trans := incrementalTransformation(nil)

	handler := NewHandler(log, ch, adminSvc, validator, executor, []models.Transformation{trans})

	require.NotNil(t, handler)
	assert.Len(t, handler.transformations, 1)
	assert.Contains(t, handler.transformations, "db.model")
}

func TestHandler_HandleTransformation_Incremental(t *testing.T) {
	log := newTestLogger()
	ch := &testutil.FakeClickHouseClient{}
	adminSvc := &adminfake.FakeAdminService{
		FirstPositions: map[string]uint64{"db.model": 10},
		LastPositions:  map[string]uint64{"db.model": 200},
	}
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{}

	handler := recordingHandler{}
	trans := incrementalTransformation(&handler)

	h := NewHandler(log, ch, adminSvc, validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:       TypeIncremental,
		ModelID:    "db.model",
		Position:   100,
		Interval:   50,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.NoError(t, err)

	assert.True(t, validator.called)
	assert.True(t, executor.executeCalled)
	assert.True(t, handler.recordCalled)
	require.NotNil(t, executor.lastTaskCtx)
	assert.Equal(t, uint64(100), executor.lastTaskCtx.Position)
	assert.Equal(t, uint64(50), executor.lastTaskCtx.Interval)
}

func TestHandler_HandleTransformation_Scheduled(t *testing.T) {
	log := newTestLogger()
	ch := &testutil.FakeClickHouseClient{}
	adminSvc := &adminfake.FakeAdminService{}
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{}

	handler := recordingHandler{}
	trans := &testutil.FakeTransformation{
		ID:      "db.scheduled",
		Type:    transformation.TypeSQL,
		Config:  transformation.Config{Type: transformation.TypeScheduled, Database: "db", Table: "scheduled"},
		Handler: &handler,
	}

	h := NewHandler(log, ch, adminSvc, validator, executor, []models.Transformation{trans})

	execTime := time.Now().UTC()
	payload := ScheduledPayload{
		Type:          TypeScheduled,
		ModelID:       "db.scheduled",
		ExecutionTime: execTime,
		EnqueuedAt:    time.Now(),
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.NoError(t, err)

	// Scheduled transformations skip dependency validation.
	assert.False(t, validator.called)
	assert.True(t, executor.executeCalled)
	assert.True(t, handler.recordCalled)
	require.NotNil(t, executor.lastTaskCtx)
	assert.Equal(t, execTime, executor.lastTaskCtx.ExecutionTime)
}

func TestHandler_HandleTransformation_ParseError(t *testing.T) {
	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		&fakeHandlerValidator{}, &fakeExecutor{}, nil)

	task := asynq.NewTask(TypeModelTransformation, []byte("not json"))
	err := h.HandleTransformation(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

func TestHandler_HandleTransformation_ModelNotFound(t *testing.T) {
	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		&fakeHandlerValidator{result: validation.Result{CanProcess: true}}, &fakeExecutor{}, nil)

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "missing.model",
		Position:  10,
		Interval:  5,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrModelConfigNotFound)
}

func TestHandler_HandleTransformation_DependencyValidationError(t *testing.T) {
	validator := &fakeHandlerValidator{err: errValidateDeps}
	executor := &fakeExecutor{}
	trans := incrementalTransformation(nil)

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "db.model",
		Position:  10,
		Interval:  5,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.ErrorIs(t, err, errValidateDeps)
	assert.False(t, executor.executeCalled)
}

func TestHandler_HandleTransformation_DependenciesNotSatisfied(t *testing.T) {
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: false}}
	executor := &fakeExecutor{}
	trans := incrementalTransformation(nil)

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "db.model",
		Position:  10,
		Interval:  5,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.ErrorIs(t, err, ErrDependenciesNotSatisfied)
	assert.False(t, executor.executeCalled)
}

func TestHandler_HandleTransformation_ExecutionError(t *testing.T) {
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{executeErr: errExecute}
	trans := incrementalTransformation(nil)

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "db.model",
		Position:  10,
		Interval:  5,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.ErrorIs(t, err, errExecute)
	assert.Contains(t, err.Error(), "execution error")
}

// TestHandler_RecordTaskSuccess_NilHandler verifies the path where the transformation
// has no handler (RecordCompletion is skipped) and bounds are recorded for incremental.
func TestHandler_RecordTaskSuccess_NilHandler(t *testing.T) {
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{}
	adminSvc := &adminfake.FakeAdminService{
		FirstPositions: map[string]uint64{"db.model": 5},
		LastPositions:  map[string]uint64{"db.model": 300},
	}
	trans := incrementalTransformation(nil)

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, adminSvc,
		validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "db.model",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.NoError(t, err)
	assert.True(t, executor.executeCalled)
}

// TestHandler_RecordTaskSuccess_RecordCompletionError verifies that a failing
// RecordCompletion is logged but does not fail the task.
func TestHandler_RecordTaskSuccess_RecordCompletionError(t *testing.T) {
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{}
	adminSvc := &adminfake.FakeAdminService{}

	handler := recordingHandler{recordErr: errRecord}
	trans := incrementalTransformation(&handler)

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, adminSvc,
		validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "db.model",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.NoError(t, err)
	assert.True(t, handler.recordCalled)
}

// TestHandler_RecordTaskSuccess_ScheduledNoBounds verifies that scheduled
// transformations do not record model bounds (the IsScheduledType branch).
func TestHandler_RecordTaskSuccess_ScheduledNoBounds(t *testing.T) {
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{}

	var nextUnprocessedCalled bool
	adminSvc := &adminfake.FakeAdminService{
		GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
			nextUnprocessedCalled = true
			return 0, nil
		},
	}

	handler := recordingHandler{}
	trans := &testutil.FakeTransformation{
		ID:      "db.scheduled",
		Type:    transformation.TypeSQL,
		Config:  transformation.Config{Type: transformation.TypeScheduled, Database: "db", Table: "scheduled"},
		Handler: &handler,
	}

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, adminSvc,
		validator, executor, []models.Transformation{trans})

	payload := ScheduledPayload{
		Type:          TypeScheduled,
		ModelID:       "db.scheduled",
		ExecutionTime: time.Now().UTC(),
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.NoError(t, err)

	// Bounds lookup must be skipped for scheduled transformations.
	assert.False(t, nextUnprocessedCalled)
}

// TestHandler_SetupTaskContext_DefaultBranch exercises the default switch arms in
// setupTaskContext using a payload type that is neither incremental nor scheduled.
func TestHandler_SetupTaskContext_DefaultBranch(t *testing.T) {
	trans := incrementalTransformation(nil)
	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		&fakeHandlerValidator{}, &fakeExecutor{}, []models.Transformation{trans})

	ctx, err := h.setupTaskContext(unmarshalablePayload{modelID: "db.model"})
	require.NoError(t, err)
	require.NotNil(t, ctx)
	assert.Equal(t, uint64(0), ctx.Position)
	assert.Equal(t, uint64(0), ctx.Interval)
	assert.False(t, ctx.ExecutionTime.IsZero())
}

func TestHandler_HandleExternalScan(t *testing.T) {
	tests := []struct {
		name      string
		payload   any
		rawData   []byte
		updateErr error
		wantErr   error
		wantSub   string
		scanType  string
	}{
		{
			name:     "incremental success",
			payload:  map[string]string{"model_id": "src.table", "scan_type": ScanTypeIncremental},
			scanType: ScanTypeIncremental,
		},
		{
			name:     "full success",
			payload:  map[string]string{"model_id": "src.table", "scan_type": ScanTypeFull},
			scanType: ScanTypeFull,
		},
		{
			name:    "unmarshal error",
			rawData: []byte("not json"),
			wantSub: "failed to unmarshal external scan payload",
		},
		{
			name:    "missing model_id",
			payload: map[string]string{"scan_type": ScanTypeFull},
			wantErr: ErrModelIDNotFound,
		},
		{
			name:    "missing scan_type",
			payload: map[string]string{"model_id": "src.table"},
			wantErr: ErrScanTypeNotFound,
		},
		{
			name:      "executor error",
			payload:   map[string]string{"model_id": "src.table", "scan_type": ScanTypeFull},
			updateErr: errExecute,
			wantSub:   "execution error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &fakeExecutor{updateBoundsErr: tt.updateErr}
			h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
				&fakeHandlerValidator{}, executor, nil)

			data := tt.rawData
			if data == nil {
				var err error
				data, err = json.Marshal(tt.payload)
				require.NoError(t, err)
			}

			task := asynq.NewTask("external:full", data)
			err := h.HandleExternalScan(context.Background(), task)

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.wantSub != "":
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantSub)
			default:
				require.NoError(t, err)
				assert.True(t, executor.updateCalled)
				assert.Equal(t, "src.table", executor.lastUpdateModel)
				assert.Equal(t, tt.scanType, executor.lastUpdateScan)
			}
		})
	}
}

func TestHandler_Routes(t *testing.T) {
	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, &adminfake.FakeAdminService{},
		&fakeHandlerValidator{}, &fakeExecutor{}, nil)

	routes := h.Routes()
	require.Len(t, routes, 3)
	assert.Contains(t, routes, TypeModelTransformation)
	assert.Contains(t, routes, "external:incremental")
	assert.Contains(t, routes, "external:full")
}

// TestHandler_RecordTaskSuccess_NoBoundsRecordedWhenZero exercises the branch where
// min/max positions are zero so RecordModelBounds is skipped.
func TestHandler_RecordTaskSuccess_NoBoundsRecordedWhenZero(t *testing.T) {
	validator := &fakeHandlerValidator{result: validation.Result{CanProcess: true}}
	executor := &fakeExecutor{}
	adminSvc := &adminfake.FakeAdminService{} // First/Next positions default to 0
	trans := incrementalTransformation(nil)

	h := NewHandler(newTestLogger(), &testutil.FakeClickHouseClient{}, adminSvc,
		validator, executor, []models.Transformation{trans})

	payload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "db.model",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	err = h.HandleTransformation(context.Background(), task)
	require.NoError(t, err)
}

// ensure admin.BoundsCache import is retained for potential future use without unused error.
var _ = admin.BoundsCache{}
