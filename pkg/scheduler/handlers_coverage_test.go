package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/liveconfig"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errTriggerBoom is a static error used to exercise trigger failure paths.
var errTriggerBoom = errors.New("boom")

// newTestService builds a service wired with the supplied fakes but without any
// Redis-backed dependencies, suitable for exercising handler logic directly.
func newTestService(dag models.DAGReader, coord coordinator.Service) *service {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	return &service{
		log:         log.WithField("service", "scheduler"),
		cfg:         &Config{Concurrency: 1},
		done:        make(chan struct{}),
		dag:         dag,
		coordinator: coord,
		mux:         asynq.NewServeMux(),
		sleep:       func(time.Duration) {}, // no-op jitter in tests
	}
}

// newDisablingApplier returns a liveconfig.Applier that reports modelID as
// disabled. It drives a real Applier through CheckAndApply with a single
// override flagged Enabled=false so the disabled set is populated.
func newDisablingApplier(t *testing.T, dag models.DAGReader, modelID, modelType string) *liveconfig.Applier {
	t.Helper()

	disabled := false
	adminSvc := &adminfake.FakeAdminService{
		ConfigOverrides: []admin.ConfigOverride{
			{
				ModelID:  modelID,
				Type:     modelType,
				Enabled:  &disabled,
				Override: json.RawMessage(`{}`),
			},
		},
		GetConfigOverrideVersionFn: func(_ context.Context) (int64, error) {
			return 1, nil
		},
	}

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	applier := liveconfig.NewApplier(dag, adminSvc, log)
	changed, err := applier.CheckAndApply(context.Background())
	require.NoError(t, err)
	require.True(t, changed)

	return applier
}

func TestHandleExternalIncremental(t *testing.T) {
	tests := []struct {
		name       string
		taskType   string
		wantErr    bool
		wantModel  string
		wantCalled bool
	}{
		{
			name:       "valid external incremental task",
			taskType:   "external:db.table:incremental",
			wantErr:    false,
			wantModel:  "db.table",
			wantCalled: true,
		},
		{
			name:       "invalid task type format",
			taskType:   "external:db.table",
			wantErr:    true,
			wantCalled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				gotModelID string
				gotScan    string
				called     bool
			)
			coord := &coordinatorfake.FakeCoordinator{
				ProcessExternalScanFn: func(modelID, scanType string) {
					called = true
					gotModelID = modelID
					gotScan = scanType
				},
			}

			svc := newTestService(&testutil.FakeDAGReader{}, coord)

			err := svc.HandleExternalIncremental(context.Background(), asynq.NewTask(tt.taskType, nil))

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidExternalTaskType)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantModel, gotModelID)
				assert.Equal(t, "incremental", gotScan)
			}
			assert.Equal(t, tt.wantCalled, called)
		})
	}
}

func TestHandleExternalFull(t *testing.T) {
	tests := []struct {
		name       string
		taskType   string
		wantErr    bool
		wantModel  string
		wantCalled bool
	}{
		{
			name:       "valid external full task",
			taskType:   "external:db.table:full",
			wantErr:    false,
			wantModel:  "db.table",
			wantCalled: true,
		},
		{
			name:       "invalid task type format",
			taskType:   "external",
			wantErr:    true,
			wantCalled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				gotModelID string
				gotScan    string
				called     bool
			)
			coord := &coordinatorfake.FakeCoordinator{
				ProcessExternalScanFn: func(modelID, scanType string) {
					called = true
					gotModelID = modelID
					gotScan = scanType
				},
			}

			svc := newTestService(&testutil.FakeDAGReader{}, coord)

			err := svc.HandleExternalFull(context.Background(), asynq.NewTask(tt.taskType, nil))

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidExternalTaskType)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantModel, gotModelID)
				assert.Equal(t, "full", gotScan)
			}
			assert.Equal(t, tt.wantCalled, called)
		})
	}
}

func TestHandleScheduledTransformation(t *testing.T) {
	scheduledConfig := transformation.Config{Type: TransformationTypeScheduled}

	tests := []struct {
		name          string
		taskType      string
		dag           *testutil.FakeDAGReader
		triggerFn     func(ctx context.Context, modelID string) error
		disable       bool
		wantErr       bool
		wantTriggered bool
	}{
		{
			name:     "successful scheduled run",
			taskType: "transformation:test.model:scheduled",
			dag: &testutil.FakeDAGReader{
				Transformations: []models.Transformation{
					&testutil.FakeTransformation{ID: "test.model", Config: scheduledConfig},
				},
			},
			wantErr:       false,
			wantTriggered: true,
		},
		{
			name:     "node not found returns error",
			taskType: "transformation:unknown.model:scheduled",
			dag:      &testutil.FakeDAGReader{NodeNotFound: true},
			wantErr:  true,
		},
		{
			name:     "not a scheduled type returns error",
			taskType: "transformation:test.model:scheduled",
			dag: &testutil.FakeDAGReader{
				Transformations: []models.Transformation{
					&testutil.FakeTransformation{
						ID:     "test.model",
						Config: transformation.Config{Type: transformation.TypeIncremental},
					},
				},
			},
			wantErr: true,
		},
		{
			name:     "scheduled run already in progress is not an error",
			taskType: "transformation:test.model:scheduled",
			dag: &testutil.FakeDAGReader{
				Transformations: []models.Transformation{
					&testutil.FakeTransformation{ID: "test.model", Config: scheduledConfig},
				},
			},
			triggerFn: func(_ context.Context, _ string) error {
				return coordinator.ErrScheduledRunInProgress
			},
			wantErr:       false,
			wantTriggered: true,
		},
		{
			name:     "trigger error is propagated",
			taskType: "transformation:test.model:scheduled",
			dag: &testutil.FakeDAGReader{
				Transformations: []models.Transformation{
					&testutil.FakeTransformation{ID: "test.model", Config: scheduledConfig},
				},
			},
			triggerFn: func(_ context.Context, _ string) error {
				return errTriggerBoom
			},
			wantErr:       true,
			wantTriggered: true,
		},
		{
			name:     "disabled model is skipped",
			taskType: "transformation:test.model:scheduled",
			dag: &testutil.FakeDAGReader{
				Transformations: []models.Transformation{
					&testutil.FakeTransformation{ID: "test.model", Config: scheduledConfig},
				},
			},
			disable:       true,
			wantErr:       false,
			wantTriggered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			triggered := false
			coord := &coordinatorfake.FakeCoordinator{
				TriggerScheduledRunFn: func(ctx context.Context, modelID string) error {
					triggered = true
					if tt.triggerFn != nil {
						return tt.triggerFn(ctx, modelID)
					}
					return nil
				},
			}

			svc := newTestService(tt.dag, coord)
			if tt.disable {
				svc.liveOverrides = newDisablingApplier(t, tt.dag, "test.model", string(models.TypeTransformation))
			}

			err := svc.HandleScheduledTransformation(context.Background(), asynq.NewTask(tt.taskType, nil))

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantTriggered, triggered)
		})
	}
}

// TestHandlersSkipDisabledModels exercises the live-disabled fast path of the
// forward and backfill handlers (the isModelDisabled true branch).
func TestHandlersSkipDisabledModels(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			&testutil.FakeTransformation{ID: "test.model", Config: transformation.Config{Type: transformation.TypeIncremental}},
		},
	}

	tests := []struct {
		name   string
		handle func(s *service) error
	}{
		{
			name: "forward skipped",
			handle: func(s *service) error {
				return s.HandleScheduledForward(context.Background(), asynq.NewTask("transformation:test.model:forward", nil))
			},
		},
		{
			name: "backfill skipped",
			handle: func(s *service) error {
				return s.HandleScheduledBackfill(context.Background(), asynq.NewTask("transformation:test.model:back", nil))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coord := &coordinatorfake.FakeCoordinator{}
			svc := newTestService(dag, coord)
			svc.liveOverrides = newDisablingApplier(t, dag, "test.model", string(models.TypeTransformation))

			err := tt.handle(svc)
			require.NoError(t, err)
			assert.Equal(t, 0, coord.ProcessCallCount(), "Process must not be called for disabled model")
		})
	}
}

// TestRegisterExternalHandlers covers registerExternalHandlers for all cache
// configurations.
func TestRegisterExternalHandlers(t *testing.T) {
	tests := []struct {
		name                  string
		cache                 *external.CacheConfig
		incrementalRegistered bool
		fullRegistered        bool
	}{
		{
			name:                  "no cache config registers nothing",
			cache:                 nil,
			incrementalRegistered: false,
			fullRegistered:        false,
		},
		{
			name: "both intervals register both handlers",
			cache: &external.CacheConfig{
				IncrementalScanInterval: time.Minute,
				FullScanInterval:        5 * time.Minute,
			},
			incrementalRegistered: true,
			fullRegistered:        true,
		},
		{
			name: "only incremental interval registers incremental handler",
			cache: &external.CacheConfig{
				IncrementalScanInterval: time.Minute,
			},
			incrementalRegistered: true,
			fullRegistered:        false,
		},
		{
			name: "only full interval registers full handler",
			cache: &external.CacheConfig{
				FullScanInterval: 5 * time.Minute,
			},
			incrementalRegistered: false,
			fullRegistered:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})

			model := &testutil.FakeExternal{
				ID:     "db.table",
				Config: external.Config{Cache: tt.cache},
			}

			svc.registerExternalHandlers(model)

			// A registered handler resolves through the mux; an unregistered one
			// returns a "handler not found" error from ProcessTask.
			incTask := asynq.NewTask("external:db.table:incremental", nil)
			incErr := svc.mux.ProcessTask(context.Background(), incTask)
			if tt.incrementalRegistered {
				require.NoError(t, incErr)
			} else {
				require.Error(t, incErr)
			}

			fullTask := asynq.NewTask("external:db.table:full", nil)
			fullErr := svc.mux.ProcessTask(context.Background(), fullTask)
			if tt.fullRegistered {
				require.NoError(t, fullErr)
			} else {
				require.Error(t, fullErr)
			}
		})
	}
}

// TestRegisterAllHandlersWithExternalAndScheduled exercises registerAllHandlers
// across external models and scheduled transformations (the loop branches not
// covered by the existing incremental-only test).
func TestRegisterAllHandlersWithExternalAndScheduled(t *testing.T) {
	scheduledTrans := &testutil.FakeTransformation{
		ID:     "sched.model",
		Config: transformation.Config{Type: TransformationTypeScheduled},
		Handler: &testutil.FakeHandler{
			HandlerType: transformation.TypeScheduled,
			Schedule:    "@every 1m",
		},
	}

	incrementalTrans := &testutil.FakeTransformation{
		ID:     "inc.model",
		Config: transformation.Config{Type: transformation.TypeIncremental},
		Handler: &testutil.FakeHandler{
			HandlerConfig: &incremental.Config{
				Type:      transformation.TypeIncremental,
				Schedules: &incremental.SchedulesConfig{ForwardFill: "@every 30s", Backfill: "@every 1m"},
			},
		},
	}

	extModel := &testutil.FakeExternal{
		ID: "ext.model",
		Config: external.Config{
			Cache: &external.CacheConfig{
				IncrementalScanInterval: time.Minute,
				FullScanInterval:        5 * time.Minute,
			},
		},
	}

	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{scheduledTrans, incrementalTrans},
		Externals: []models.Node{
			{NodeType: models.NodeTypeExternal, Model: extModel},
			// A non-external node in the externals slice must be skipped.
			{NodeType: models.NodeTypeExternal, Model: "not-an-external"},
		},
	}

	svc := newTestService(dag, &coordinatorfake.FakeCoordinator{})
	svc.cfg.Consolidation = "@every 10m"

	svc.registerAllHandlers()

	// Spot-check that handlers for the side-effect-free categories resolve
	// (no "handler not found" error). These dispatch to no-op coordinator calls.
	for _, taskType := range []string{
		"external:ext.model:incremental",
		"external:ext.model:full",
		ConsolidationTaskType,
	} {
		err := svc.mux.ProcessTask(context.Background(), asynq.NewTask(taskType, nil))
		require.NoError(t, err, "handler for %s should be registered", taskType)
	}
}

// TestRegisterTransformationHandlersNilHandler covers the early returns in
// registerTransformationHandlers for incremental transformations with a nil
// handler and a non-incremental handler config.
func TestRegisterTransformationHandlersNilHandler(t *testing.T) {
	tests := []struct {
		name  string
		trans *testutil.FakeTransformation
	}{
		{
			name: "nil handler returns early",
			trans: &testutil.FakeTransformation{
				ID:      "test.model",
				Config:  transformation.Config{Type: transformation.TypeIncremental},
				Handler: nil,
			},
		},
		{
			name: "non-incremental handler config returns early",
			trans: &testutil.FakeTransformation{
				ID:     "test.model",
				Config: transformation.Config{Type: transformation.TypeIncremental},
				Handler: &testutil.FakeHandler{
					HandlerConfig: "not-an-incremental-config",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})

			svc.registerTransformationHandlers(tt.trans)

			// No forward handler should be registered.
			err := svc.mux.ProcessTask(context.Background(), asynq.NewTask("transformation:test.model:forward", nil))
			require.Error(t, err, "no handler should be registered")
		})
	}
}
