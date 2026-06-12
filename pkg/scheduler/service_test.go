package scheduler

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test NewService
func TestNewService(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Concurrency: 10,
			},
			wantErr: false,
		},
		{
			name: "valid config with consolidation schedule",
			cfg: &Config{
				Concurrency:   10,
				Consolidation: "@every 5m",
			},
			wantErr: false,
		},
		{
			name: "invalid config - zero concurrency",
			cfg: &Config{
				Concurrency: 0,
			},
			wantErr: true,
		},
		{
			name: "invalid config - negative concurrency",
			cfg: &Config{
				Concurrency: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			// Use a valid redis.Options for testing
			redisOpt := &redis.Options{
				Addr: "localhost:6379",
				DB:   0,
			}
			mockDAG := &testutil.FakeDAGReader{}
			mockCoord := &coordinatorfake.FakeCoordinator{}
			mockAdmin := &adminfake.FakeAdminService{}

			svc, err := NewService(log, tt.cfg, redisOpt, mockDAG, mockCoord, mockAdmin)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, svc)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, svc)

				// Verify it implements the Service interface
				var _ = svc
			}
		})
	}
}

// Test Config Validate
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Concurrency: 10,
			},
			wantErr: false,
		},
		{
			name: "valid config with custom consolidation",
			cfg: &Config{
				Concurrency:   10,
				Consolidation: "@hourly",
			},
			wantErr: false,
		},
		{
			name: "valid config with cron consolidation",
			cfg: &Config{
				Concurrency:   10,
				Consolidation: "0 */2 * * *", // Every 2 hours
			},
			wantErr: false,
		},
		{
			name: "zero concurrency",
			cfg: &Config{
				Concurrency: 0,
			},
			wantErr: true,
		},
		{
			name: "negative concurrency",
			cfg: &Config{
				Concurrency: -1,
			},
			wantErr: true,
		},
		{
			name: "high concurrency",
			cfg: &Config{
				Concurrency: 100,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidConcurrency)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test extractModelID function
func TestExtractModelID(t *testing.T) {
	tests := []struct {
		name     string
		taskType string
		expected string
	}{
		{
			name:     "forward task",
			taskType: "transformation:analytics.block_propagation:forward",
			expected: "analytics.block_propagation",
		},
		{
			name:     "back task",
			taskType: "transformation:test.model:back",
			expected: "test.model",
		},
		{
			name:     "no prefix",
			taskType: "test.model:forward",
			expected: "",
		},
		{
			name:     "empty string",
			taskType: "",
			expected: "",
		},
		{
			name:     "just prefix",
			taskType: "transformation:",
			expected: "",
		},
		{
			name:     "consolidation task",
			taskType: "consolidation",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractModelID(tt.taskType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test HandleScheduledForward
func TestHandleScheduledForward(t *testing.T) {
	tests := []struct {
		name          string
		taskType      string
		setupMocks    func(*testutil.FakeDAGReader, *coordinatorfake.FakeCoordinator)
		wantErr       bool
		expectedCalls int
	}{
		{
			name:     "successful forward processing",
			taskType: "transformation:test.model:forward",
			setupMocks: func(dag *testutil.FakeDAGReader, _ *coordinatorfake.FakeCoordinator) {
				dag.Transformations = []models.Transformation{
					&testutil.FakeTransformation{
						ID:     "test.model",
						Config: transformation.Config{},
					},
				}
			},
			wantErr:       false,
			expectedCalls: 1,
		},
		{
			name:     "node not found",
			taskType: "transformation:unknown.model:forward",
			setupMocks: func(dag *testutil.FakeDAGReader, _ *coordinatorfake.FakeCoordinator) {
				dag.NodeNotFound = true
			},
			wantErr:       true,
			expectedCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockDAG := &testutil.FakeDAGReader{}
			mockCoord := &coordinatorfake.FakeCoordinator{}

			tt.setupMocks(mockDAG, mockCoord)

			// Create service manually to avoid Redis dependency
			svc := &service{
				log:         log.WithField("service", "scheduler"),
				cfg:         &Config{Concurrency: 1},
				done:        make(chan struct{}),
				dag:         mockDAG,
				coordinator: mockCoord,
			}

			// Create asynq task
			task := asynq.NewTask(tt.taskType, nil)

			err := svc.HandleScheduledForward(context.Background(), task)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedCalls, mockCoord.ProcessCalls)
		})
	}
}

// Test HandleScheduledBackfill
func TestHandleScheduledBackfill(t *testing.T) {
	tests := []struct {
		name          string
		taskType      string
		setupMocks    func(*testutil.FakeDAGReader, *coordinatorfake.FakeCoordinator)
		wantErr       bool
		expectedCalls int
	}{
		{
			name:     "successful backfill processing",
			taskType: "transformation:test.model:back",
			setupMocks: func(dag *testutil.FakeDAGReader, _ *coordinatorfake.FakeCoordinator) {
				dag.Transformations = []models.Transformation{
					&testutil.FakeTransformation{
						ID: "test.model",
						Config: transformation.Config{
							Type:     transformation.TypeIncremental,
							Database: "test_db",
							Table:    "model",
						},
						Handler: &testutil.FakeHandler{
							BackfillEnabled:  true,
							BackfillSchedule: "*/5 * * * *",
						},
					},
				}
			},
			wantErr:       false,
			expectedCalls: 1,
		},
		{
			name:     "node not found",
			taskType: "transformation:unknown.model:back",
			setupMocks: func(dag *testutil.FakeDAGReader, _ *coordinatorfake.FakeCoordinator) {
				dag.NodeNotFound = true
			},
			wantErr:       true,
			expectedCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockDAG := &testutil.FakeDAGReader{}
			mockCoord := &coordinatorfake.FakeCoordinator{}

			tt.setupMocks(mockDAG, mockCoord)

			// Create service manually to avoid Redis dependency
			svc := &service{
				log:         log.WithField("service", "scheduler"),
				cfg:         &Config{Concurrency: 1},
				done:        make(chan struct{}),
				dag:         mockDAG,
				coordinator: mockCoord,
			}

			// Create asynq task
			task := asynq.NewTask(tt.taskType, nil)

			err := svc.HandleScheduledBackfill(context.Background(), task)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedCalls, mockCoord.ProcessCalls)
		})
	}
}

// Test service Stop without Start
func TestServiceStopWithoutStart(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	cfg := &Config{Concurrency: 10}
	redisOpt := &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	}
	mockDAG := &testutil.FakeDAGReader{}
	mockCoord := &coordinatorfake.FakeCoordinator{}
	mockAdmin := &adminfake.FakeAdminService{}

	svc, err := NewService(log, cfg, redisOpt, mockDAG, mockCoord, mockAdmin)
	require.NoError(t, err)

	// Should not panic when stopping without starting
	err = svc.Stop()
	assert.NoError(t, err)
}

// Test HandleConsolidation
func TestHandleConsolidation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	mockDAG := &testutil.FakeDAGReader{}
	mockCoord := &coordinatorfake.FakeCoordinator{}

	// Create service manually to avoid Redis dependency
	svc := &service{
		log:         log.WithField("service", "scheduler"),
		cfg:         &Config{Concurrency: 1},
		done:        make(chan struct{}),
		dag:         mockDAG,
		coordinator: mockCoord,
	}

	// Create asynq task
	task := asynq.NewTask(ConsolidationTaskType, nil)

	err := svc.HandleConsolidation(context.Background(), task)
	require.NoError(t, err)

	// Should have called RunConsolidation once
	assert.Equal(t, 1, mockCoord.ConsolidationCalls)
}

// Benchmark tests
func BenchmarkExtractModelID(b *testing.B) {
	taskType := "transformation:analytics.block_propagation:forward"

	b.ResetTimer()
	for range b.N {
		_ = extractModelID(taskType)
	}
}

func BenchmarkNewService(b *testing.B) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	cfg := &Config{Concurrency: 10}
	redisOpt := &redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	}
	mockDAG := &testutil.FakeDAGReader{}
	mockCoord := &coordinatorfake.FakeCoordinator{}
	mockAdmin := &adminfake.FakeAdminService{}

	b.ResetTimer()
	for range b.N {
		_, _ = NewService(log, cfg, redisOpt, mockDAG, mockCoord, mockAdmin)
	}
}

// TestRegisterAllHandlers tests that handlers are registered on all instances
func TestRegisterAllHandlers(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	redisOpt := &redis.Options{
		Addr: "localhost:6379",
	}

	mockTrans := &testutil.FakeTransformation{
		ID: "test.model",
		Config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
		},
	}

	mockDAG := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{mockTrans},
	}

	mockCoord := &coordinatorfake.FakeCoordinator{}

	cfg := &Config{
		Concurrency:   10,
		Consolidation: "@hourly",
	}

	mockAdmin := &adminfake.FakeAdminService{}
	svc, err := NewService(logger, cfg, redisOpt, mockDAG, mockCoord, mockAdmin)
	require.NoError(t, err)

	s := svc.(*service)

	// Call registerAllHandlers
	s.registerAllHandlers()

	// Verify mux has handlers registered
	// We can't directly inspect mux handlers, but we can verify no panic occurred
	// and the method completed successfully
	assert.NotNil(t, s.mux, "ServeMux should be initialized")
}

// TestRegisterHandlersWithEmptySchedules verifies that forward and backfill handlers
// are registered even when schedules are initially empty. This is critical for config
// overrides: if backfill is "" at startup and later set to "@every 1m" via a live
// override, the asynq mux must already have a handler registered for the task type.
func TestRegisterHandlersWithEmptySchedules(t *testing.T) {
	tests := []struct {
		name             string
		handler          *testutil.FakeHandler
		forwardTaskType  string
		backfillTaskType string
	}{
		{
			name: "empty schedules still registers handlers",
			handler: &testutil.FakeHandler{
				HandlerConfig: &incremental.Config{
					Type:      transformation.TypeIncremental,
					Schedules: &incremental.SchedulesConfig{ForwardFill: "", Backfill: ""},
				},
			},
			forwardTaskType:  "transformation:test.model:forward",
			backfillTaskType: "transformation:test.model:back",
		},
		{
			name: "populated schedules also registers handlers",
			handler: &testutil.FakeHandler{
				HandlerConfig: &incremental.Config{
					Type:      transformation.TypeIncremental,
					Schedules: &incremental.SchedulesConfig{ForwardFill: "@every 30s", Backfill: "@every 1m"},
				},
			},
			forwardTaskType:  "transformation:test.model:forward",
			backfillTaskType: "transformation:test.model:back",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			svc := &service{
				log:  log.WithField("service", "scheduler"),
				cfg:  &Config{Concurrency: 1},
				done: make(chan struct{}),
				dag: &testutil.FakeDAGReader{
					// Mirror the legacy mock: unknown IDs resolve to a default node so
					// dispatching a task through the mux exercises the handler.
					TransformationFallbackFn: func(id string) models.Transformation {
						return &testutil.FakeTransformation{ID: id}
					},
				},
				mux:         asynq.NewServeMux(),
				coordinator: &coordinatorfake.FakeCoordinator{},
			}

			trans := &testutil.FakeTransformation{
				ID: "test.model",
				Config: transformation.Config{
					Database: "test_db",
					Table:    "test_table",
				},
				Handler: tt.handler,
			}

			svc.registerTransformationHandlers(trans)

			// Verify handlers are registered by dispatching tasks through the mux.
			// If no handler is registered, ProcessTask returns "handler not found".
			forwardTask := asynq.NewTask(tt.forwardTaskType, nil)
			err := svc.mux.ProcessTask(context.Background(), forwardTask)
			require.NoError(t, err, "forward handler should be registered regardless of schedule config")

			backfillTask := asynq.NewTask(tt.backfillTaskType, nil)
			err = svc.mux.ProcessTask(context.Background(), backfillTask)
			assert.NoError(t, err, "backfill handler should be registered regardless of schedule config")
		})
	}
}

func TestExtractExternalTaskComponents(t *testing.T) {
	tests := []struct {
		name          string
		taskType      string
		expectedID    string
		expectedError bool
	}{
		{
			name:          "valid incremental task",
			taskType:      "external:db.table:incremental",
			expectedID:    "db.table",
			expectedError: false,
		},
		{
			name:          "valid full task",
			taskType:      "external:analytics.events:full",
			expectedID:    "analytics.events",
			expectedError: false,
		},
		{
			name:          "invalid - only two parts",
			taskType:      "external:db.table",
			expectedID:    "",
			expectedError: true,
		},
		{
			name:          "invalid - only one part",
			taskType:      "external",
			expectedID:    "",
			expectedError: true,
		},
		{
			name:          "invalid - four parts",
			taskType:      "external:db:table:extra:part",
			expectedID:    "",
			expectedError: true,
		},
		{
			name:          "invalid - empty string",
			taskType:      "",
			expectedID:    "",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modelID, err := extractExternalTaskComponents(tt.taskType)
			if tt.expectedError {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidExternalTaskType)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedID, modelID)
			}
		})
	}
}
