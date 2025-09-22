package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors
var (
	errMockNode = errors.New("node not found")
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
			mockDAG := &mockDAGReader{}
			mockCoord := &mockCoordinator{}
			mockAdmin := newMockAdminService()

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
			expected: "test.model",
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
		setupMocks    func(*mockDAGReader, *mockCoordinator)
		wantErr       bool
		expectedCalls int
	}{
		{
			name:     "successful forward processing",
			taskType: "transformation:test.model:forward",
			setupMocks: func(dag *mockDAGReader, _ *mockCoordinator) {
				dag.transformations = []models.Transformation{
					&mockTransformation{
						id:   "test.model",
						conf: transformation.Config{},
					},
				}
			},
			wantErr:       false,
			expectedCalls: 1,
		},
		{
			name:     "node not found",
			taskType: "transformation:unknown.model:forward",
			setupMocks: func(dag *mockDAGReader, _ *mockCoordinator) {
				dag.nodeNotFound = true
			},
			wantErr:       true,
			expectedCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockDAG := &mockDAGReader{}
			mockCoord := &mockCoordinator{}

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

			assert.Equal(t, tt.expectedCalls, mockCoord.processCalls)
		})
	}
}

// Test HandleScheduledBackfill
func TestHandleScheduledBackfill(t *testing.T) {
	tests := []struct {
		name          string
		taskType      string
		setupMocks    func(*mockDAGReader, *mockCoordinator)
		wantErr       bool
		expectedCalls int
	}{
		{
			name:     "successful backfill processing",
			taskType: "transformation:test.model:back",
			setupMocks: func(dag *mockDAGReader, _ *mockCoordinator) {
				dag.transformations = []models.Transformation{
					&mockTransformation{
						id: "test.model",
						conf: transformation.Config{
							Database: "test_db",
							Table:    "model",
							Interval: &transformation.IntervalConfig{
								Max: 100,
								Min: 0,
							},
							Schedules: &transformation.SchedulesConfig{
								ForwardFill: "@every 1m",
								Backfill:    "*/5 * * * *",
							},
							Dependencies: []transformation.Dependency{},
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
			setupMocks: func(dag *mockDAGReader, _ *mockCoordinator) {
				dag.nodeNotFound = true
			},
			wantErr:       true,
			expectedCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockDAG := &mockDAGReader{}
			mockCoord := &mockCoordinator{}

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

			assert.Equal(t, tt.expectedCalls, mockCoord.processCalls)
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
	mockDAG := &mockDAGReader{}
	mockCoord := &mockCoordinator{}
	mockAdmin := newMockAdminService()

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
	mockDAG := &mockDAGReader{}
	mockCoord := &mockCoordinator{}

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
	assert.Equal(t, 1, mockCoord.consolidationCalls)
}

// Benchmark tests
func BenchmarkExtractModelID(b *testing.B) {
	taskType := "transformation:analytics.block_propagation:forward"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	mockDAG := &mockDAGReader{}
	mockCoord := &mockCoordinator{}
	mockAdmin := newMockAdminService()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewService(log, cfg, redisOpt, mockDAG, mockCoord, mockAdmin)
	}
}

// Mock implementations

type mockDAGReader struct {
	transformations []models.Transformation
	nodeNotFound    bool
}

func (m *mockDAGReader) GetNode(_ string) (models.Node, error) {
	return models.Node{}, nil
}

func (m *mockDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	if m.nodeNotFound {
		return nil, errMockNode
	}
	for _, t := range m.transformations {
		if t.GetID() == id {
			return t, nil
		}
	}
	// Return a default transformation if not found
	return &mockTransformation{id: id}, nil
}

func (m *mockDAGReader) GetExternalNode(_ string) (models.External, error) {
	return nil, nil
}

func (m *mockDAGReader) GetDependencies(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetDependents(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetAllDependencies(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetAllDependents(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetTransformationNodes() []models.Transformation {
	return m.transformations
}

func (m *mockDAGReader) GetExternalNodes() []models.Node {
	return []models.Node{}
}

func (m *mockDAGReader) IsPathBetween(_, _ string) bool {
	return false
}

var _ models.DAGReader = (*mockDAGReader)(nil)

type mockCoordinator struct {
	processCalls       int
	processErr         error
	consolidationCalls int
}

func (m *mockCoordinator) Start(_ context.Context) error {
	return m.processErr
}

func (m *mockCoordinator) Stop() error {
	return m.processErr
}

func (m *mockCoordinator) Process(_ models.Transformation, _ coordinator.Direction) {
	m.processCalls++
}

func (m *mockCoordinator) RunConsolidation(_ context.Context) {
	m.consolidationCalls++
}

func (m *mockCoordinator) ProcessExternalScan(_, _ string) {
	// Mock implementation - does nothing
}

var _ coordinator.Service = (*mockCoordinator)(nil)

// mockAdminService implements admin.Service interface for testing
type mockAdminService struct {
	externalBounds map[string]*admin.BoundsCache
}

func newMockAdminService() *mockAdminService {
	return &mockAdminService{
		externalBounds: make(map[string]*admin.BoundsCache),
	}
}

func (m *mockAdminService) GetLastProcessedEndPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) GetNextUnprocessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) GetLastProcessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) GetFirstPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return nil
}

func (m *mockAdminService) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return false, nil
}

func (m *mockAdminService) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return nil, nil
}

func (m *mockAdminService) Consolidate(_ context.Context, _, _ uint64) error {
	return nil
}

func (m *mockAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *mockAdminService) ClearCompletions(_ context.Context, _ string) error {
	return nil
}

func (m *mockAdminService) GetExternalBounds(_ context.Context, modelID string) (*admin.BoundsCache, error) {
	if bounds, ok := m.externalBounds[modelID]; ok {
		return bounds, nil
	}
	return nil, nil
}

func (m *mockAdminService) SetExternalBounds(_ context.Context, bounds *admin.BoundsCache) error {
	if m.externalBounds == nil {
		m.externalBounds = make(map[string]*admin.BoundsCache)
	}
	m.externalBounds[bounds.ModelID] = bounds
	return nil
}

func (m *mockAdminService) GetAdminDatabase() string {
	return "admin"
}

func (m *mockAdminService) GetAdminTable() string {
	return "cbt"
}

var _ admin.Service = (*mockAdminService)(nil)

type mockTransformation struct {
	id   string
	conf transformation.Config
	deps []string
	sql  string
	typ  string
}

func (m *mockTransformation) GetID() string                     { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config { return &m.conf }
func (m *mockTransformation) GetValue() string                  { return "" }
func (m *mockTransformation) GetDependencies() []string         { return m.deps }
func (m *mockTransformation) GetSQL() string                    { return m.sql }
func (m *mockTransformation) GetType() string                   { return m.typ }
func (m *mockTransformation) GetEnvironmentVariables() []string { return []string{} }
func (m *mockTransformation) SetDefaultDatabase(defaultDB string) {
	if m.conf.Database == "" {
		m.conf.Database = defaultDB
	}
}

var _ models.Transformation = (*mockTransformation)(nil)

// TestEmptyScheduleHandling tests that empty schedule values are handled gracefully
func TestEmptyScheduleHandling(t *testing.T) {
	tests := []struct {
		name                string
		forwardFillSchedule string
		backfillSchedule    string
		expectError         bool
		description         string
	}{
		{
			name:                "both schedules empty",
			forwardFillSchedule: "",
			backfillSchedule:    "",
			expectError:         false,
			description:         "Empty schedules should not cause errors",
		},
		{
			name:                "forward empty, backfill set",
			forwardFillSchedule: "",
			backfillSchedule:    "@every 5m",
			expectError:         false,
			description:         "Only backfill should be registered",
		},
		{
			name:                "forward set, backfill empty",
			forwardFillSchedule: "@every 1m",
			backfillSchedule:    "",
			expectError:         false,
			description:         "Only forward fill should be registered",
		},
		{
			name:                "both schedules set",
			forwardFillSchedule: "@every 1m",
			backfillSchedule:    "@every 5m",
			expectError:         false,
			description:         "Both tasks should be registered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			redisOpt := &redis.Options{
				Addr: "localhost:6379",
			}

			// Create mock DAG with test transformation
			mockTransformation := &mockTransformation{
				id: "test.model",
				conf: transformation.Config{
					Database: "test_db",
					Table:    "test_table",
					Schedules: &transformation.SchedulesConfig{
						ForwardFill: tt.forwardFillSchedule,
						Backfill:    tt.backfillSchedule,
					},
				},
			}

			mockDAG := &mockDAGReader{
				transformations: []models.Transformation{mockTransformation},
			}

			mockCoordinator := &mockCoordinator{}

			cfg := &Config{
				Concurrency:   10,
				Consolidation: "", // Empty consolidation schedule
			}

			// Create service
			mockAdmin := newMockAdminService()
			svc, err := NewService(logger, cfg, redisOpt, mockDAG, mockCoordinator, mockAdmin)
			require.NoError(t, err)

			s := svc.(*service)

			// Test reconcileSchedules
			err = s.reconcileSchedules()

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// TestRegisterScheduledTaskWithEmptySchedule tests the registerScheduledTask method directly
func TestRegisterScheduledTaskWithEmptySchedule(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	redisOpt := &redis.Options{
		Addr: "localhost:6379",
	}

	mockDAG := &mockDAGReader{}
	mockCoordinator := &mockCoordinator{}

	cfg := &Config{
		Concurrency: 10,
	}

	mockAdmin := newMockAdminService()
	svc, err := NewService(logger, cfg, redisOpt, mockDAG, mockCoordinator, mockAdmin)
	require.NoError(t, err)

	s := svc.(*service)

	// Test with empty schedule - should not error
	err = s.registerScheduledTask("test:task", "")
	assert.NoError(t, err, "Empty schedule should not cause error")

	// Test with valid schedule - would error without real Redis but that's OK for this test
	// We're just verifying the empty schedule path doesn't panic
}
