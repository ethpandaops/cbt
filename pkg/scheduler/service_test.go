package scheduler

import (
	"context"
	"errors"
	"testing"

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

			svc, err := NewService(log, tt.cfg, redisOpt, mockDAG, mockCoord)

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
							Type:     transformation.TypeIncremental,
							Database: "test_db",
							Table:    "model",
						},
						handler: &mockHandler{
							backfillEnabled:  true,
							backfillSchedule: "*/5 * * * *",
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

	svc, err := NewService(log, cfg, redisOpt, mockDAG, mockCoord)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewService(log, cfg, redisOpt, mockDAG, mockCoord)
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

func (m *mockCoordinator) ProcessBoundsOrchestration(_ context.Context) {
	// Mock implementation - does nothing
}

var _ coordinator.Service = (*mockCoordinator)(nil)

// Mock handler for testing
type mockHandler struct {
	schedule           string
	forwardFillEnabled bool
	forwardSchedule    string
	backfillEnabled    bool
	backfillSchedule   string
}

func (m *mockHandler) Type() transformation.Type {
	return transformation.TypeIncremental
}

func (m *mockHandler) Config() any {
	return &transformation.Config{
		Type:     transformation.TypeIncremental,
		Database: "test",
		Table:    "test",
	}
}

func (m *mockHandler) Validate() error {
	return nil
}

func (m *mockHandler) ShouldTrackPosition() bool {
	return true
}

func (m *mockHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return map[string]any{}
}

func (m *mockHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{
		Database: "admin",
		Table:    "cbt",
	}
}

func (m *mockHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

func (m *mockHandler) GetSchedule() string {
	return m.schedule
}

func (m *mockHandler) IsForwardFillEnabled() bool {
	return m.forwardFillEnabled
}

func (m *mockHandler) GetForwardSchedule() string {
	return m.forwardSchedule
}

func (m *mockHandler) IsBackfillEnabled() bool {
	return m.backfillEnabled
}

func (m *mockHandler) GetBackfillSchedule() string {
	return m.backfillSchedule
}

type mockTransformation struct {
	id      string
	conf    transformation.Config
	handler transformation.Handler
	deps    []string
	sql     string
	typ     string
}

func (m *mockTransformation) GetID() string                     { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config { return &m.conf }
func (m *mockTransformation) GetHandler() transformation.Handler {
	if m.handler != nil {
		return m.handler
	}
	// Return a default mock handler for backward compatibility
	return &mockHandler{}
}
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
