package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors
var (
	errMockDAGError       = errors.New("mock DAG error")
	errNotATransformation = errors.New("not a transformation")
	errNotImplemented     = errors.New("not implemented")
)

// Test service creation (ethPandaOps requirement)
func TestNewService(t *testing.T) {
	log := logrus.New()
	// Use nil for Redis options - we're not testing Redis connectivity
	var redisOpt *redis.Options
	mockDAG := &mockDAGReader{}
	mockAdmin := &mockAdminService{}
	mockValidator := validation.NewMockValidator()

	svc, err := NewService(log, redisOpt, mockDAG, mockAdmin, mockValidator)
	require.NoError(t, err)
	assert.NotNil(t, svc)

	// Verify it implements the Service interface
	var _ = svc
}

// Test service creation and basic lifecycle without Redis
func TestServiceCreation(t *testing.T) {
	log := logrus.New()
	var redisOpt *redis.Options
	mockDAG := &mockDAGReader{}
	mockAdmin := &mockAdminService{}
	mockValidator := validation.NewMockValidator()

	svc, err := NewService(log, redisOpt, mockDAG, mockAdmin, mockValidator)
	require.NoError(t, err)

	serviceCast := svc.(*service)

	// Verify internal structures are initialized
	assert.NotNil(t, serviceCast.done)
	assert.NotNil(t, serviceCast.taskCheck)
	assert.NotNil(t, serviceCast.taskMark)
	assert.NotNil(t, serviceCast.log)
	assert.NotNil(t, serviceCast.dag)
	assert.NotNil(t, serviceCast.admin)
	assert.NotNil(t, serviceCast.validator)

	// Note: We can't test Start/Stop without mocking Redis/Asynq properly
	// Those would be integration tests, not unit tests
}

// Test Process method logic (without actual Redis connection)
func TestProcessLogic(t *testing.T) {
	// Skip this test as Process requires queueManager which needs Redis
	t.Skip("Process method requires Redis connection via queueManager - this is an integration test")

	// Note: The Process method depends on queueManager and inspector being initialized
	// during Start(), which requires a valid Redis connection. Without Start being called,
	// queueManager is nil and Process will panic with a nil pointer dereference.
	// This functionality should be tested in integration tests with a real or mocked Redis.
}

// Test concurrent access to service methods (without Redis)
func TestConcurrentAccess(t *testing.T) {
	// Skip this test as Process requires queueManager which needs Redis
	t.Skip("Process method requires Redis connection via queueManager - this is an integration test")

	// Note: Testing concurrent access to Process method requires queueManager to be initialized,
	// which only happens during Start() with a valid Redis connection.
	// This functionality should be tested in integration tests with proper Redis mocking.
}

// Benchmark service creation (without Redis)
func BenchmarkServiceCreation(b *testing.B) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	var redisOpt *redis.Options
	mockDAG := &mockDAGReader{
		nodes: map[string]models.Node{
			"model.bench": {
				NodeType: models.NodeTypeTransformation,
				Model:    &mockTransformation{id: "model.bench", interval: 100},
			},
		},
	}
	mockAdmin := &mockAdminService{
		lastPositions: map[string]uint64{
			"model.bench": 10000,
		},
	}
	mockValidator := validation.NewMockValidator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewService(log, redisOpt, mockDAG, mockAdmin, mockValidator)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Mock implementations for testing

type mockDAGReader struct {
	nodes map[string]models.Node
	mu    sync.RWMutex
}

func (m *mockDAGReader) GetNode(id string) (models.Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if node, ok := m.nodes[id]; ok {
		return node, nil
	}
	return models.Node{}, errMockDAGError
}

func (m *mockDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	node, err := m.GetNode(id)
	if err != nil {
		return nil, err
	}
	if trans, ok := node.Model.(models.Transformation); ok {
		return trans, nil
	}
	return nil, errNotATransformation
}

func (m *mockDAGReader) GetExternalNode(_ string) (models.External, error) {
	return nil, errNotImplemented
}

func (m *mockDAGReader) GetDependencies(_ string) []string               { return []string{} }
func (m *mockDAGReader) GetDependents(_ string) []string                 { return []string{} }
func (m *mockDAGReader) GetAllDependencies(_ string) []string            { return []string{} }
func (m *mockDAGReader) GetAllDependents(_ string) []string              { return []string{} }
func (m *mockDAGReader) GetTransformationNodes() []models.Transformation { return nil }
func (m *mockDAGReader) GetExternalNodes() []models.Node                 { return []models.Node{} }
func (m *mockDAGReader) IsPathBetween(_, _ string) bool                  { return false }

type mockAdminService struct {
	lastPositions  map[string]uint64
	firstPositions map[string]uint64
	gaps           map[string][]admin.GapInfo
	externalBounds map[string]*admin.BoundsCache
	mu             sync.RWMutex
}

func (m *mockAdminService) GetLastProcessedEndPosition(_ context.Context, modelID string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pos, ok := m.lastPositions[modelID]; ok {
		return pos, nil
	}
	return 0, nil
}

func (m *mockAdminService) GetNextUnprocessedPosition(_ context.Context, modelID string) (uint64, error) {
	// For mock purposes, this is the same as GetLastProcessedEndPosition
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pos, ok := m.lastPositions[modelID]; ok {
		return pos, nil
	}
	return 0, nil
}

func (m *mockAdminService) GetLastProcessedPosition(_ context.Context, modelID string) (uint64, error) {
	// For mock purposes, return the last position if it exists, otherwise 0
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pos, ok := m.lastPositions[modelID]; ok {
		// In real implementation, this would be just the position without interval
		// For testing, we'll return the position directly
		return pos, nil
	}
	return 0, nil
}

func (m *mockAdminService) GetFirstPosition(_ context.Context, modelID string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pos, ok := m.firstPositions[modelID]; ok {
		return pos, nil
	}
	return 0, nil
}

func (m *mockAdminService) RecordCompletion(_ context.Context, modelID string, position, _ uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastPositions[modelID] = position
	return nil
}

func (m *mockAdminService) FindGaps(_ context.Context, modelID string, _, _, _ uint64) ([]admin.GapInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if gaps, ok := m.gaps[modelID]; ok {
		return gaps, nil
	}
	return []admin.GapInfo{}, nil
}

func (m *mockAdminService) GetExternalBounds(_ context.Context, modelID string) (*admin.BoundsCache, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if bounds, ok := m.externalBounds[modelID]; ok {
		return bounds, nil
	}
	return nil, nil
}

func (m *mockAdminService) SetExternalBounds(_ context.Context, cache *admin.BoundsCache) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.externalBounds == nil {
		m.externalBounds = make(map[string]*admin.BoundsCache)
	}
	m.externalBounds[cache.ModelID] = cache
	return nil
}

func (m *mockAdminService) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return false, nil
}

func (m *mockAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *mockAdminService) GetAdminDatabase() string {
	return "admin"
}

func (m *mockAdminService) GetAdminTable() string {
	return "cbt"
}

// Mock transformation for testing
type mockTransformation struct {
	id       string
	interval uint64
	config   *transformation.Config // Allow custom config for testing
}

func (m *mockTransformation) GetID() string { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config {
	if m.config != nil {
		return m.config
	}
	// Default config for backward compatibility
	return &transformation.Config{
		Database: "test_db",
		Table:    "test_table",
		ForwardFill: &transformation.ForwardFillConfig{
			Interval: m.interval,
			Schedule: "@every 1m",
		},
		Dependencies: []string{},
	}
}
func (m *mockTransformation) GetValue() string                  { return "" }
func (m *mockTransformation) GetDependencies() []string         { return []string{} }
func (m *mockTransformation) GetSQL() string                    { return "" }
func (m *mockTransformation) GetType() string                   { return "transformation" }
func (m *mockTransformation) GetEnvironmentVariables() []string { return []string{} }

// Ensure mockTransformation implements the Transformation interface
var _ models.Transformation = (*mockTransformation)(nil)

// Test adjustIntervalForDependencies - the core logic for partial interval support
func TestAdjustIntervalForDependencies(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name               string
		nextPos            uint64
		interval           uint64
		maxValid           uint64
		minPartial         uint64
		allowPartial       bool
		expectedInterval   uint64
		expectedShouldStop bool
		description        string
	}{
		{
			name:               "full_interval_available",
			nextPos:            100,
			interval:           10,
			maxValid:           120,
			minPartial:         0,
			allowPartial:       true,
			expectedInterval:   10,
			expectedShouldStop: false,
			description:        "When full interval fits within dependencies, use full interval",
		},
		{
			name:               "partial_interval_needed",
			nextPos:            100,
			interval:           10,
			maxValid:           105,
			minPartial:         0,
			allowPartial:       true,
			expectedInterval:   5,
			expectedShouldStop: false,
			description:        "When dependencies limit interval, use available partial",
		},
		{
			name:               "partial_interval_below_minimum",
			nextPos:            100,
			interval:           10,
			maxValid:           102,
			minPartial:         5,
			allowPartial:       true,
			expectedInterval:   10,
			expectedShouldStop: true,
			description:        "When available interval is below minimum, stop processing",
		},
		{
			name:               "partial_interval_meets_minimum",
			nextPos:            100,
			interval:           10,
			maxValid:           107,
			minPartial:         5,
			allowPartial:       true,
			expectedInterval:   7,
			expectedShouldStop: false,
			description:        "When available interval meets minimum, use it",
		},
		{
			name:               "position_at_max_valid",
			nextPos:            100,
			interval:           10,
			maxValid:           100,
			minPartial:         0,
			allowPartial:       true,
			expectedInterval:   10,
			expectedShouldStop: false,
			description:        "When position is at maxValid, return original interval",
		},
		{
			name:               "position_beyond_max_valid",
			nextPos:            100,
			interval:           10,
			maxValid:           90,
			minPartial:         0,
			allowPartial:       true,
			expectedInterval:   10,
			expectedShouldStop: false,
			description:        "When position is beyond maxValid, return original interval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock validator
			mockValidator := validation.NewMockValidator()
			mockValidator.GetValidRangeFunc = func(_ context.Context, _ string) (uint64, uint64, error) {
				return 0, tt.maxValid, nil
			}

			// Setup mock transformation
			trans := &mockTransformation{
				id: "test.model",
				config: &transformation.Config{
					Database: "test",
					Table:    "model",
					ForwardFill: &transformation.ForwardFillConfig{
						Interval:              tt.interval,
						Schedule:              "@every 1m",
						AllowPartialIntervals: tt.allowPartial,
						MinPartialInterval:    tt.minPartial,
					},
					Dependencies: []string{"dep1"},
				},
			}

			// Create service
			svc := &service{
				log:       logger.WithField("test", tt.name),
				validator: mockValidator,
			}

			// Test the method
			ctx := context.Background()
			adjustedInterval, shouldStop := svc.adjustIntervalForDependencies(ctx, trans, tt.nextPos, tt.interval)

			// Assert results
			assert.Equal(t, tt.expectedInterval, adjustedInterval, tt.description)
			assert.Equal(t, tt.expectedShouldStop, shouldStop, tt.description)
		})
	}
}

// Test ForwardFillConfig validation
func TestForwardFillConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  transformation.ForwardFillConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_with_partial_intervals",
			config: transformation.ForwardFillConfig{
				Interval:              3600,
				Schedule:              "0 * * * *",
				AllowPartialIntervals: true,
				MinPartialInterval:    900,
			},
			wantErr: false,
		},
		{
			name: "valid_without_partial_intervals",
			config: transformation.ForwardFillConfig{
				Interval: 3600,
				Schedule: "0 * * * *",
			},
			wantErr: false,
		},
		{
			name: "min_partial_exceeds_interval",
			config: transformation.ForwardFillConfig{
				Interval:              3600,
				Schedule:              "0 * * * *",
				AllowPartialIntervals: true,
				MinPartialInterval:    7200,
			},
			wantErr: true,
			errMsg:  "min_partial_interval cannot exceed interval",
		},
		{
			name: "min_partial_without_allow_flag",
			config: transformation.ForwardFillConfig{
				Interval:              3600,
				Schedule:              "0 * * * *",
				AllowPartialIntervals: false,
				MinPartialInterval:    900,
			},
			wantErr: true,
			errMsg:  "min_partial_interval requires allow_partial_intervals to be true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test IsPartialIntervalsAllowed helper function
func TestIsPartialIntervalsAllowed(t *testing.T) {
	tests := []struct {
		name     string
		config   *transformation.Config
		expected bool
	}{
		{
			name: "partial_intervals_enabled",
			config: &transformation.Config{
				ForwardFill: &transformation.ForwardFillConfig{
					AllowPartialIntervals: true,
				},
			},
			expected: true,
		},
		{
			name: "partial_intervals_disabled",
			config: &transformation.Config{
				ForwardFill: &transformation.ForwardFillConfig{
					AllowPartialIntervals: false,
				},
			},
			expected: false,
		},
		{
			name:     "no_forwardfill_config",
			config:   &transformation.Config{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.IsPartialIntervalsAllowed()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test GetMinPartialInterval helper function
func TestGetMinPartialInterval(t *testing.T) {
	tests := []struct {
		name     string
		config   *transformation.Config
		expected uint64
	}{
		{
			name: "with_min_partial",
			config: &transformation.Config{
				ForwardFill: &transformation.ForwardFillConfig{
					MinPartialInterval: 500,
				},
			},
			expected: 500,
		},
		{
			name: "without_min_partial",
			config: &transformation.Config{
				ForwardFill: &transformation.ForwardFillConfig{},
			},
			expected: 0,
		},
		{
			name:     "no_forwardfill_config",
			config:   &transformation.Config{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.GetMinPartialInterval()
			assert.Equal(t, tt.expected, result)
		})
	}
}
