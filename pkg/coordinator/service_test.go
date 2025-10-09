package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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

// Test Process method logic with miniredis
func TestProcessForward(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	// Start miniredis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisOpt := &redis.Options{
		Addr: mr.Addr(),
	}

	mockDAG := &mockDAGReader{
		nodes: make(map[string]models.Node),
	}
	mockAdmin := &mockAdminService{
		lastPositions: make(map[string]uint64),
	}
	mockValidator := validation.NewMockValidator()

	svc, err := NewService(log, redisOpt, mockDAG, mockAdmin, mockValidator)
	require.NoError(t, err)

	// Start the service to initialize queueManager
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = svc.Start(ctx)
	require.NoError(t, err)
	defer svc.Stop()

	// Create a mock transformation
	mockTrans := &mockTransformation{
		id:       "test.model",
		interval: 100,
	}

	// Test that Process doesn't panic
	svc.Process(mockTrans, DirectionForward)

	// Test passes if no panic occurs
}

// Test concurrent access to Process method with miniredis
func TestProcessBackfill(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	// Start miniredis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisOpt := &redis.Options{
		Addr: mr.Addr(),
	}

	mockDAG := &mockDAGReader{
		nodes: make(map[string]models.Node),
	}
	mockAdmin := &mockAdminService{
		lastPositions: make(map[string]uint64),
	}
	mockValidator := validation.NewMockValidator()

	svc, err := NewService(log, redisOpt, mockDAG, mockAdmin, mockValidator)
	require.NoError(t, err)

	// Start the service to initialize queueManager
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = svc.Start(ctx)
	require.NoError(t, err)
	defer svc.Stop()

	// Create a mock transformation
	mockTrans := &mockTransformation{
		id:       "test.model",
		interval: 100,
	}

	// Test that Process doesn't panic with backfill direction
	svc.Process(mockTrans, DirectionBack)

	// Test passes if no panic occurs
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

func (m *mockAdminService) GetIncrementalAdminDatabase() string {
	return "admin"
}

func (m *mockAdminService) GetIncrementalAdminTable() string {
	return "cbt"
}

func (m *mockAdminService) GetScheduledAdminDatabase() string {
	return "admin"
}

func (m *mockAdminService) GetScheduledAdminTable() string {
	return "cbt_scheduled"
}

func (m *mockAdminService) RecordScheduledCompletion(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (m *mockAdminService) GetLastScheduledExecution(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (m *mockAdminService) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}

// Mock handler for testing
type mockHandler struct {
	interval               uint64
	minInterval            uint64
	forwardFillEnabled     bool
	backfillEnabled        bool
	allowsPartialIntervals bool
	dependencies           []string
	limits                 *struct{ Min, Max uint64 }
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

func (m *mockHandler) GetMaxInterval() uint64 {
	return m.interval
}

func (m *mockHandler) GetMinInterval() uint64 {
	return m.minInterval
}

func (m *mockHandler) AllowsPartialIntervals() bool {
	return m.allowsPartialIntervals
}

func (m *mockHandler) IsForwardFillEnabled() bool {
	return m.forwardFillEnabled
}

func (m *mockHandler) IsBackfillEnabled() bool {
	return m.backfillEnabled
}

func (m *mockHandler) GetFlattenedDependencies() []string {
	return m.dependencies
}

func (m *mockHandler) GetLimits() *struct{ Min, Max uint64 } {
	return m.limits
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

// Mock transformation for testing
type mockTransformation struct {
	id       string
	interval uint64
	config   *transformation.Config // Allow custom config for testing
	handler  transformation.Handler // Mock handler for testing
}

func (m *mockTransformation) GetID() string { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config {
	if m.config != nil {
		return m.config
	}
	// Default config for new architecture
	return &transformation.Config{
		Type:     transformation.TypeIncremental,
		Database: "test_db",
		Table:    "test_table",
	}
}
func (m *mockTransformation) GetHandler() transformation.Handler {
	if m.handler != nil {
		return m.handler
	}
	// Return a default mock handler
	return &mockHandler{
		interval:               m.interval,
		forwardFillEnabled:     true,
		allowsPartialIntervals: false,
		dependencies:           []string{},
	}
}
func (m *mockTransformation) GetValue() string                  { return "" }
func (m *mockTransformation) GetDependencies() []string         { return []string{} }
func (m *mockTransformation) GetSQL() string                    { return "" }
func (m *mockTransformation) GetType() string                   { return "transformation" }
func (m *mockTransformation) GetEnvironmentVariables() []string { return []string{} }
func (m *mockTransformation) SetDefaultDatabase(defaultDB string) {
	if m.config != nil && m.config.Database == "" {
		m.config.Database = defaultDB
	}
}

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
					Type:     transformation.TypeIncremental,
					Database: "test",
					Table:    "model",
				},
				handler: &mockHandler{
					interval:               tt.interval,
					minInterval:            tt.minPartial,
					allowsPartialIntervals: tt.allowPartial,
					forwardFillEnabled:     true,
					dependencies:           []string{"dep1"},
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

// Test IntervalConfig validation is now handled by incremental handler
// This test has been removed as IntervalConfig is now internal to the incremental package

// Test AllowsPartialIntervals through handler
func TestAllowsPartialIntervals(t *testing.T) {
	tests := []struct {
		name     string
		handler  *mockHandler
		expected bool
	}{
		{
			name: "partial_intervals_enabled",
			handler: &mockHandler{
				interval:               3600,
				allowsPartialIntervals: true,
			},
			expected: true,
		},
		{
			name: "partial_intervals_disabled",
			handler: &mockHandler{
				interval:               3600,
				allowsPartialIntervals: false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.handler.AllowsPartialIntervals()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test GetMaxInterval through handler
func TestGetMaxInterval(t *testing.T) {
	tests := []struct {
		name     string
		handler  *mockHandler
		expected uint64
	}{
		{
			name: "with_interval",
			handler: &mockHandler{
				interval: 3600,
			},
			expected: 3600,
		},
		{
			name: "without_interval",
			handler: &mockHandler{
				interval: 0,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.handler.GetMaxInterval()
			assert.Equal(t, tt.expected, result)
		})
	}
}
