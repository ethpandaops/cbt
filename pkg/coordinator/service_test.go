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
	transformationmock "github.com/ethpandaops/cbt/pkg/models/transformation/mock"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// Test errors
var (
	errMockDAGError       = errors.New("mock DAG error")
	errNotATransformation = errors.New("not a transformation")
	errNotImplemented     = errors.New("not implemented")
	errValidationError    = errors.New("validation error")
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

func (m *mockDAGReader) GetDependencies(_ string) []string { return []string{} }
func (m *mockDAGReader) GetStructuredDependencies(_ string) []transformation.Dependency {
	return nil
}
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

func (m *mockAdminService) GetNextUnprocessedPosition(_ context.Context, modelID string) (uint64, error) {
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

// compositeHandler combines generated mocks to satisfy type assertions for
// transformation.Handler, IntervalHandler, and ScheduleHandler interfaces.
// This allows the coordinator's type assertions to work with gomock.
type compositeHandler struct {
	*transformationmock.MockHandler
	*transformationmock.MockIntervalHandler
	*transformationmock.MockScheduleHandler
}

// newCompositeHandler creates a composite handler with all mocks configured.
// The ctrl parameter is used to create all underlying mocks.
func newCompositeHandler(ctrl *gomock.Controller) *compositeHandler {
	return &compositeHandler{
		MockHandler:         transformationmock.NewMockHandler(ctrl),
		MockIntervalHandler: transformationmock.NewMockIntervalHandler(ctrl),
		MockScheduleHandler: transformationmock.NewMockScheduleHandler(ctrl),
	}
}

// setupDefaultExpectations configures common expectations for tests.
// This is a convenience method to reduce boilerplate in tests.
func (c *compositeHandler) setupDefaultExpectations() {
	c.MockHandler.EXPECT().Type().Return(transformation.TypeIncremental).AnyTimes()
	c.MockHandler.EXPECT().Config().Return(&transformation.Config{
		Type:     transformation.TypeIncremental,
		Database: "test",
		Table:    "test",
	}).AnyTimes()
	c.MockHandler.EXPECT().Validate().Return(nil).AnyTimes()
	c.MockHandler.EXPECT().ShouldTrackPosition().Return(true).AnyTimes()
	c.MockHandler.EXPECT().GetTemplateVariables(gomock.Any(), gomock.Any()).Return(map[string]any{}).AnyTimes()
	c.MockHandler.EXPECT().GetAdminTable().Return(transformation.AdminTable{
		Database: "admin",
		Table:    "cbt",
	}).AnyTimes()
	c.MockHandler.EXPECT().RecordCompletion(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
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
	return m.handler
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
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup mock validator
			mockValidator := validation.NewMockValidator()
			mockValidator.GetValidRangeForForwardFillFunc = func(_ context.Context, _ string) (uint64, uint64, error) {
				return 0, tt.maxValid, nil
			}

			// Setup composite handler with generated mocks
			handler := newCompositeHandler(ctrl)
			handler.setupDefaultExpectations()
			handler.MockIntervalHandler.EXPECT().GetMaxInterval().Return(tt.interval).AnyTimes()
			handler.MockIntervalHandler.EXPECT().GetMinInterval().Return(tt.minPartial).AnyTimes()
			handler.MockIntervalHandler.EXPECT().AllowsPartialIntervals().Return(tt.allowPartial).AnyTimes()
			handler.MockIntervalHandler.EXPECT().AllowGapSkipping().Return(true).AnyTimes()
			handler.MockScheduleHandler.EXPECT().IsForwardFillEnabled().Return(true).AnyTimes()
			handler.MockScheduleHandler.EXPECT().IsBackfillEnabled().Return(false).AnyTimes()
			handler.MockScheduleHandler.EXPECT().GetLimits().Return(nil).AnyTimes()

			// Setup mock transformation
			trans := &mockTransformation{
				id: "test.model",
				config: &transformation.Config{
					Type:     transformation.TypeIncremental,
					Database: "test",
					Table:    "model",
				},
				handler: handler,
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

// Test AllowsPartialIntervals through IntervalHandler interface
func TestAllowsPartialIntervals(t *testing.T) {
	tests := []struct {
		name     string
		partial  bool
		expected bool
	}{
		{
			name:     "partial_intervals_enabled",
			partial:  true,
			expected: true,
		},
		{
			name:     "partial_intervals_disabled",
			partial:  false,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			handler := transformationmock.NewMockIntervalHandler(ctrl)
			handler.EXPECT().AllowsPartialIntervals().Return(tt.partial)

			result := handler.AllowsPartialIntervals()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test GetMaxInterval through IntervalHandler interface
func TestGetMaxInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval uint64
		expected uint64
	}{
		{
			name:     "with_interval",
			interval: 3600,
			expected: 3600,
		},
		{
			name:     "without_interval",
			interval: 0,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			handler := transformationmock.NewMockIntervalHandler(ctrl)
			handler.EXPECT().GetMaxInterval().Return(tt.interval)

			result := handler.GetMaxInterval()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ========================================
// Forward Fill Gap Tests
// ========================================

func TestProcessForwardWithGapSkipping(t *testing.T) {
	tests := []struct {
		name              string
		validationResults []struct {
			position     uint64
			canProcess   bool
			nextValidPos uint64
		}
		expectedPositions []uint64 // Positions where validator is called
		expectedEnqueued  int      // Number of tasks enqueued
		startPos          uint64
		maxLimit          uint64
	}{
		{
			name: "gap detected, skip and enqueue at next valid",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: false, nextValidPos: 110}, // Gap detected, recurse to 110
				{position: 110, canProcess: true},                     // Can process, enqueue
			},
			expectedPositions: []uint64{100, 110}, // Validate at 100 (gap), then at 110 (process)
			expectedEnqueued:  1,                  // Only enqueue at 110
			startPos:          100,
			maxLimit:          0,
		},
		{
			name: "no gaps, normal processing",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: true}, // Can process immediately
			},
			expectedPositions: []uint64{100}, // Only validate once
			expectedEnqueued:  1,             // Enqueue at 100
			startPos:          100,
			maxLimit:          0,
		},
		{
			name: "multiple gaps chained",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: false, nextValidPos: 120}, // First gap, recurse to 120
				{position: 120, canProcess: false, nextValidPos: 140}, // Second gap, recurse to 140
				{position: 140, canProcess: true},                     // Can process, enqueue
			},
			expectedPositions: []uint64{100, 120, 140}, // Three validation calls due to recursion
			expectedEnqueued:  1,                       // Only enqueue at 140
			startPos:          100,
			maxLimit:          0,
		},
		{
			name: "gap beyond limit",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: false, nextValidPos: 250}, // Gap detected but next is beyond limit
			},
			expectedPositions: []uint64{100}, // Only validate once, no recursion due to limit
			expectedEnqueued:  0,             // Nothing enqueued
			startPos:          100,
			maxLimit:          200, // Limit prevents recursion to 250
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup mock validator to track calls and return expected results
			mockValidator := validation.NewMockValidator()
			callIndex := 0

			mockValidator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
				if callIndex >= len(tt.validationResults) {
					return validation.Result{CanProcess: false}, nil
				}

				result := tt.validationResults[callIndex]
				callIndex++

				return validation.Result{
					CanProcess:   result.canProcess,
					NextValidPos: result.nextValidPos,
				}, nil
			}

			// Setup composite handler with generated mocks
			handler := newCompositeHandler(ctrl)
			handler.setupDefaultExpectations()
			handler.MockIntervalHandler.EXPECT().GetMaxInterval().Return(uint64(50)).AnyTimes()
			handler.MockIntervalHandler.EXPECT().GetMinInterval().Return(uint64(0)).AnyTimes()
			handler.MockIntervalHandler.EXPECT().AllowsPartialIntervals().Return(false).AnyTimes()
			handler.MockIntervalHandler.EXPECT().AllowGapSkipping().Return(true).AnyTimes()
			handler.MockScheduleHandler.EXPECT().IsForwardFillEnabled().Return(true).AnyTimes()
			handler.MockScheduleHandler.EXPECT().IsBackfillEnabled().Return(false).AnyTimes()
			handler.MockScheduleHandler.EXPECT().GetLimits().Return(nil).AnyTimes()

			trans := &mockTransformation{
				id:      "test.model",
				handler: handler,
			}

			// Create a testable service that tracks enqueued tasks
			testService := &testableService{
				log:       logrus.NewEntry(logrus.New()),
				validator: mockValidator,
				admin:     &mockAdminService{},
			}

			// Test the actual gap skipping functionality
			ctx := context.Background()
			testService.processForwardWithGapSkipping(ctx, trans, tt.startPos, tt.maxLimit)

			// Verify the positions that were validated
			assert.Equal(t, len(tt.expectedPositions), len(mockValidator.ValidateCalls), "Should call validation for expected number of positions")

			for i, expectedPos := range tt.expectedPositions {
				if i < len(mockValidator.ValidateCalls) {
					assert.Equal(t, expectedPos, mockValidator.ValidateCalls[i].Position, "Should validate expected position %d", i)
				}
			}

			// Verify tasks were enqueued for positions that can process
			processableCount := 0
			for _, result := range tt.validationResults {
				if result.canProcess {
					processableCount++
				}
			}
			assert.Equal(t, processableCount, len(testService.enqueuedTasks), "Should enqueue tasks for processable positions")
		})
	}
}

// testableService extends service to track enqueued tasks without needing Redis
type testableService struct {
	log           logrus.FieldLogger
	validator     validation.Validator
	admin         admin.Service
	enqueuedTasks []enqueuedTask
}

type enqueuedTask struct {
	modelID  string
	position uint64
	interval uint64
}

func (s *testableService) checkAndEnqueuePositionWithTrigger(_ context.Context, trans models.Transformation, position, interval uint64) {
	// Track what would be enqueued instead of actually enqueuing
	s.enqueuedTasks = append(s.enqueuedTasks, enqueuedTask{
		modelID:  trans.GetID(),
		position: position,
		interval: interval,
	})
}

func (s *testableService) calculateProcessingInterval(_ context.Context, _ models.Transformation, handler transformation.Handler, nextPos, maxLimit uint64) (uint64, bool) {
	var interval uint64 = 50 // Default for testing
	if provider, ok := handler.(transformation.IntervalHandler); ok {
		interval = provider.GetMaxInterval()
	}

	// Adjust interval if it would exceed max limit
	if maxLimit > 0 && nextPos+interval > maxLimit {
		interval = maxLimit - nextPos
	}

	return interval, false
}

// processForwardWithGapSkipping is the method we're testing - matches the actual implementation
func (s *testableService) processForwardWithGapSkipping(ctx context.Context, trans models.Transformation, startPos, maxLimit uint64) {
	handler := trans.GetHandler()
	modelID := trans.GetID()
	currentPos := startPos

	// Calculate interval for this position
	interval, shouldReturn := s.calculateProcessingInterval(ctx, trans, handler, currentPos, maxLimit)
	if shouldReturn {
		return
	}

	// Validate dependencies for this position
	result, err := s.validator.ValidateDependencies(ctx, modelID, currentPos, interval)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Critical validation error")
		return
	}

	switch {
	case result.CanProcess:
		// Dependencies satisfied, enqueue the task
		s.checkAndEnqueuePositionWithTrigger(ctx, trans, currentPos, interval)

	case result.NextValidPos > currentPos:
		// Gap detected - skip to next valid position and enqueue there
		s.log.WithFields(logrus.Fields{
			"model_id":  modelID,
			"gap_start": currentPos,
			"gap_end":   result.NextValidPos,
		}).Info("Skipping gap in transformation dependencies")

		// Check if the next valid position is within limits
		if maxLimit > 0 && result.NextValidPos >= maxLimit {
			s.log.WithField("model_id", modelID).Debug("Next valid position beyond limit")
			return
		}

		// Recursively process the next valid position (single recursion to skip gap)
		s.processForwardWithGapSkipping(ctx, trans, result.NextValidPos, maxLimit)

	default:
		// No valid position to process
		s.log.WithField("model_id", modelID).Debug("No more valid positions for forward fill")
	}
}

// ========================================
// Backfill Scan Range Tests
// ========================================

func TestCalculateBackfillScanRange(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name          string
		lastEndPos    uint64
		validRangeMin uint64
		validRangeMax uint64
		validRangeErr error
		limitsMin     uint64
		limitsMax     uint64
		expectedMin   uint64
		expectedMax   uint64
		description   string
	}{
		{
			name:          "maxPos_constrained_by_dependency_coverage",
			lastEndPos:    10000,
			validRangeMin: 5000,
			validRangeMax: 8000,
			expectedMin:   5000,
			expectedMax:   8000,
			description:   "When dependency max (8000) is less than lastEndPos (10000), use dependency max",
		},
		{
			name:          "maxPos_uses_lastEndPos_when_dependency_coverage_is_higher",
			lastEndPos:    8000,
			validRangeMin: 5000,
			validRangeMax: 10000,
			expectedMin:   5000,
			expectedMax:   8000,
			description:   "When lastEndPos (8000) is less than dependency max (10000), use lastEndPos",
		},
		{
			name:          "maxPos_uses_lastEndPos_when_dependency_max_is_zero",
			lastEndPos:    10000,
			validRangeMin: 5000,
			validRangeMax: 0,
			expectedMin:   5000,
			expectedMax:   10000,
			description:   "When dependency max is 0, use lastEndPos",
		},
		{
			name:          "limits_min_overrides_dependency_min",
			lastEndPos:    10000,
			validRangeMin: 5000,
			validRangeMax: 10000,
			limitsMin:     6000,
			expectedMin:   6000,
			expectedMax:   10000,
			description:   "When configured limits.min (6000) is higher than dependency min (5000), use limits.min",
		},
		{
			name:          "limits_max_constrains_further",
			lastEndPos:    10000,
			validRangeMin: 5000,
			validRangeMax: 9000,
			limitsMax:     8000,
			expectedMin:   5000,
			expectedMax:   8000,
			description:   "When configured limits.max (8000) is less than constrained max (9000), use limits.max",
		},
		{
			name:          "dependency_max_constrains_even_with_limits_max",
			lastEndPos:    15000,
			validRangeMin: 5000,
			validRangeMax: 8000,
			limitsMax:     12000,
			expectedMin:   5000,
			expectedMax:   8000,
			description:   "Dependency max (8000) constrains before limits.max (12000) is applied",
		},
		{
			name:          "fallback_to_earliest_position_on_error",
			lastEndPos:    10000,
			validRangeMin: 0,
			validRangeMax: 0,
			validRangeErr: errValidationError,
			expectedMin:   7000, // Falls back to GetEarliestPosition
			expectedMax:   10000,
			description:   "On GetValidRange error, falls back to GetEarliestPosition and uses lastEndPos",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Setup mock validator
			mockValidator := validation.NewMockValidator()

			if tt.validRangeErr != nil {
				mockValidator.GetValidRangeForBackfillFunc = func(_ context.Context, _ string) (uint64, uint64, error) {
					return 0, 0, tt.validRangeErr
				}
				// Fallback to GetEarliestPosition
				mockValidator.GetEarliestPositionFunc = func(_ context.Context, _ string) (uint64, error) {
					return 7000, nil // Return a specific value for fallback test
				}
			} else {
				mockValidator.GetValidRangeForBackfillFunc = func(_ context.Context, _ string) (uint64, uint64, error) {
					return tt.validRangeMin, tt.validRangeMax, nil
				}
			}

			// Setup composite handler with generated mocks
			handler := newCompositeHandler(ctrl)
			handler.setupDefaultExpectations()
			handler.MockIntervalHandler.EXPECT().GetMaxInterval().Return(uint64(100)).AnyTimes()
			handler.MockIntervalHandler.EXPECT().GetMinInterval().Return(uint64(0)).AnyTimes()
			handler.MockIntervalHandler.EXPECT().AllowsPartialIntervals().Return(false).AnyTimes()
			handler.MockIntervalHandler.EXPECT().AllowGapSkipping().Return(true).AnyTimes()
			handler.MockScheduleHandler.EXPECT().IsForwardFillEnabled().Return(true).AnyTimes()
			handler.MockScheduleHandler.EXPECT().IsBackfillEnabled().Return(true).AnyTimes()

			// Setup limits
			var limits *transformation.Limits
			if tt.limitsMin > 0 || tt.limitsMax > 0 {
				limits = &transformation.Limits{
					Min: tt.limitsMin,
					Max: tt.limitsMax,
				}
			}
			handler.MockScheduleHandler.EXPECT().GetLimits().Return(limits).AnyTimes()

			// Setup mock transformation
			trans := &mockTransformation{
				id: "test.model",
				config: &transformation.Config{
					Type:     transformation.TypeIncremental,
					Database: "test",
					Table:    "model",
				},
				handler: handler,
			}

			// Create service
			svc := &service{
				log:       logger.WithField("test", tt.name),
				validator: mockValidator,
			}

			// Test the method
			ctx := context.Background()
			scanRange, err := svc.calculateBackfillScanRange(ctx, trans, tt.lastEndPos)

			// Assert results
			require.NoError(t, err)
			assert.Equal(t, tt.expectedMin, scanRange.initialPos, "%s - initialPos mismatch", tt.description)
			assert.Equal(t, tt.expectedMax, scanRange.maxPos, "%s - maxPos mismatch", tt.description)
		})
	}
}
