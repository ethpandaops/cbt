package coordinator

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockValidatorWithGaps provides controlled validation results for testing gap skipping
type mockValidatorWithGaps struct {
	results []validation.Result
	calls   []validationCall
	index   int
}

type validationCall struct {
	modelID  string
	position uint64
	interval uint64
}

func (m *mockValidatorWithGaps) ValidateDependencies(_ context.Context, modelID string, position, interval uint64) (validation.Result, error) {
	// Track the call
	m.calls = append(m.calls, validationCall{
		modelID:  modelID,
		position: position,
		interval: interval,
	})

	// Return the next pre-configured result
	if m.index < len(m.results) {
		result := m.results[m.index]
		m.index++
		return result, nil
	}

	// Default to no more data
	return validation.Result{CanProcess: false}, nil
}

func (m *mockValidatorWithGaps) GetInitialPosition(_ context.Context, _ string) (uint64, error) {
	return 100, nil
}

func (m *mockValidatorWithGaps) GetEarliestPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockValidatorWithGaps) GetValidRange(_ context.Context, _ string) (minPos, maxPos uint64, err error) {
	return 0, 1000, nil
}

// mockTransformationForGaps implements models.Transformation for testing
type mockTransformationForGaps struct {
	id     string
	config *transformation.Config
}

func (m *mockTransformationForGaps) GetID() string {
	return m.id
}

func (m *mockTransformationForGaps) GetConfig() *transformation.Config {
	return m.config
}

func (m *mockTransformationForGaps) GetConfigMutable() *transformation.Config {
	return m.config
}

func (m *mockTransformationForGaps) GetType() string {
	return "sql"
}

func (m *mockTransformationForGaps) GetValue() string {
	return ""
}

func (m *mockTransformationForGaps) SetDefaultDatabase(_ string) {
	// No-op for testing
}

func TestProcessForwardWithGapSkipping(t *testing.T) {
	tests := []struct {
		name              string
		startPos          uint64
		config            *transformation.Config
		validationResults []validation.Result
		expectedCalls     []validationCall
		description       string
	}{
		{
			name:     "no gaps - continuous processing",
			startPos: 100,
			config: &transformation.Config{
				Interval: &transformation.IntervalConfig{
					Max: 50,
				},
				Limits: &transformation.LimitsConfig{
					Max: 300,
				},
			},
			validationResults: []validation.Result{
				{CanProcess: true},  // Process 100-150
				{CanProcess: true},  // Process 150-200
				{CanProcess: true},  // Process 200-250
				{CanProcess: true},  // Process 250-300
				{CanProcess: false}, // Stop at limit
			},
			expectedCalls: []validationCall{
				{modelID: "test.model", position: 100, interval: 50},
				{modelID: "test.model", position: 150, interval: 50},
				{modelID: "test.model", position: 200, interval: 50},
				{modelID: "test.model", position: 250, interval: 50},
			},
			description: "Should process continuously when no gaps",
		},
		{
			name:     "gap at 101-109 - skip to 110",
			startPos: 100,
			config: &transformation.Config{
				Interval: &transformation.IntervalConfig{
					Max: 50,
				},
				Limits: &transformation.LimitsConfig{
					Max: 300,
				},
			},
			validationResults: []validation.Result{
				{CanProcess: false, NextValidPos: 110}, // Gap detected at 100, skip to 110
				{CanProcess: true},                     // Process 110-160
				{CanProcess: true},                     // Process 160-210
				{CanProcess: true},                     // Process 210-260
				{CanProcess: true},                     // Process 260-300 (adjusted interval)
				{CanProcess: false},                    // Stop
			},
			expectedCalls: []validationCall{
				{modelID: "test.model", position: 100, interval: 50}, // Gap detected
				{modelID: "test.model", position: 110, interval: 50}, // After gap
				{modelID: "test.model", position: 160, interval: 50},
				{modelID: "test.model", position: 210, interval: 50},
				{modelID: "test.model", position: 260, interval: 40}, // Adjusted for limit
			},
			description: "Should skip gap and continue from next valid position",
		},
		{
			name:     "multiple gaps - skip each one",
			startPos: 100,
			config: &transformation.Config{
				Interval: &transformation.IntervalConfig{
					Max: 50,
				},
				Limits: &transformation.LimitsConfig{
					Max: 400,
				},
			},
			validationResults: []validation.Result{
				{CanProcess: false, NextValidPos: 110}, // Gap 1: 100-109
				{CanProcess: true},                     // Process 110-160
				{CanProcess: false, NextValidPos: 200}, // Gap 2: 160-199
				{CanProcess: true},                     // Process 200-250
				{CanProcess: false, NextValidPos: 300}, // Gap 3: 250-299
				{CanProcess: true},                     // Process 300-350
				{CanProcess: true},                     // Process 350-400
				{CanProcess: false},                    // Stop at limit
			},
			expectedCalls: []validationCall{
				{modelID: "test.model", position: 100, interval: 50},
				{modelID: "test.model", position: 110, interval: 50},
				{modelID: "test.model", position: 160, interval: 50},
				{modelID: "test.model", position: 200, interval: 50},
				{modelID: "test.model", position: 250, interval: 50},
				{modelID: "test.model", position: 300, interval: 50},
				{modelID: "test.model", position: 350, interval: 50},
			},
			description: "Should skip multiple gaps and continue processing",
		},
		{
			name:     "no more data after gap",
			startPos: 100,
			config: &transformation.Config{
				Interval: &transformation.IntervalConfig{
					Max: 50,
				},
				Limits: &transformation.LimitsConfig{
					Max: 300,
				},
			},
			validationResults: []validation.Result{
				{CanProcess: true},                   // Process 100-150
				{CanProcess: false, NextValidPos: 0}, // No more data (NextValidPos = 0)
			},
			expectedCalls: []validationCall{
				{modelID: "test.model", position: 100, interval: 50},
				{modelID: "test.model", position: 150, interval: 50},
			},
			description: "Should stop when NextValidPos is 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock components
			mockValidator := &mockValidatorWithGaps{
				results: tt.validationResults,
				calls:   []validationCall{},
			}

			mockAdmin := &mockAdminServiceForGaps{
				nextPositions: map[string]uint64{"test.model": tt.startPos},
			}

			// Create service - queueManager can be nil since we're only testing validation
			// The checkAndEnqueuePositionWithTrigger will be called but won't actually enqueue
			s := &service{
				log:          logrus.NewEntry(logrus.New()),
				validator:    mockValidator,
				admin:        mockAdmin,
				queueManager: nil, // nil is OK - service checks for nil
				done:         make(chan struct{}),
				taskCheck:    make(chan taskOperation),
				taskMark:     make(chan string, 100),
			}

			// Create transformation
			trans := &mockTransformationForGaps{
				id:     "test.model",
				config: tt.config,
			}

			// Execute
			ctx := context.Background()
			s.processForwardWithGapSkipping(ctx, models.Transformation(trans), tt.startPos)

			// Verify validation calls
			require.Len(t, mockValidator.calls, len(tt.expectedCalls), tt.description)
			for i, expected := range tt.expectedCalls {
				assert.Equal(t, expected.modelID, mockValidator.calls[i].modelID, "Call %d: modelID mismatch", i)
				assert.Equal(t, expected.position, mockValidator.calls[i].position, "Call %d: position mismatch", i)
				assert.Equal(t, expected.interval, mockValidator.calls[i].interval, "Call %d: interval mismatch", i)
			}
		})
	}
}

// TestGapSkippingScenario tests the specific scenario from the plan
func TestGapSkippingScenario(t *testing.T) {
	// This tests the exact scenario:
	// - TableA (transformation) has data at [0-100] and [110-150] with gap [101-109]
	// - TableB depends on TableA and is at position 100
	// - TableB should skip to position 110 and continue processing

	mockValidator := &mockValidatorWithGaps{
		results: []validation.Result{
			// Position 100: Gap detected at 101-109
			{CanProcess: false, NextValidPos: 110},
			// Position 110: Can process
			{CanProcess: true},
			// Position 160: No more data from TableA (max is 150)
			{CanProcess: false, NextValidPos: 0},
		},
		calls: []validationCall{},
	}

	mockAdmin := &mockAdminServiceForGaps{
		nextPositions: map[string]uint64{"table.b": 100},
	}

	s := &service{
		log:       logrus.NewEntry(logrus.New()),
		validator: mockValidator,
		admin:     mockAdmin,
		done:      make(chan struct{}),
	}

	trans := &mockTransformationForGaps{
		id: "table.b",
		config: &transformation.Config{
			Interval: &transformation.IntervalConfig{
				Max: 50,
			},
		},
	}

	ctx := context.Background()
	s.processForwardWithGapSkipping(ctx, models.Transformation(trans), 100)

	// Verify the sequence of validation calls
	require.Len(t, mockValidator.calls, 3, "Should make 3 validation calls")

	// First call: detect gap at position 100
	assert.Equal(t, "table.b", mockValidator.calls[0].modelID)
	assert.Equal(t, uint64(100), mockValidator.calls[0].position)
	assert.Equal(t, uint64(50), mockValidator.calls[0].interval)

	// Second call: process from position 110 (after gap)
	assert.Equal(t, "table.b", mockValidator.calls[1].modelID)
	assert.Equal(t, uint64(110), mockValidator.calls[1].position)
	assert.Equal(t, uint64(50), mockValidator.calls[1].interval)

	// Third call: check position 160 (110 + 50)
	assert.Equal(t, "table.b", mockValidator.calls[2].modelID)
	assert.Equal(t, uint64(160), mockValidator.calls[2].position)
	assert.Equal(t, uint64(50), mockValidator.calls[2].interval)
}

// mockAdminServiceForGaps for testing gap scenarios
type mockAdminServiceForGaps struct {
	nextPositions map[string]uint64
}

func (m *mockAdminServiceForGaps) GetNextUnprocessedPosition(_ context.Context, modelID string) (uint64, error) {
	if pos, ok := m.nextPositions[modelID]; ok {
		return pos, nil
	}
	return 0, nil
}

func (m *mockAdminServiceForGaps) GetLastProcessedEndPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminServiceForGaps) GetLastProcessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminServiceForGaps) GetFirstPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminServiceForGaps) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return nil
}

func (m *mockAdminServiceForGaps) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return false, nil
}

func (m *mockAdminServiceForGaps) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return nil, nil
}

func (m *mockAdminServiceForGaps) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *mockAdminServiceForGaps) GetExternalBounds(_ context.Context, _ string) (*admin.BoundsCache, error) {
	return nil, nil
}

func (m *mockAdminServiceForGaps) SetExternalBounds(_ context.Context, _ *admin.BoundsCache) error {
	return nil
}

func (m *mockAdminServiceForGaps) GetAdminDatabase() string {
	return "test_db"
}

func (m *mockAdminServiceForGaps) GetAdminTable() string {
	return "test_table"
}
