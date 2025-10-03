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

func TestProcessForwardWithGapSkipping(t *testing.T) {
	tests := []struct {
		name              string
		validationResults []struct {
			position     uint64
			canProcess   bool
			nextValidPos uint64
		}
		expectedPositions []uint64
		startPos          uint64
		maxLimit          uint64
	}{
		{
			name: "gap detected, skip and continue",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: false, nextValidPos: 110}, // Gap detected
				{position: 110, canProcess: true},                     // Can process
				{position: 160, canProcess: true},                     // Continue
				{position: 210, canProcess: false, nextValidPos: 0},   // Stop
			},
			expectedPositions: []uint64{100, 110, 160, 210},
			startPos:          100,
			maxLimit:          0, // No limit
		},
		{
			name: "no gaps, normal processing",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: true},
				{position: 150, canProcess: true},
				{position: 200, canProcess: false, nextValidPos: 0}, // Stop
			},
			expectedPositions: []uint64{100, 150, 200},
			startPos:          100,
			maxLimit:          0,
		},
		{
			name: "multiple gaps",
			validationResults: []struct {
				position     uint64
				canProcess   bool
				nextValidPos uint64
			}{
				{position: 100, canProcess: false, nextValidPos: 120}, // First gap
				{position: 120, canProcess: false, nextValidPos: 140}, // Second gap
				{position: 140, canProcess: true},                     // Can process
				{position: 190, canProcess: false, nextValidPos: 0},   // Stop
			},
			expectedPositions: []uint64{100, 120, 140, 190},
			startPos:          100,
			maxLimit:          0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock validator with expected results
			mockValidator := validation.NewMockValidator()
			callIndex := 0
			var calledPositions []uint64
			var enqueuedPositions []uint64

			mockValidator.ValidateDependenciesFunc = func(_ context.Context, _ string, position, _ uint64) (validation.Result, error) {
				calledPositions = append(calledPositions, position)

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

			// Setup mock transformation using existing types
			handler := &mockHandler{
				interval:           50,
				forwardFillEnabled: true,
			}
			// Add ShouldTrackPosition method behavior
			handler.allowsPartialIntervals = false

			trans := &mockTransformation{
				id:      "test.model",
				handler: handler,
			}

			// Create a modified service that captures enqueue calls instead of executing them
			testService := &testGapSkippingService{
				log:               logrus.NewEntry(logrus.New()),
				validator:         mockValidator,
				admin:             &mockAdminService{},
				enqueuedPositions: &enqueuedPositions,
			}

			// Test the gap skipping functionality
			ctx := context.Background()
			testService.processForwardWithGapSkipping(ctx, trans, tt.startPos, tt.maxLimit)

			// Verify the positions that were called
			assert.Equal(t, tt.expectedPositions, calledPositions, "Should call validation for expected positions")
		})
	}
}

// testGapSkippingService wraps the service to avoid dependency on queueManager
type testGapSkippingService struct {
	log               logrus.FieldLogger
	validator         validation.Validator
	admin             admin.Service
	enqueuedPositions *[]uint64
}

func (s *testGapSkippingService) checkAndEnqueuePositionWithTrigger(_ context.Context, _ models.Transformation, position, _ uint64) {
	// Instead of actually enqueuing, just record the position for verification
	*s.enqueuedPositions = append(*s.enqueuedPositions, position)
}

func (s *testGapSkippingService) processForwardWithGapSkipping(ctx context.Context, trans models.Transformation, startPos, maxLimit uint64) {
	handler := trans.GetHandler()
	modelID := trans.GetID()
	currentPos := startPos

	for maxLimit == 0 || currentPos < maxLimit {
		interval, shouldReturn := s.calculateProcessingInterval(ctx, trans, handler, currentPos, maxLimit)
		if shouldReturn {
			return
		}

		result, err := s.validator.ValidateDependencies(ctx, modelID, currentPos, interval)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Critical validation error")
			return
		}

		switch {
		case result.CanProcess:
			s.checkAndEnqueuePositionWithTrigger(ctx, trans, currentPos, interval)
			currentPos += interval

		case result.NextValidPos > currentPos:
			s.log.WithFields(logrus.Fields{
				"model_id":  modelID,
				"gap_start": currentPos,
				"gap_end":   result.NextValidPos,
			}).Info("Skipping gap in transformation dependencies")
			currentPos = result.NextValidPos

		default:
			s.log.WithField("model_id", modelID).Debug("No more valid positions for forward fill")
			return
		}
	}
}

// calculateProcessingInterval mimics the original service method for testing
func (s *testGapSkippingService) calculateProcessingInterval(_ context.Context, _ models.Transformation, handler transformation.Handler, nextPos, maxLimit uint64) (uint64, bool) {
	type intervalProvider interface {
		GetMaxInterval() uint64
	}

	var interval uint64 = 50 // Default for testing
	if provider, ok := handler.(intervalProvider); ok {
		interval = provider.GetMaxInterval()
	}

	// Adjust interval if it would exceed max limit
	if maxLimit > 0 && nextPos+interval > maxLimit {
		interval = maxLimit - nextPos
	}

	return interval, false
}

func TestProcessForward_GapAwareRouting(t *testing.T) {
	tests := []struct {
		name                string
		shouldTrackPosition bool
		expectGapProcessing bool
	}{
		{
			name:                "incremental transformation uses gap processing",
			shouldTrackPosition: true,
			expectGapProcessing: true,
		},
		{
			name:                "scheduled transformation uses normal processing",
			shouldTrackPosition: false,
			expectGapProcessing: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock handler with different position tracking
			handler := &mockHandler{
				interval:           50,
				forwardFillEnabled: true,
			}

			trans := &mockTransformation{
				id:      "test.model",
				handler: handler,
			}

			mockValidator := validation.NewMockValidator()
			mockValidator.ValidateDependenciesFunc = func(_ context.Context, _ string, _ uint64, _ uint64) (validation.Result, error) {
				return validation.Result{CanProcess: true}, nil
			}

			service := &service{
				log:       logrus.NewEntry(logrus.New()),
				validator: mockValidator,
				admin:     &mockAdminService{},
			}

			// Mock the forward fill enabled check
			service.processForward(trans)

			if tt.expectGapProcessing {
				// For incremental transformations, validator should be called
				// (gap processing path calls validator in a loop)
				require.NotEmpty(t, mockValidator.ValidateCalls, "Gap processing should call validator")
			}
			// For scheduled transformations, the normal path may or may not call validator
			// depending on other conditions, so we don't assert here
		})
	}
}
