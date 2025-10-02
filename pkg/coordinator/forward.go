package coordinator

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
)

// processForward handles forward fill processing for transformations with gap-aware capabilities
func (s *service) processForward(trans models.Transformation) {
	handler := trans.GetHandler()

	// Skip if no forward fill schedule configured
	if !s.isForwardFillEnabled(handler) {
		return
	}

	ctx := context.Background()

	// Get the starting position for processing
	nextPos, err := s.getProcessingPosition(ctx, trans)
	if err != nil {
		return
	}

	// Check if position exceeds configured limits
	maxLimit := s.getMaxLimit(handler)
	if s.isPositionBeyondLimit(trans.GetID(), nextPos, maxLimit) {
		return
	}

	// Use gap-aware forward processing for enhanced dependency handling
	s.processForwardWithGapSkipping(ctx, trans, nextPos)
}

// isForwardFillEnabled checks if forward fill is enabled for the handler
func (s *service) isForwardFillEnabled(handler transformation.Handler) bool {
	type scheduleProvider interface {
		IsForwardFillEnabled() bool
	}
	provider, ok := handler.(scheduleProvider)
	return ok && provider.IsForwardFillEnabled()
}

// getProcessingPosition gets the next position to process, calculating initial if needed
func (s *service) getProcessingPosition(ctx context.Context, trans models.Transformation) (uint64, error) {
	nextPos, err := s.admin.GetNextUnprocessedPosition(ctx, trans.GetID())
	if err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Error("Failed to get next unprocessed position")
		return 0, err
	}

	s.log.WithFields(logrus.Fields{
		"model_id": trans.GetID(),
		"next_pos": nextPos,
	}).Debug("Got next unprocessed position for forward fill")

	// If this is the first run, calculate initial position
	if nextPos == 0 {
		s.log.WithField("model_id", trans.GetID()).Debug("No data processed yet, calculating initial position")

		initialPos, err := s.validator.GetInitialPosition(ctx, trans.GetID())
		if err != nil {
			s.log.WithError(err).WithField("model_id", trans.GetID()).Error("Failed to calculate initial position")
			return 0, err
		}

		s.log.WithFields(logrus.Fields{
			"model_id":    trans.GetID(),
			"initial_pos": initialPos,
		}).Debug("Calculated initial position")

		nextPos = initialPos
	}

	return nextPos, nil
}

// getMaxLimit extracts the max limit from the handler
func (s *service) getMaxLimit(handler transformation.Handler) uint64 {
	type limitsProvider interface {
		GetLimits() *struct{ Min, Max uint64 }
	}
	if provider, ok := handler.(limitsProvider); ok {
		if limits := provider.GetLimits(); limits != nil && limits.Max > 0 {
			return limits.Max
		}
	}
	return 0
}

// isPositionBeyondLimit checks if the position exceeds the configured limit
func (s *service) isPositionBeyondLimit(modelID string, position, maxLimit uint64) bool {
	if maxLimit > 0 && position >= maxLimit {
		s.log.WithFields(logrus.Fields{
			"model_id":   modelID,
			"position":   position,
			"limits_max": maxLimit,
		}).Debug("Position is at or beyond max limit, skipping forward processing")
		return true
	}
	return false
}

// processForwardWithGapSkipping performs forward fill processing with the ability to skip
// over gaps in transformation dependencies. When a gap is detected, it jumps to the next
// position where all dependencies have data available, allowing forward fill to continue
// rather than stopping at the first gap.
func (s *service) processForwardWithGapSkipping(
	ctx context.Context,
	trans models.Transformation,
	startPos uint64,
) {
	var (
		handler    = trans.GetHandler()
		modelID    = trans.GetID()
		currentPos = startPos
	)

	// Get max limit and interval from handler
	maxLimit := s.getMaxLimit(handler)

	// Get max interval
	var maxInterval uint64
	if intervalProvider, ok := handler.(interface{ GetMaxInterval() uint64 }); ok {
		maxInterval = intervalProvider.GetMaxInterval()
	}

	// Process forward until we reach configured limits or run out of dependency data
	for maxLimit == 0 || currentPos < maxLimit {
		// Calculate processing interval, adjusting if it would exceed configured limits
		interval := maxInterval
		if maxLimit > 0 {
			if currentPos+interval > maxLimit {
				interval = maxLimit - currentPos
			}
		}

		// Check if dependencies have data for this range
		result, err := s.validator.ValidateDependencies(ctx, modelID, currentPos, interval)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Critical validation error")
			return
		}

		switch {
		case result.CanProcess:
			// All dependencies have data for this range - enqueue for processing
			s.checkAndEnqueuePositionWithTrigger(ctx, trans, currentPos, interval)
			currentPos += interval

		case result.NextValidPos > currentPos:
			// Gap detected in dependencies - skip to the next position with data
			s.log.WithFields(logrus.Fields{
				"model_id":  modelID,
				"gap_start": currentPos,
				"gap_end":   result.NextValidPos,
			}).Info("Skipping gap in transformation dependencies")
			currentPos = result.NextValidPos

		default:
			// No more valid positions found (reached end of dependency data)
			s.log.WithFields(logrus.Fields{
				"model_id":      modelID,
				"last_position": currentPos,
			}).Debug("No more valid positions found - reached end of dependency data")
			return
		}
	}
}
