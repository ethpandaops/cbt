package coordinator

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
)

// processForward handles forward fill processing for transformations
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

	// Use gap-aware processing for incremental transformations
	if handler.ShouldTrackPosition() {
		s.processForwardWithGapSkipping(ctx, trans, nextPos, maxLimit)
	} else {
		interval, shouldReturn := s.calculateProcessingInterval(ctx, trans, handler, nextPos, maxLimit)
		if shouldReturn {
			return
		}
		s.checkAndEnqueuePositionWithTrigger(ctx, trans, nextPos, interval)
	}
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

// calculateProcessingInterval determines the interval to use for processing
func (s *service) calculateProcessingInterval(ctx context.Context, trans models.Transformation, handler transformation.Handler, nextPos, maxLimit uint64) (uint64, bool) {
	type intervalProvider interface {
		GetMaxInterval() uint64
		AllowsPartialIntervals() bool
	}

	var interval uint64
	var allowsPartial bool
	if provider, ok := handler.(intervalProvider); ok {
		interval = provider.GetMaxInterval()
		allowsPartial = provider.AllowsPartialIntervals()
	}

	// Adjust interval if it would exceed max limit
	if maxLimit > 0 && nextPos+interval > maxLimit {
		originalInterval := interval
		interval = maxLimit - nextPos
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"position":          nextPos,
			"original_interval": originalInterval,
			"adjusted_interval": interval,
			"limits_max":        maxLimit,
		}).Debug("Adjusted interval to respect max limit")
	}

	// Check for partial interval processing based on dependency availability
	if allowsPartial {
		adjustedInterval, shouldReturn := s.adjustIntervalForDependencies(ctx, trans, nextPos, interval)
		if shouldReturn {
			return 0, true
		}
		interval = adjustedInterval
	}

	return interval, false
}

// adjustIntervalForDependencies adjusts the interval based on dependency availability
// Returns the adjusted interval and whether processing should stop
func (s *service) adjustIntervalForDependencies(ctx context.Context, trans models.Transformation, nextPos, interval uint64) (uint64, bool) {
	// Get the valid range based on dependencies
	_, maxValid, err := s.validator.GetValidRange(ctx, trans.GetID())
	if err != nil || maxValid == 0 || nextPos >= maxValid {
		// Can't determine valid range or position already beyond range
		return interval, false
	}

	// Check if dependencies limit our interval
	availableInterval := maxValid - nextPos
	if availableInterval >= interval {
		// Full interval is available
		return interval, false
	}

	// Dependencies don't have enough data for full interval
	type minIntervalProvider interface {
		GetMinInterval() uint64
	}
	var minInterval uint64
	if provider, ok := trans.GetHandler().(minIntervalProvider); ok {
		minInterval = provider.GetMinInterval()
	}

	// Check if the available interval meets minimum requirements
	if availableInterval < minInterval {
		// Available interval is too small
		s.log.WithFields(logrus.Fields{
			"model_id":           trans.GetID(),
			"position":           nextPos,
			"available_interval": availableInterval,
			"min_interval":       minInterval,
		}).Debug("Available interval below minimum interval, waiting for more data")
		return interval, true // Signal to return/stop processing
	}

	// Use partial interval
	originalInterval := interval
	s.log.WithFields(logrus.Fields{
		"model_id":          trans.GetID(),
		"position":          nextPos,
		"original_interval": originalInterval,
		"adjusted_interval": availableInterval,
		"dependency_limit":  maxValid,
		"reason":            "dependency_availability",
	}).Info("Using partial interval due to dependency limits")

	return availableInterval, false
}

// processForwardWithGapSkipping processes forward fill with gap detection and skipping
func (s *service) processForwardWithGapSkipping(ctx context.Context, trans models.Transformation, startPos, maxLimit uint64) {
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
