package coordinator

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
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

	// Route to gap-aware processing for incremental transformations
	// which process sequential positions (0, 1, 2, ...) and can have gaps
	// in their dependency data that need to be skipped. Scheduled
	// transformations run on time schedules (hourly, daily) and don't have
	// position-based gaps, so they use the normal processing path.
	if handler.ShouldTrackPosition() && s.shouldAllowGapSkipping(handler) {
		// Gap-aware path: detect and skip gaps in dependencies
		s.processForwardWithGapSkipping(ctx, trans, nextPos, maxLimit)
	} else {
		// Normal path: for scheduled transformations or incremental with gap skipping disabled
		interval, shouldReturn := s.calculateProcessingInterval(ctx, trans, handler, nextPos, maxLimit)
		if shouldReturn {
			return
		}

		s.checkAndEnqueuePositionWithTrigger(ctx, trans, nextPos, interval, string(DirectionForward))
	}
}

// isForwardFillEnabled checks if forward fill is enabled for the handler
func (s *service) isForwardFillEnabled(handler transformation.Handler) bool {
	provider, ok := handler.(transformation.ScheduleHandler)
	return ok && provider.IsForwardFillEnabled()
}

// shouldAllowGapSkipping checks if gap skipping is allowed for the handler
func (s *service) shouldAllowGapSkipping(handler transformation.Handler) bool {
	provider, ok := handler.(transformation.IntervalHandler)
	if !ok {
		return true // default: allow gap skipping
	}
	return provider.AllowGapSkipping()
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

		initialPos, err := s.validator.GetStartPosition(ctx, trans.GetID())
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
	if provider, ok := handler.(transformation.ScheduleHandler); ok {
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
	var interval uint64

	var allowsPartial bool

	if provider, ok := handler.(transformation.IntervalHandler); ok {
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
	_, maxValid, err := s.validator.GetValidRange(ctx, trans.GetID(), validation.Union)
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
	var minInterval uint64
	if provider, ok := trans.GetHandler().(transformation.IntervalHandler); ok {
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

// processForwardWithGapSkipping processes forward fill with gap detection and skipping.
//
// This function processes ONE position per call.
// The coordinator calls processForward periodically, which then calls this function.
//
// Gap Skipping Logic:
// 1. Validate dependencies at the current position
// 2. If dependencies are satisfied -> enqueue the task
// 3. If a gap is detected -> recursively skip to the next valid position
// 4. This ensures we don't waste resources trying to process positions with missing data
//
// Example Flow:
//
//	Position 100: Gap detected in dependency, NextValidPos=110
//	-> Recursively calls with position 110
//	Position 110: Dependencies satisfied
//	-> Enqueues task for position 110
func (s *service) processForwardWithGapSkipping(
	ctx context.Context,
	trans models.Transformation,
	startPos, maxLimit uint64,
) {
	var (
		handler    = trans.GetHandler()
		modelID    = trans.GetID()
		currentPos = startPos
	)

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
		// Dependencies are satisfied at this position
		// Enqueue the transformation task for processing
		s.checkAndEnqueuePositionWithTrigger(ctx, trans, currentPos, interval, string(DirectionForward))

	case result.NextValidPos > currentPos:
		// Dependencies have missing data in range [currentPos, currentPos+interval]
		// NextValidPos indicates where the dependency data resumes
		// Example: Gap [101-109] means we can't process 100, but can process 110
		s.log.WithFields(logrus.Fields{
			"model_id":  modelID,
			"gap_start": currentPos,
			"gap_end":   result.NextValidPos,
		}).Info("Skipping gap in transformation dependencies")

		// Safety check: Don't skip beyond configured limits
		if maxLimit > 0 && result.NextValidPos >= maxLimit {
			s.log.WithField("model_id", modelID).Debug("Next valid position beyond limit")
			return
		}

		// Skip to the next valid position and try again
		s.processForwardWithGapSkipping(ctx, trans, result.NextValidPos, maxLimit)

	default:
		// Dependencies don't have any more data available
		// This is normal when we've caught up with real-time data
		s.log.WithField("model_id", modelID).Debug("No more valid positions for forward fill")
	}
}
