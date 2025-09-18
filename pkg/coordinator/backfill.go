package coordinator

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
)

// backfillScanRange holds the range for backfill gap scanning
type backfillScanRange struct {
	initialPos uint64
	maxPos     uint64
}

// processBack handles backfill processing for transformations
func (s *service) processBack(trans models.Transformation) {
	handler := trans.GetHandler()
	if handler == nil {
		return
	}

	type scheduleProvider interface {
		IsBackfillEnabled() bool
	}
	if provider, ok := handler.(scheduleProvider); !ok || !provider.IsBackfillEnabled() {
		return
	}

	ctx := context.Background()
	s.checkBackfillOpportunities(ctx, trans)
}

// checkBackfillOpportunities scans for and processes gaps in transformation data
func (s *service) checkBackfillOpportunities(ctx context.Context, trans models.Transformation) {
	// Get max interval from handler
	type intervalProvider interface {
		GetMaxInterval() uint64
	}
	var maxInterval uint64
	if provider, ok := trans.GetHandler().(intervalProvider); ok {
		maxInterval = provider.GetMaxInterval()
	}

	// Get last processed positions
	lastPos, lastEndPos, hasData := s.getBackfillBounds(ctx, trans.GetID())
	if !hasData {
		return
	}

	// Log the last processed range
	if lastEndPos > 0 {
		s.log.WithFields(logrus.Fields{
			"model_id":             trans.GetID(),
			"last_processed_start": lastPos,
			"last_processed_end":   lastEndPos,
			"backfill_interval":    maxInterval,
		}).Debug("Starting gap scan - found existing processed data")
	} else {
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"backfill_interval": maxInterval,
		}).Debug("Starting gap scan - no existing data")
	}

	// Check if we have enough data to scan
	if lastEndPos < maxInterval {
		s.log.WithField("model_id", trans.GetID()).Debug("No data yet, skipping gap scan")
		return
	}

	// Calculate scan range
	scanRange, err := s.calculateBackfillScanRange(ctx, trans, lastEndPos)
	if err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Debug("Failed to calculate scan range")
		return
	}

	// Find all gaps in the processed data
	s.log.WithFields(logrus.Fields{
		"model_id":          trans.GetID(),
		"scan_min_pos":      scanRange.initialPos,
		"scan_max_pos":      scanRange.maxPos,
		"backfill_interval": maxInterval,
	}).Debug("Scanning for gaps in processed data")

	gaps, err := s.admin.FindGaps(ctx, trans.GetID(), scanRange.initialPos, scanRange.maxPos, maxInterval)
	if err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Error("Failed to find gaps")
		return
	}

	// Check if we found any gaps
	if len(gaps) == 0 {
		s.log.WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"min_pos":  scanRange.initialPos,
			"max_pos":  scanRange.maxPos,
		}).Debug("No gaps found in processed data")
		return
	}

	// Log gap summary
	var interval uint64
	if handler := trans.GetHandler(); handler != nil {
		type intervalProvider interface {
			GetMaxInterval() uint64
		}
		if provider, ok := handler.(intervalProvider); ok {
			interval = provider.GetMaxInterval()
		}
	}
	s.log.WithFields(logrus.Fields{
		"model_id":  trans.GetID(),
		"gap_count": len(gaps),
		"min_pos":   scanRange.initialPos,
		"max_pos":   scanRange.maxPos,
		"interval":  interval,
	}).Info("Found gaps in processed data")

	// Process gaps - queue only one task per scan
	for i, gap := range gaps {
		if s.processSingleGap(ctx, trans, gap, i) {
			// Only queue one task per gap scan
			s.log.WithFields(logrus.Fields{
				"model_id":       trans.GetID(),
				"remaining_gaps": len(gaps) - i - 1,
			}).Debug("Enqueued one backfill task, will re-scan for more gaps after completion")
			break
		}
	}
}

// getBackfillBounds retrieves the last processed positions for backfill scanning
func (s *service) getBackfillBounds(ctx context.Context, modelID string) (lastPos, lastEndPos uint64, hasData bool) {
	lastEndPos, err := s.admin.GetLastProcessedEndPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to get last processed end position for gap scan")
		return 0, 0, false
	}

	// Also get the actual last position for clearer logging
	lastPos, _ = s.admin.GetLastProcessedPosition(ctx, modelID)

	return lastPos, lastEndPos, true
}

// calculateBackfillScanRange determines the range to scan for gaps
func (s *service) calculateBackfillScanRange(ctx context.Context, trans models.Transformation, lastEndPos uint64) (*backfillScanRange, error) {
	// Get initial position based on dependencies
	initialPos, err := s.validator.GetEarliestPosition(ctx, trans.GetID())
	if err != nil {
		return nil, err
	}

	s.log.WithFields(logrus.Fields{
		"model_id":               trans.GetID(),
		"calculated_initial_pos": initialPos,
		"based_on":               "dependency analysis",
	}).Debug("Calculated earliest position from dependencies")

	// Apply minimum limit if configured
	var maxPos uint64
	if handler := trans.GetHandler(); handler != nil {
		initialPos = s.applyMinimumLimit(trans.GetID(), handler, initialPos)
		// Apply maximum limit if configured
		maxPos = s.applyMaximumLimit(trans.GetID(), handler, lastEndPos)
	} else {
		maxPos = lastEndPos
	}

	return &backfillScanRange{
		initialPos: initialPos,
		maxPos:     maxPos,
	}, nil
}

// applyMinimumLimit applies the configured minimum limit to the initial position
func (s *service) applyMinimumLimit(modelID string, handler transformation.Handler, initialPos uint64) uint64 {
	type limitsProvider interface {
		GetLimits() *struct{ Min, Max uint64 }
	}

	var minLimit uint64
	if provider, ok := handler.(limitsProvider); ok {
		if limits := provider.GetLimits(); limits != nil && limits.Min > 0 {
			minLimit = limits.Min
		}
	}

	if minLimit > initialPos {
		s.log.WithFields(logrus.Fields{
			"model_id":           modelID,
			"initial_pos_before": initialPos,
			"initial_pos_after":  minLimit,
			"limits_min":         minLimit,
			"reason":             "using configured minimum",
		}).Debug("Adjusted initial position based on configured limits")
		return minLimit
	}

	// Build log fields based on whether limits are configured
	logFields := logrus.Fields{
		"model_id":    modelID,
		"initial_pos": initialPos,
	}

	if minLimit > 0 {
		logFields["limits_min"] = minLimit
		logFields["reason"] = "calculated position is higher than limit"
	} else {
		logFields["limits_min"] = "not configured"
		logFields["reason"] = "no limits configured"
	}

	s.log.WithFields(logFields).Debug("Using calculated initial position for gap scanning")
	return initialPos
}

// applyMaximumLimit applies the configured maximum limit to the scan range
func (s *service) applyMaximumLimit(modelID string, handler transformation.Handler, lastEndPos uint64) uint64 {
	type limitsProvider interface {
		GetLimits() *struct{ Min, Max uint64 }
	}

	var maxLimit uint64
	if provider, ok := handler.(limitsProvider); ok {
		if limits := provider.GetLimits(); limits != nil && limits.Max > 0 {
			maxLimit = limits.Max
		}
	}

	if maxLimit > 0 && maxLimit < lastEndPos {
		s.log.WithFields(logrus.Fields{
			"model_id":               modelID,
			"last_processed_end_pos": lastEndPos,
			"limits_max":             maxLimit,
		}).Debug("Applying maximum position limit for gap scanning")
		return maxLimit
	}
	return lastEndPos
}

// processSingleGap processes a single gap for backfill
func (s *service) processSingleGap(ctx context.Context, trans models.Transformation, gap admin.GapInfo, gapIndex int) bool {
	gapSize := gap.EndPos - gap.StartPos

	// Get max interval from handler
	type intervalProvider interface {
		GetMaxInterval() uint64
	}
	var maxInterval uint64
	if provider, ok := trans.GetHandler().(intervalProvider); ok {
		maxInterval = provider.GetMaxInterval()
	}
	intervalToUse := maxInterval

	// Adjust interval for small gaps
	if gapSize < maxInterval {
		intervalToUse = gapSize
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"gap_index":         gapIndex,
			"gap_size":          gapSize,
			"model_interval":    maxInterval,
			"adjusted_interval": intervalToUse,
		}).Debug("Adjusted interval for small gap")
	}

	// Calculate position (work backwards from gap end)
	pos := gap.EndPos - intervalToUse

	s.log.WithFields(logrus.Fields{
		"model_id":                 trans.GetID(),
		"gap_index":                gapIndex,
		"gap_start":                gap.StartPos,
		"gap_end":                  gap.EndPos,
		"gap_size":                 gapSize,
		"backfill_position":        pos,
		"backfill_interval":        intervalToUse,
		"will_process_range_start": pos,
		"will_process_range_end":   pos + intervalToUse,
	}).Debug("Processing gap for backfill")

	// Check if task is already pending
	payload := tasks.IncrementalTaskPayload{
		ModelID:  trans.GetID(),
		Position: pos,
		Interval: intervalToUse,
	}

	isPending, err := s.queueManager.IsTaskPendingOrRunning(payload)
	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"position": pos,
			"interval": intervalToUse,
		}).Error("Failed to check if task is pending")
		return false
	}

	if isPending {
		s.log.WithFields(logrus.Fields{
			"model_id":  trans.GetID(),
			"gap_index": gapIndex,
			"position":  pos,
			"interval":  intervalToUse,
		}).Debug("Task already pending for gap, skipping")
		return false
	}

	s.log.WithFields(logrus.Fields{
		"model_id":       trans.GetID(),
		"gap_index":      gapIndex,
		"gap_start":      gap.StartPos,
		"gap_end":        gap.EndPos,
		"position":       pos,
		"interval":       intervalToUse,
		"model_interval": maxInterval,
		"gap_size":       gapSize,
	}).Info("Enqueueing backfill task for gap (processing from end backward)")

	s.checkAndEnqueuePositionWithTrigger(ctx, trans, pos, intervalToUse)
	return true
}
