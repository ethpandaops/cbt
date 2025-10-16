package coordinator

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/observability"
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
	handler := trans.GetHandler()
	if handler == nil {
		return
	}

	// Get max interval from handler
	maxInterval := s.getMaxInterval(handler)

	// Get last processed positions
	lastPos, lastEndPos, hasData := s.getBackfillBounds(ctx, trans.GetID())
	if !hasData {
		return
	}

	// Handle no data case
	if !s.handleNoDataCase(trans, handler, lastEndPos) {
		return
	}

	// Log existing data if present
	s.logExistingData(trans.GetID(), lastPos, lastEndPos, maxInterval)

	// Check if we have enough data to scan
	if lastEndPos < maxInterval {
		s.log.WithField("model_id", trans.GetID()).Debug("Not enough data to scan for gaps yet")
		return
	}

	// Find and process gaps
	gaps := s.findGaps(ctx, trans, lastEndPos, maxInterval)
	if gaps == nil {
		return
	}

	if len(gaps) == 0 {
		return
	}

	// Process the found gaps
	s.processGaps(ctx, trans, gaps)
}

func (s *service) getMaxInterval(handler transformation.Handler) uint64 {
	type intervalProvider interface {
		GetMaxInterval() uint64
	}
	if provider, ok := handler.(intervalProvider); ok {
		return provider.GetMaxInterval()
	}
	return 0
}

func (s *service) handleNoDataCase(trans models.Transformation, handler transformation.Handler, lastEndPos uint64) bool {
	if lastEndPos != 0 {
		return true
	}

	type scheduleProvider interface {
		IsForwardFillEnabled() bool
	}
	if provider, ok := handler.(scheduleProvider); ok && provider.IsForwardFillEnabled() {
		s.log.WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"reason":   "forward fill enabled, waiting for initial forward fill to populate data",
		}).Debug("Skipping backfill - no data yet and forward fill is enabled")
		return false
	}

	// If forward fill is not enabled, backfill can take the lead
	s.log.WithFields(logrus.Fields{
		"model_id": trans.GetID(),
		"reason":   "forward fill not enabled, backfill will handle initial population",
	}).Debug("No data yet but forward fill disabled - backfill will populate from earliest position")
	return true
}

func (s *service) logExistingData(modelID string, lastPos, lastEndPos, maxInterval uint64) {
	if lastEndPos > 0 {
		s.log.WithFields(logrus.Fields{
			"model_id":             modelID,
			"last_processed_start": lastPos,
			"last_processed_end":   lastEndPos,
			"backfill_interval":    maxInterval,
		}).Debug("Starting gap scan - found existing processed data")
	}
}

func (s *service) findGaps(ctx context.Context, trans models.Transformation, lastEndPos, maxInterval uint64) []admin.GapInfo {
	// Calculate scan range
	scanRange, err := s.calculateBackfillScanRange(ctx, trans, lastEndPos)
	if err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Debug("Failed to calculate scan range")
		return nil
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
		return nil
	}

	// Check if we found any gaps
	if len(gaps) == 0 {
		s.log.WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"min_pos":  scanRange.initialPos,
			"max_pos":  scanRange.maxPos,
		}).Debug("No gaps found in processed data")
		return gaps
	}

	// Log gap summary
	s.logGapSummary(trans, gaps, scanRange)
	return gaps
}

func (s *service) logGapSummary(trans models.Transformation, gaps []admin.GapInfo, scanRange *backfillScanRange) {
	interval := s.getMaxInterval(trans.GetHandler())
	s.log.WithFields(logrus.Fields{
		"model_id":  trans.GetID(),
		"gap_count": len(gaps),
		"min_pos":   scanRange.initialPos,
		"max_pos":   scanRange.maxPos,
		"interval":  interval,
	}).Info("Found gaps in processed data")
}

func (s *service) processGaps(ctx context.Context, trans models.Transformation, gaps []admin.GapInfo) {
	const maxGapsToCheck = 10
	stats := &gapProcessingStats{}

	for i, gap := range gaps {
		if !s.shouldContinueProcessing(trans.GetID(), stats, i, len(gaps), maxGapsToCheck) {
			break
		}

		stats.checked++

		if !s.isGapFillable(ctx, trans, gap, i) {
			s.handleUnfillableGap(trans.GetID(), gap, i, stats)
			continue
		}

		if s.handleFillableGap(ctx, trans, gap, i, stats, len(gaps)) {
			break
		}
	}

	s.logProcessingSummary(trans.GetID(), stats, len(gaps))
}

type gapProcessingStats struct {
	checked  int
	skipped  int
	enqueued int
}

func (s *service) shouldContinueProcessing(modelID string, stats *gapProcessingStats, index, totalGaps, maxCheck int) bool {
	if stats.checked >= maxCheck {
		observability.RecordBackfillGapCheckLimitReached(modelID)
		s.log.WithFields(logrus.Fields{
			"model_id":       modelID,
			"gaps_checked":   stats.checked,
			"gaps_skipped":   stats.skipped,
			"gaps_enqueued":  stats.enqueued,
			"remaining_gaps": totalGaps - index,
		}).Debug("Reached max gap check limit, will re-scan for more gaps after completion")
		return false
	}
	return true
}

func (s *service) handleUnfillableGap(modelID string, gap admin.GapInfo, index int, stats *gapProcessingStats) {
	stats.skipped++
	observability.RecordBackfillGapAnalysis(modelID, "blocked")
	s.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"gap_index": index,
		"gap_start": gap.StartPos,
		"gap_end":   gap.EndPos,
		"gap_size":  gap.EndPos - gap.StartPos,
	}).Debug("Gap not fillable due to dependency constraints, checking next gap")
}

func (s *service) handleFillableGap(ctx context.Context, trans models.Transformation, gap admin.GapInfo, index int, stats *gapProcessingStats, totalGaps int) bool {
	observability.RecordBackfillGapAnalysis(trans.GetID(), "fillable")
	if s.processSingleGap(ctx, trans, gap, index) {
		stats.enqueued++
		s.log.WithFields(logrus.Fields{
			"model_id":       trans.GetID(),
			"gap_index":      index,
			"gaps_checked":   stats.checked,
			"gaps_skipped":   stats.skipped,
			"gaps_enqueued":  stats.enqueued,
			"remaining_gaps": totalGaps - index - 1,
		}).Info("Enqueued fillable backfill task")
		return true
	}
	return false
}

func (s *service) logProcessingSummary(modelID string, stats *gapProcessingStats, totalGaps int) {
	if stats.enqueued == 0 && stats.checked > 0 {
		s.log.WithFields(logrus.Fields{
			"model_id":     modelID,
			"total_gaps":   totalGaps,
			"gaps_checked": stats.checked,
			"gaps_skipped": stats.skipped,
		}).Info("No fillable gaps found - all checked gaps blocked by dependencies")
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

// isGapFillable checks if a gap can be filled based on dependency validation.
// This method performs the same interval calculation as processSingleGap to determine
// the actual position and interval that would be used, then validates dependencies
// for that specific position. This respects the model's interval.max constraint,
// ensuring we validate the position that will actually be processed.
func (s *service) isGapFillable(ctx context.Context, trans models.Transformation, gap admin.GapInfo, gapIndex int) bool {
	handler := trans.GetHandler()
	if handler == nil {
		return false
	}

	// Get max and min intervals from handler (same logic as processSingleGap)
	type intervalProvider interface {
		GetMaxInterval() uint64
		GetMinInterval() uint64
	}
	var maxInterval, minInterval uint64
	if provider, ok := handler.(intervalProvider); ok {
		maxInterval = provider.GetMaxInterval()
		minInterval = provider.GetMinInterval()
	}

	gapSize := gap.EndPos - gap.StartPos

	// Determine interval to use (same logic as processSingleGap)
	// This ensures we validate for the ACTUAL interval that will be processed
	var intervalToUse uint64
	switch {
	case minInterval == 0:
		// If min interval is 0, use gap size but cap at maxInterval
		if gapSize > maxInterval {
			intervalToUse = maxInterval
		} else {
			intervalToUse = gapSize
		}
	case gapSize < minInterval:
		intervalToUse = minInterval
	case gapSize < maxInterval:
		intervalToUse = gapSize
	default:
		intervalToUse = maxInterval
	}

	// Calculate position (same logic as processSingleGap)
	pos := gap.EndPos - intervalToUse

	// Validate dependencies for this exact position and interval
	validationResult, err := s.validator.ValidateDependencies(ctx, trans.GetID(), pos, intervalToUse)
	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id":  trans.GetID(),
			"gap_index": gapIndex,
			"position":  pos,
			"interval":  intervalToUse,
		}).Debug("Validation error checking gap fillability")
		return false
	}

	if !validationResult.CanProcess {
		// Log why this gap can't be filled
		logFields := logrus.Fields{
			"model_id":  trans.GetID(),
			"gap_index": gapIndex,
			"gap_start": gap.StartPos,
			"gap_end":   gap.EndPos,
			"position":  pos,
			"interval":  intervalToUse,
		}
		if validationResult.NextValidPos > 0 {
			logFields["next_valid_pos"] = validationResult.NextValidPos
		}
		if len(validationResult.Errors) > 0 {
			logFields["validation_errors"] = validationResult.Errors
		}
		s.log.WithFields(logFields).Debug("Gap validation failed - dependencies not satisfied")
		return false
	}

	return true
}

// processSingleGap processes a single gap for backfill
func (s *service) processSingleGap(ctx context.Context, trans models.Transformation, gap admin.GapInfo, gapIndex int) bool {
	gapSize := gap.EndPos - gap.StartPos

	// Get max and min intervals from handler
	type intervalProvider interface {
		GetMaxInterval() uint64
		GetMinInterval() uint64
	}
	var maxInterval, minInterval uint64
	if provider, ok := trans.GetHandler().(intervalProvider); ok {
		maxInterval = provider.GetMaxInterval()
		minInterval = provider.GetMinInterval()
	}

	// Determine interval to use
	var intervalToUse uint64
	switch {
	case minInterval == 0:
		// If min interval is 0, use gap size but cap at maxInterval
		if gapSize > maxInterval {
			intervalToUse = maxInterval
			s.log.WithFields(logrus.Fields{
				"model_id":          trans.GetID(),
				"gap_index":         gapIndex,
				"gap_size":          gapSize,
				"max_interval":      maxInterval,
				"adjusted_interval": intervalToUse,
			}).Debug("Capped interval at max for large gap (min interval is 0)")
		} else {
			intervalToUse = gapSize
		}
	case gapSize < minInterval:
		// If gap is smaller than min interval, use min interval (may overlap)
		intervalToUse = minInterval
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"gap_index":         gapIndex,
			"gap_size":          gapSize,
			"min_interval":      minInterval,
			"adjusted_interval": intervalToUse,
		}).Debug("Using min interval for small gap (may overlap with existing data)")
	case gapSize < maxInterval:
		// If gap is between min and max, use the gap size
		intervalToUse = gapSize
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"gap_index":         gapIndex,
			"gap_size":          gapSize,
			"model_interval":    maxInterval,
			"adjusted_interval": intervalToUse,
		}).Debug("Adjusted interval for small gap")
	default:
		// Use max interval for large gaps
		intervalToUse = maxInterval
	}

	// Calculate position - work backwards from gap end to meet forward fill
	// This fills recent data first (closer to current time) rather than oldest data first
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
	}).Info("Enqueueing backfill task for gap")

	// checkAndEnqueuePositionWithTrigger handles deduplication via IsTaskPendingOrRunning
	s.checkAndEnqueuePositionWithTrigger(ctx, trans, pos, intervalToUse, string(DirectionBack))
	return true
}
