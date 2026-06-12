package validation

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// checkIncrementalDependencyGaps checks for gaps in incremental transformation dependencies.
// Returns the furthest gap end position and whether any gaps were found.
// For OR groups, a gap only blocks if ALL members have gaps.
func (v *dependencyValidator) checkIncrementalDependencyGaps(
	ctx context.Context,
	modelID string,
	position, interval uint64,
) (nextValidPos uint64, hasGaps bool, err error) {
	node, err := v.dag.GetNode(modelID)
	if err != nil {
		return 0, false, err
	}

	model, ok := node.Model.(models.Transformation)
	if !ok {
		return 0, false, nil
	}

	handler := model.GetHandler()
	if handler == nil {
		return 0, false, nil
	}

	endPos := position + interval

	// Get structured dependencies (with OR group support)
	depProvider, ok := handler.(transformation.DependencyHandler)
	if !ok {
		return 0, false, nil
	}

	nextValid, gaps := v.checkDependencyGaps(ctx, depProvider.GetDependencies(), position, endPos)
	return nextValid, gaps, nil
}

// checkDependencyGaps checks structured dependencies for gaps
func (v *dependencyValidator) checkDependencyGaps(
	ctx context.Context,
	deps []transformation.Dependency,
	position, endPos uint64,
) (nextValidPos uint64, hasGaps bool) {
	var maxGapEnd uint64
	for _, dep := range deps {
		var nextValid uint64
		var depHasGaps bool
		if dep.IsGroup {
			nextValid, depHasGaps = v.checkORGroupGaps(ctx, dep.GroupDeps, position, endPos)
		} else {
			nextValid, depHasGaps = v.checkSingleDependencyGaps(ctx, dep.SingleDep, position, endPos)
		}
		if depHasGaps && nextValid > maxGapEnd {
			maxGapEnd = nextValid
			hasGaps = true
		}
	}
	return maxGapEnd, hasGaps
}

// checkORGroupGaps checks if an OR group has gaps.
// An OR group only blocks if ALL members have gaps.
func (v *dependencyValidator) checkORGroupGaps(
	ctx context.Context,
	groupDeps []string,
	position, endPos uint64,
) (nextValidPos uint64, hasGaps bool) {
	var minNextValid uint64
	for _, depID := range groupDeps {
		nextValid, depHasGaps := v.checkSingleDependencyGaps(ctx, depID, position, endPos)
		if !depHasGaps {
			return 0, false // At least one member has no gaps
		}
		if minNextValid == 0 || nextValid < minNextValid {
			minNextValid = nextValid
		}
	}
	return minNextValid, true
}

// checkSingleDependencyGaps checks a single dependency for gaps
func (v *dependencyValidator) checkSingleDependencyGaps(
	ctx context.Context,
	depID string,
	position, endPos uint64,
) (nextValidPos uint64, hasGaps bool) {
	if !v.isIncrementalTransformation(depID) {
		return 0, false
	}

	firstPos, err := v.admin.GetFirstPosition(ctx, depID)
	if err != nil {
		v.log.WithError(err).WithField("dependency_id", depID).Debug("Gap check: failed to get first position")
		return 0, false
	}

	lastEndPos, err := v.admin.GetNextUnprocessedPosition(ctx, depID)
	if err != nil {
		v.log.WithError(err).WithField("dependency_id", depID).Debug("Gap check: failed to get last position")
		return 0, false
	}

	gaps, err := v.admin.FindGaps(ctx, depID, firstPos, lastEndPos, maxGapQueryLimit)
	if err != nil {
		v.log.WithError(err).WithField("dependency_id", depID).Debug("Gap check: failed to query gaps")
		return 0, false
	}

	return v.processGapsForRange(gaps, position, endPos)
}

// isIncrementalTransformation checks if a dependency is an incremental transformation.
//
// Incremental vs Scheduled Transformations:
//   - Incremental: Process sequential positions (0, 1, 2, ...), can have gaps
//     Example: Processing blockchain blocks sequentially
//   - Scheduled: Run on time schedules (hourly, daily), don't have position gaps
func (v *dependencyValidator) isIncrementalTransformation(depID string) bool {
	depNode, err := v.dag.GetNode(depID)
	if err != nil {
		return false
	}

	if depNode.NodeType != models.NodeTypeTransformation {
		return false
	}

	depModel, ok := depNode.Model.(models.Transformation)
	if !ok {
		return false
	}

	depHandler := depModel.GetHandler()
	if depHandler == nil {
		return false
	}

	return depHandler.ShouldTrackPosition()
}

// processGapsForRange finds the furthest gap end that affects our processing range.
//
// Gap Processing Logic:
// - A gap affects our range if it overlaps with [position, endPos]
// - We need the furthest (maximum) gap end to know where to skip to
// - Overlapping gaps are handled by taking the maximum end position
//
// Example:
//
//	Processing range [100-150]
//	Gap1: [105-115] -> affects range, end=115
//	Gap2: [110-125] -> affects range, end=125
//	Gap3: [160-170] -> doesn't affect range (outside)
//	Result: Returns 125 (maximum of affecting gaps)
func (v *dependencyValidator) processGapsForRange(
	gaps []admin.GapInfo,
	position, endPos uint64,
) (uint64, bool) {
	var (
		maxGapEnd = uint64(0)
		hasGaps   = false
	)

	for _, gap := range gaps {
		// Check if this gap overlaps with our processing range
		// A gap overlaps if:
		// - It starts before our range ends (gap.StartPos < endPos) AND
		// - It ends after our range starts (gap.EndPos > position)
		if gap.StartPos < endPos && gap.EndPos > position {
			hasGaps = true
			// Track the furthest gap end - this is where we can safely resume
			if gap.EndPos > maxGapEnd {
				maxGapEnd = gap.EndPos
			}
		}
	}

	return maxGapEnd, hasGaps
}
