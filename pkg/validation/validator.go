// Package validation provides dependency validation for CBT models.
// It determines valid position ranges based on dependency data availability
// and validates whether positions can be processed.
package validation

//go:generate mockgen -package mock -destination mock/validator.mock.go -source validator.go Validator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

// Ensure dependencyValidator implements Validator interface
var _ Validator = (*dependencyValidator)(nil)

// RangeSemantics determines how dependency bounds are combined
type RangeSemantics int

const (
	// Union semantics: min = MIN(external_mins), useful for forward fill
	// where we can start as soon as ANY external dependency has data
	Union RangeSemantics = iota

	// Intersection semantics: min = MAX(all_mins), useful for backfill
	// where we need ALL dependencies to have data at a position
	Intersection
)

// Validator defines the interface for dependency validation
type Validator interface {
	// ValidateDependencies checks if all dependencies are satisfied for a given position.
	// Returns whether the position can be processed and the next valid position if not.
	ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error)

	// GetValidRange returns the valid position range [min, max] based on dependency bounds.
	// Use Union for forward fill, Intersection for backfill.
	GetValidRange(ctx context.Context, modelID string, semantics RangeSemantics) (minPos, maxPos uint64, err error)

	// GetStartPosition calculates the starting position for a NEW model.
	// Considers fill direction (head-first vs tail-first) from handler config.
	GetStartPosition(ctx context.Context, modelID string) (uint64, error)
}

// ExternalValidator defines the interface for external model bounds retrieval
type ExternalValidator interface {
	GetMinMax(ctx context.Context, model models.External) (uint64, uint64, error)
}

// dependencyValidator implements the Validator interface
type dependencyValidator struct {
	log             logrus.FieldLogger
	admin           admin.Service
	externalManager ExternalValidator
	dag             models.DAGReader
}

// Result contains the result of dependency validation
type Result struct {
	CanProcess   bool    // Whether current position can be processed
	Errors       []error // Validation errors
	NextValidPos uint64  // Next position where dependencies are available (0 if can process or no next position)
}

// maxGapQueryLimit is the maximum number of gaps to retrieve in a single query.
const maxGapQueryLimit = 1000

// Validation-specific errors
var (
	ErrModelNotFound               = errors.New("model not found")
	ErrDependencyNotFound          = errors.New("dependency model not found")
	ErrRangeNotAvailable           = errors.New("required range not available")
	ErrRangeNotCovered             = errors.New("range not fully covered")
	ErrNotTransformationModel      = errors.New("model is not a transformation")
	ErrInvalidDependencyType       = errors.New("invalid dependency type")
	ErrInvalidModelType            = errors.New("invalid dependency model type")
	ErrFailedModelCast             = errors.New("failed to cast model to transformation")
	ErrNoORDependencyAvailable     = errors.New("no dependencies in OR group are available")
	ErrUninitializedTransformation = errors.New("transformation dependency has not been initialized")
)

// getTransformationModel retrieves and validates a transformation model from the DAG.
// It performs the common pattern of: get node -> check type -> cast to transformation.
func (v *dependencyValidator) getTransformationModel(modelID string) (models.Transformation, error) {
	node, err := v.dag.GetNode(modelID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrModelNotFound, modelID)
	}

	if node.NodeType != models.NodeTypeTransformation {
		return nil, fmt.Errorf("%w: %s", ErrNotTransformationModel, modelID)
	}

	model, ok := node.Model.(models.Transformation)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFailedModelCast, modelID)
	}

	return model, nil
}

// NewDependencyValidator creates a new dependency validator
func NewDependencyValidator(
	log logrus.FieldLogger,
	chClient clickhouse.ClientInterface,
	adminService admin.Service,
	modelsService models.Service,
) Validator {
	externalManager := NewExternalModelExecutor(log, adminService)

	return &dependencyValidator{
		log:             log.WithField("service", "validator"),
		admin:           adminService,
		externalManager: externalManager,
		dag:             modelsService.GetDAG(),
	}
}

// ValidateDependencies validates whether all dependencies for a model are satisfied at a given position.
// For incremental transformations, it also detects gaps in dependencies and returns the next valid position.
func (v *dependencyValidator) ValidateDependencies(
	ctx context.Context,
	modelID string,
	position, interval uint64,
) (Result, error) {
	// Use intersection semantics to ensure ALL dependencies have data at this position
	minValid, maxValid, err := v.GetValidRange(ctx, modelID, Intersection)
	if err != nil {
		return Result{CanProcess: false, Errors: []error{err}}, nil
	}

	requestedEnd := position + interval
	canProcess := position >= minValid && requestedEnd <= maxValid

	if !canProcess {
		// If position is below minValid (e.g., due to limits.min config),
		// tell forward fill to skip to minValid instead of giving up
		nextValidPos := uint64(0)
		if position < minValid {
			nextValidPos = minValid
			v.log.WithFields(logrus.Fields{
				"model_id":       modelID,
				"position":       position,
				"min_valid":      minValid,
				"next_valid_pos": nextValidPos,
				"reason":         "position below minimum valid range",
			}).Debug("Position below minValid, setting NextValidPos to skip gap")
		}

		return Result{
			CanProcess:   false,
			NextValidPos: nextValidPos,
			Errors:       []error{ErrRangeNotAvailable},
		}, nil
	}

	// Gap-aware processing for incremental transformations
	// Check if any incremental transformation dependencies have gaps (missing data ranges)
	// This prevents processing positions where dependency data is missing
	nextValidPos, hasGaps, err := v.checkIncrementalDependencyGaps(ctx, modelID, position, interval)
	if err != nil {
		v.log.WithError(err).WithField("model_id", modelID).Debug("Failed to check dependency gaps")
		return Result{CanProcess: true}, nil
	}

	if hasGaps {
		// Gap detected: Cannot process this position, but we know where to skip to
		// NextValidPos tells the coordinator to jump ahead to where data IS available
		return Result{
			CanProcess:   false,
			NextValidPos: nextValidPos,
			Errors:       []error{ErrRangeNotCovered},
		}, nil
	}

	return Result{CanProcess: true}, nil
}

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

	// Try structured dependencies first (with OR group support)
	type dependencyProvider interface {
		GetDependencies() []transformation.Dependency
	}
	if depProvider, ok := handler.(dependencyProvider); ok {
		nextValid, gaps := v.checkDependencyGaps(ctx, depProvider.GetDependencies(), position, endPos)
		return nextValid, gaps, nil
	}

	// Fallback to flattened dependencies
	type flatProvider interface{ GetFlattenedDependencies() []string }
	if provider, ok := handler.(flatProvider); ok {
		nextValid, gaps := v.checkFlattenedDependencyGaps(ctx, provider.GetFlattenedDependencies(), position, endPos)
		return nextValid, gaps, nil
	}

	return 0, false, nil
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

// checkFlattenedDependencyGaps checks flattened dependencies for gaps (legacy fallback)
func (v *dependencyValidator) checkFlattenedDependencyGaps(
	ctx context.Context,
	depIDs []string,
	position, endPos uint64,
) (nextValidPos uint64, hasGaps bool) {
	var maxGapEnd uint64
	for _, depID := range depIDs {
		nextValid, depHasGaps := v.checkSingleDependencyGaps(ctx, depID, position, endPos)
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
		return 0, false
	}

	lastEndPos, err := v.admin.GetNextUnprocessedPosition(ctx, depID)
	if err != nil {
		return 0, false
	}

	gaps, err := v.admin.FindGaps(ctx, depID, firstPos, lastEndPos, maxGapQueryLimit)
	if err != nil {
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

// GetStartPosition calculates the starting position for a NEW model.
// Supports both head-first (from most recent data) and tail-first (from oldest data) strategies.
func (v *dependencyValidator) GetStartPosition(ctx context.Context, modelID string) (uint64, error) {
	model, err := v.getTransformationModel(modelID)
	if err != nil {
		return 0, err
	}

	handler := model.GetHandler()

	// Get interval from handler
	var interval uint64
	if handler != nil {
		if intervalProvider, ok := handler.(interface{ GetMaxInterval() uint64 }); ok {
			interval = intervalProvider.GetMaxInterval()
		}
	}

	// Get fill direction from handler (default: head-first)
	direction := "head"
	if handler != nil {
		if directionProvider, ok := handler.(interface{ GetFillDirection() string }); ok {
			direction = directionProvider.GetFillDirection()
		}
	}

	minPos, maxPos, err := v.GetValidRange(ctx, modelID, Union)
	if err != nil {
		return 0, err
	}

	if maxPos == 0 || maxPos == ^uint64(0) {
		return 0, nil
	}

	var startPos uint64
	if direction == "tail" {
		startPos = minPos
	} else {
		// Head-first: start one interval back from max, but not below min
		if maxPos > interval && maxPos-interval > minPos {
			startPos = maxPos - interval
		} else {
			startPos = minPos
		}
	}

	v.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"direction": direction,
		"start_pos": startPos,
	}).Debug("Calculated start position")

	return startPos, nil
}

// dependencyBounds holds min/max bounds for dependencies
type dependencyBounds struct {
	externalMins       []uint64
	externalMaxs       []uint64
	transformationMins []uint64
	transformationMaxs []uint64
}

// getBoundsForDependency gets bounds for a dependency node (external or transformation)
func (v *dependencyValidator) getBoundsForDependency(ctx context.Context, depID string) (minBound, maxBound uint64, nodeType models.NodeType, err error) {
	depNode, err := v.dag.GetNode(depID)
	if err != nil {
		return 0, 0, "", fmt.Errorf("%w: %s", ErrDependencyNotFound, depID)
	}

	switch depNode.NodeType {
	case models.NodeTypeExternal:
		externalModel, ok := depNode.Model.(models.External)
		if !ok {
			return 0, 0, "", fmt.Errorf("%w: %T", ErrInvalidModelType, depNode.Model)
		}
		minBound, maxBound, err = v.externalManager.GetMinMax(ctx, externalModel)
		if err != nil {
			return 0, 0, "", fmt.Errorf("failed to get bounds for external %s: %w", depID, err)
		}
		return minBound, maxBound, models.NodeTypeExternal, nil

	case models.NodeTypeTransformation:
		minBound, err = v.admin.GetFirstPosition(ctx, depID)
		if err != nil {
			return 0, 0, "", fmt.Errorf("failed to get first position for %s: %w", depID, err)
		}
		maxBound, err = v.admin.GetNextUnprocessedPosition(ctx, depID)
		if err != nil {
			return 0, 0, "", fmt.Errorf("failed to get last position for %s: %w", depID, err)
		}
		observability.RecordModelBounds(depID, minBound, maxBound)
		return minBound, maxBound, models.NodeTypeTransformation, nil

	default:
		return 0, 0, "", fmt.Errorf("%w: %s", ErrInvalidDependencyType, depNode.NodeType)
	}
}

// collectDependencyBoundsWithOR collects dependency bounds considering OR groups
// For OR groups, it takes the best available bounds from the group
func (v *dependencyValidator) collectDependencyBoundsWithOR(ctx context.Context, model models.Transformation) (*dependencyBounds, error) {
	bounds := &dependencyBounds{
		externalMins:       []uint64{},
		externalMaxs:       []uint64{},
		transformationMins: []uint64{},
		transformationMaxs: []uint64{},
	}

	// Get dependencies from handler
	handler := model.GetHandler()
	if handler == nil {
		return bounds, nil
	}

	// For incremental types, get dependencies with OR support
	if err := v.processDependenciesFromHandler(ctx, handler, bounds); err != nil {
		return nil, err
	}

	return bounds, nil
}

// processDependenciesFromHandler processes dependencies from a handler if it provides them
func (v *dependencyValidator) processDependenciesFromHandler(ctx context.Context, handler transformation.Handler, bounds *dependencyBounds) error {
	type dependencyProvider interface {
		GetDependencies() []transformation.Dependency
	}

	depProvider, ok := handler.(dependencyProvider)
	if !ok {
		return nil
	}

	dependencies := depProvider.GetDependencies()
	for _, dep := range dependencies {
		if dep.IsGroup {
			// Process OR group
			if err := v.processORGroup(ctx, dep, bounds); err != nil {
				return err
			}
		} else {
			// Process single dependency
			if err := v.processSingleDependency(ctx, dep.SingleDep, bounds); err != nil {
				return err
			}
		}
	}
	return nil
}

// processORGroup processes an OR group dependency and adds the union of all members' bounds.
// For OR semantics ("at least one must be available"), we use:
//   - min = MIN(all members) - can start when ANY member has data
//   - max = MAX(all members) - can continue as long as ANY member has data
func (v *dependencyValidator) processORGroup(ctx context.Context, dep transformation.Dependency, bounds *dependencyBounds) error {
	var (
		orGroupMin     = ^uint64(0)
		orGroupMax     = uint64(0)
		hasValidMember = false
		nodeType       = models.NodeType("")
	)

	for _, depID := range dep.GroupDeps {
		minDep, maxDep, depNodeType, err := v.getBoundsForDependency(ctx, depID)
		if err != nil || (minDep == 0 && maxDep == 0) {
			continue // Skip unavailable or empty dependencies
		}

		if minDep < orGroupMin {
			orGroupMin = minDep
		}
		if maxDep > orGroupMax {
			orGroupMax = maxDep
		}
		hasValidMember = true
		nodeType = depNodeType
	}

	if !hasValidMember {
		return fmt.Errorf("%w: %v", ErrNoORDependencyAvailable, dep.GroupDeps)
	}

	switch nodeType {
	case models.NodeTypeExternal:
		bounds.externalMins = append(bounds.externalMins, orGroupMin)
		bounds.externalMaxs = append(bounds.externalMaxs, orGroupMax)
	case models.NodeTypeTransformation:
		bounds.transformationMins = append(bounds.transformationMins, orGroupMin)
		bounds.transformationMaxs = append(bounds.transformationMaxs, orGroupMax)
	}

	return nil
}

// processSingleDependency processes a single (AND) dependency
func (v *dependencyValidator) processSingleDependency(ctx context.Context, depID string, bounds *dependencyBounds) error {
	minDep, maxDep, nodeType, err := v.getBoundsForDependency(ctx, depID)
	if err != nil {
		return err
	}

	switch nodeType {
	case models.NodeTypeExternal:
		bounds.externalMins = append(bounds.externalMins, minDep)
		bounds.externalMaxs = append(bounds.externalMaxs, maxDep)

	case models.NodeTypeTransformation:
		// Only include incremental transformations in bounds calculation.
		// Scheduled transformations don't track positions.
		if v.isIncrementalTransformation(depID) {
			if minDep == 0 && maxDep == 0 {
				return fmt.Errorf("%w: %s", ErrUninitializedTransformation, depID)
			}
			bounds.transformationMins = append(bounds.transformationMins, minDep)
			bounds.transformationMaxs = append(bounds.transformationMaxs, maxDep)
		}
	}

	return nil
}

// calculateRangeUnion calculates range using union semantics for forward fill.
// min = MAX(MIN(external_mins), MAX(transformation_mins))
// max = MIN(all maxes)
func (v *dependencyValidator) calculateRangeUnion(bounds *dependencyBounds) (minPos, maxPos uint64) {
	var finalMin uint64
	if len(bounds.externalMins) > 0 {
		finalMin = slices.Min(bounds.externalMins)
	}

	if len(bounds.transformationMins) > 0 {
		transformationMax := slices.Max(bounds.transformationMins)
		if transformationMax > finalMin {
			finalMin = transformationMax
		}
	}

	finalMax := ^uint64(0)

	allMaxes := make([]uint64, 0, len(bounds.externalMaxs)+len(bounds.transformationMaxs))
	allMaxes = append(allMaxes, bounds.externalMaxs...)
	allMaxes = append(allMaxes, bounds.transformationMaxs...)

	if len(allMaxes) > 0 {
		finalMax = slices.Min(allMaxes)
	}

	return finalMin, finalMax
}

// calculateRangeIntersection calculates range using intersection semantics for backfill.
// min = MAX(all mins) - requires ALL dependencies to have data
// max = MIN(all maxes)
func (v *dependencyValidator) calculateRangeIntersection(bounds *dependencyBounds) (minPos, maxPos uint64) {
	allMins := make([]uint64, 0, len(bounds.externalMins)+len(bounds.transformationMins))
	allMins = append(allMins, bounds.externalMins...)
	allMins = append(allMins, bounds.transformationMins...)

	var finalMin uint64
	if len(allMins) > 0 {
		finalMin = slices.Max(allMins)
	}

	finalMax := ^uint64(0)

	allMaxes := make([]uint64, 0, len(bounds.externalMaxs)+len(bounds.transformationMaxs))
	allMaxes = append(allMaxes, bounds.externalMaxs...)
	allMaxes = append(allMaxes, bounds.transformationMaxs...)

	if len(allMaxes) > 0 {
		finalMax = slices.Min(allMaxes)
	}

	return finalMin, finalMax
}

// GetValidRange returns the valid position range [min, max] based on dependency bounds.
// Use Union semantics for forward fill, Intersection for backfill.
func (v *dependencyValidator) GetValidRange(ctx context.Context, modelID string, semantics RangeSemantics) (minPos, maxPos uint64, err error) {
	model, err := v.getTransformationModel(modelID)
	if err != nil {
		return 0, 0, err
	}

	if !v.hasDependencies(model) {
		return 0, 0, nil
	}

	bounds, err := v.collectDependencyBoundsWithOR(ctx, model)
	if err != nil {
		return 0, 0, err
	}

	// Calculate range based on semantics
	var finalMin, finalMax uint64
	if semantics == Intersection {
		finalMin, finalMax = v.calculateRangeIntersection(bounds)
	} else {
		finalMin, finalMax = v.calculateRangeUnion(bounds)
	}

	// Apply configured limits and buffer
	finalMin, finalMax = v.applyLimitsFromHandler(model.GetHandler(), finalMin, finalMax)
	finalMax = v.applyFillBuffer(model.GetHandler(), modelID, finalMin, finalMax)

	if finalMin > finalMax {
		return 0, 0, nil
	}

	v.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"semantics": semantics,
		"final_min": finalMin,
		"final_max": finalMax,
	}).Debug("Calculated valid range")

	return finalMin, finalMax, nil
}

// hasDependencies checks if a model has dependencies through its handler
func (v *dependencyValidator) hasDependencies(model models.Transformation) bool {
	handler := model.GetHandler()
	if handler == nil {
		return false
	}

	type dependencyProvider interface{ GetFlattenedDependencies() []string }
	depProvider, ok := handler.(dependencyProvider)
	if !ok {
		return false
	}

	deps := depProvider.GetFlattenedDependencies()
	return len(deps) > 0
}

// applyLimitsFromHandler applies configured limits from handler if available
func (v *dependencyValidator) applyLimitsFromHandler(handler transformation.Handler, finalMin, finalMax uint64) (adjustedMin, adjustedMax uint64) {
	if handler == nil {
		return finalMin, finalMax
	}

	provider, ok := handler.(transformation.LimitsHandler)
	if !ok {
		return finalMin, finalMax
	}

	limits := provider.GetLimits()
	if limits == nil {
		return finalMin, finalMax
	}

	if limits.Min > 0 && limits.Min > finalMin {
		finalMin = limits.Min
	}

	if limits.Max > 0 && limits.Max < finalMax {
		finalMax = limits.Max
	}

	return finalMin, finalMax
}

// applyFillBuffer applies fill.buffer configuration if available
// Buffer reduces the max position to stay N positions behind dependency data
func (v *dependencyValidator) applyFillBuffer(handler transformation.Handler, modelID string, finalMin, finalMax uint64) uint64 {
	if handler == nil {
		return finalMax
	}

	type bufferProvider interface {
		GetFillBuffer() uint64
	}

	provider, ok := handler.(bufferProvider)
	if !ok {
		return finalMax
	}

	buffer := provider.GetFillBuffer()
	if buffer == 0 {
		return finalMax
	}

	// Apply buffer: stay N positions behind dependency max
	if finalMax > buffer {
		originalMax := finalMax
		finalMax -= buffer
		v.log.WithFields(logrus.Fields{
			"model_id":     modelID,
			"original_max": originalMax,
			"buffer":       buffer,
			"adjusted_max": finalMax,
		}).Debug("Applied fill.buffer to dependency max position")
	} else {
		// Buffer exceeds max, set to min
		v.log.WithFields(logrus.Fields{
			"model_id": modelID,
			"max":      finalMax,
			"buffer":   buffer,
		}).Debug("Buffer exceeds max, setting max to min")
		finalMax = finalMin
	}

	return finalMax
}
