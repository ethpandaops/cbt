// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

// Validator defines the interface for dependency validation (ethPandaOps pattern)
type Validator interface {
	// ValidateDependencies checks if all dependencies are satisfied for a given position
	ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error)

	// GetInitialPosition calculates the initial position for a model based on its dependencies
	GetInitialPosition(ctx context.Context, modelID string) (uint64, error)

	// GetEarliestPosition gets the earliest available position for a model
	GetEarliestPosition(ctx context.Context, modelID string) (uint64, error)

	// GetValidRange returns the valid position range [min, max] for a model based on its dependencies
	// min = MAX(MIN(external_mins), MAX(transformation_mins))
	// max = MIN(MAX(external_maxs), MIN(transformation_maxs))
	GetValidRange(ctx context.Context, modelID string) (minPos, maxPos uint64, err error)
}

// ExternalValidator defines the interface for external model validation
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
	ErrInsufficientRange           = errors.New("insufficient dependency range for interval")
	ErrNoORDependencyAvailable     = errors.New("no dependencies in OR group are available")
	ErrUninitializedTransformation = errors.New("transformation dependency has not been initialized")
)

// NewDependencyValidator creates a new dependency validator
func NewDependencyValidator(
	log logrus.FieldLogger,
	chClient clickhouse.ClientInterface,
	adminService admin.Service,
	modelsService models.Service,
) Validator {
	externalManager := NewExternalModelExecutor(log, chClient, adminService, modelsService)

	return &dependencyValidator{
		log:             log.WithField("service", "validator"),
		admin:           adminService,
		externalManager: externalManager,
		dag:             modelsService.GetDAG(),
	}
}

// ValidateDependencies checks if all dependencies are satisfied for a model at a given position
func (v *dependencyValidator) ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error) {
	// Check if position falls within dependency bounds
	minValid, maxValid, err := v.GetValidRange(ctx, modelID)
	if err != nil {
		return Result{CanProcess: false, Errors: []error{err}}, nil
	}

	requestedEnd := position + interval
	canProcess := position >= minValid && requestedEnd <= maxValid

	if !canProcess {
		return Result{
			CanProcess: false,
			Errors:     []error{ErrRangeNotAvailable},
		}, nil
	}

	// Check for gaps in incremental transformation dependencies
	nextValidPos, hasGaps, err := v.checkIncrementalDependencyGaps(ctx, modelID, position, interval)
	if err != nil {
		v.log.WithError(err).WithField("model_id", modelID).Debug("Failed to check dependency gaps")
		return Result{CanProcess: true}, nil
	}

	if hasGaps {
		return Result{
			CanProcess:   false,
			NextValidPos: nextValidPos,
			Errors:       []error{ErrRangeNotCovered},
		}, nil
	}

	return Result{CanProcess: true}, nil
}

// checkIncrementalDependencyGaps checks for gaps in incremental transformation dependencies
func (v *dependencyValidator) checkIncrementalDependencyGaps(ctx context.Context, modelID string, position, interval uint64) (nextValidPos uint64, hasGaps bool, err error) {
	// Get model and its dependencies
	node, err := v.dag.GetNode(modelID)
	if err != nil {
		return 0, false, err
	}

	model, ok := node.Model.(models.Transformation)
	if !ok {
		return 0, false, nil // Not a transformation, no gaps to check
	}

	handler := model.GetHandler()
	if handler == nil {
		return 0, false, nil
	}

	// Get dependencies from handler
	type depProvider interface {
		GetFlattenedDependencies() []string
	}
	provider, ok := handler.(depProvider)
	if !ok {
		return 0, false, nil
	}

	endPos := position + interval
	maxGapEnd := uint64(0)
	hasAnyGaps := false

	// Check each dependency for gaps
	for _, depID := range provider.GetFlattenedDependencies() {
		// Only check incremental transformation dependencies
		if !v.isIncrementalTransformation(depID) {
			continue
		}

		// Get gaps for this dependency in our requested range
		gaps, err := v.admin.FindGaps(ctx, depID, position, endPos, 1000)
		if err != nil {
			v.log.WithError(err).WithField("dependency_id", depID).Debug("Failed to find gaps in dependency")
			continue
		}

		// Process gaps with proper sorting and merging
		nextValid, hasGaps := v.processGapsForRange(gaps, position, endPos)
		if hasGaps {
			hasAnyGaps = true
			if nextValid > maxGapEnd {
				maxGapEnd = nextValid
			}
		}
	}

	return maxGapEnd, hasAnyGaps, nil
}

// isIncrementalTransformation checks if a dependency is an incremental transformation
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

	// Only incremental transformations track positions and can have gaps
	return depHandler.ShouldTrackPosition()
}

// processGapsForRange finds the furthest gap end that affects our range
func (v *dependencyValidator) processGapsForRange(gaps []admin.GapInfo, position, endPos uint64) (uint64, bool) {
	maxGapEnd := uint64(0)
	hasGaps := false

	for _, gap := range gaps {
		// Gap affects our range if it overlaps with [position, endPos]
		if gap.StartPos < endPos && gap.EndPos > position {
			hasGaps = true
			if gap.EndPos > maxGapEnd {
				maxGapEnd = gap.EndPos
			}
		}
	}

	return maxGapEnd, hasGaps
}

// findMin returns the minimum value from a slice of uint64
func findMin(values []uint64) uint64 {
	if len(values) == 0 {
		return 0
	}
	minVal := values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
	}
	return minVal
}

// findMax returns the maximum value from a slice of uint64
func findMax(values []uint64) uint64 {
	if len(values) == 0 {
		return 0
	}
	maxVal := values[0]
	for _, v := range values[1:] {
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal
}

// GetEarliestPosition calculates the earliest position for a model based on its dependencies
// Returns the earliest position where all dependencies have data available (for backfill scanning)
func (v *dependencyValidator) GetEarliestPosition(ctx context.Context, modelID string) (uint64, error) {
	v.log.WithField("model_id", modelID).Debug("GetEarliestPosition called")

	// Use GetValidRange to get the valid range
	minPos, _, err := v.GetValidRange(ctx, modelID)
	if err != nil {
		// Return 0 for invalid models (backward compatibility)
		if errors.Is(err, ErrModelNotFound) || errors.Is(err, ErrNotTransformationModel) {
			return 0, nil
		}
		return 0, err
	}

	// Get the model's interval for alignment
	node, err := v.dag.GetNode(modelID)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrModelNotFound, modelID)
	}

	if node.NodeType != models.NodeTypeTransformation {
		return 0, fmt.Errorf("%w: %s", ErrNotTransformationModel, modelID)
	}

	model, ok := node.Model.(models.Transformation)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrFailedModelCast, modelID)
	}
	// Get interval from handler
	var interval uint64
	handler := model.GetHandler()
	if handler != nil {
		if intervalProvider, ok := handler.(interface{ GetMaxInterval() uint64 }); ok {
			interval = intervalProvider.GetMaxInterval()
		}
	}

	// For backfill gap detection, we want to start from where data is available
	// Don't round up past the actual data availability point
	if minPos > 0 && interval > 0 {
		// Align to interval boundary (round down)
		alignedPos := (minPos / interval) * interval

		// For backfill, if rounding down would start before data is available,
		// use the actual data start position instead of rounding up past it
		if alignedPos < minPos {
			v.log.WithFields(logrus.Fields{
				"model_id":            modelID,
				"data_available_from": minPos,
				"would_align_to":      alignedPos,
				"interval":            interval,
				"using_actual_start":  minPos,
			}).Debug("Using actual data start position for backfill (not rounding up)")
			// Use the actual position where data starts
			return minPos, nil
		}

		v.log.WithFields(logrus.Fields{
			"model_id":   modelID,
			"minPos":     minPos,
			"alignedPos": alignedPos,
			"interval":   interval,
		}).Debug("Using aligned position for backfill")

		return alignedPos, nil
	}

	return minPos, nil
}

// GetInitialPosition calculates the initial position for a model starting from the head (most recent data)
// Returns the latest position where all dependencies have data available minus one interval
func (v *dependencyValidator) GetInitialPosition(ctx context.Context, modelID string) (uint64, error) {
	v.log.WithField("model_id", modelID).Debug("GetInitialPosition called (head-first)")

	// Get the model's interval
	node, err := v.dag.GetNode(modelID)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrModelNotFound, modelID)
	}

	if node.NodeType != models.NodeTypeTransformation {
		return 0, fmt.Errorf("%w: %s", ErrNotTransformationModel, modelID)
	}

	model, ok := node.Model.(models.Transformation)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrFailedModelCast, modelID)
	}
	// Get interval from handler
	var interval uint64
	handler := model.GetHandler()
	if handler != nil {
		if intervalProvider, ok := handler.(interface{ GetMaxInterval() uint64 }); ok {
			interval = intervalProvider.GetMaxInterval()
		}
	}

	// Use GetValidRange to get the valid range
	minPos, maxPos, err := v.GetValidRange(ctx, modelID)
	if err != nil {
		return 0, err
	}

	// If no data available
	if maxPos == 0 || maxPos == ^uint64(0) {
		return 0, nil
	}

	// Simple calculation: start one interval back from max, but not below min
	var initialPos uint64
	if maxPos > interval {
		targetPos := maxPos - interval
		if targetPos > minPos {
			initialPos = targetPos
		} else {
			initialPos = minPos
		}
	} else {
		// Not enough data for even one interval from 0
		initialPos = minPos
	}

	v.log.WithFields(logrus.Fields{
		"model_id":   modelID,
		"minPos":     minPos,
		"maxPos":     maxPos,
		"interval":   interval,
		"initialPos": initialPos,
	}).Debug("Calculated initial position (head-first)")

	return initialPos, nil
}

// dependencyBounds holds min/max bounds for dependencies
type dependencyBounds struct {
	externalMins       []uint64
	externalMaxs       []uint64
	transformationMins []uint64
	transformationMaxs []uint64
}

// collectExternalBounds collects bounds for an external dependency
func (v *dependencyValidator) collectExternalBounds(ctx context.Context, depNode models.Node, depID string) (minDep, maxDep uint64, err error) {
	externalModel, ok := depNode.Model.(models.External)
	if !ok {
		return 0, 0, fmt.Errorf("%w: %T", ErrInvalidModelType, depNode.Model)
	}

	// Get min/max for external model (with lag applied if configured)
	minDep, maxDep, err = v.externalManager.GetMinMax(ctx, externalModel)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get bounds for external %s: %w", depID, err)
	}

	return minDep, maxDep, nil
}

// collectTransformationBounds collects bounds for a transformation dependency
func (v *dependencyValidator) collectTransformationBounds(ctx context.Context, depID string) (minDep, maxDep uint64, err error) {
	// Get first and last position for transformation
	minDep, err = v.admin.GetFirstPosition(ctx, depID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get first position for %s: %w", depID, err)
	}

	maxDep, err = v.admin.GetLastProcessedEndPosition(ctx, depID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get last position for %s: %w", depID, err)
	}

	// Record transformation bounds in metrics
	observability.RecordModelBounds(depID, minDep, maxDep)

	return minDep, maxDep, nil
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

// processORGroup processes an OR group dependency and adds the best available bounds
func (v *dependencyValidator) processORGroup(ctx context.Context, dep transformation.Dependency, bounds *dependencyBounds) error {
	bestMin := uint64(0)
	bestMax := uint64(0)
	bestFound := false
	bestDepID := ""
	bestNodeType := models.NodeType("")

	// Find the best available dependency in the OR group
	for _, depID := range dep.GroupDeps {
		depNode, err := v.dag.GetNode(depID)
		if err != nil {
			v.log.WithFields(logrus.Fields{
				"dependency": depID,
				"error":      err,
			}).Debug("Skipping missing dependency in OR group")
			continue // Skip missing dependencies in OR groups
		}

		minDep, maxDep, err := v.getBoundsForNode(ctx, depNode, depID)
		if err != nil {
			v.log.WithFields(logrus.Fields{
				"dependency": depID,
				"error":      err,
			}).Debug("Skipping dependency with bounds error in OR group")
			continue
		}

		// For transformations, skip if no data (not initialized)
		if depNode.NodeType == models.NodeTypeTransformation && (minDep == 0 && maxDep == 0) {
			v.log.WithFields(logrus.Fields{
				"dependency": depID,
				"min":        minDep,
				"max":        maxDep,
			}).Debug("Skipping uninitialized transformation dependency in OR group")
			continue
		}

		// Use the dependency with the best (widest) range
		if !bestFound || (maxDep-minDep) > (bestMax-bestMin) {
			bestMin = minDep
			bestMax = maxDep
			bestFound = true
			bestDepID = depID
			bestNodeType = depNode.NodeType
		}
	}

	if !bestFound {
		return fmt.Errorf("%w: %v (all dependencies are uninitialized or unavailable)", ErrNoORDependencyAvailable, dep.GroupDeps)
	}

	// Add the best bounds to the appropriate category
	switch bestNodeType {
	case models.NodeTypeExternal:
		bounds.externalMins = append(bounds.externalMins, bestMin)
		bounds.externalMaxs = append(bounds.externalMaxs, bestMax)
	case models.NodeTypeTransformation:
		bounds.transformationMins = append(bounds.transformationMins, bestMin)
		bounds.transformationMaxs = append(bounds.transformationMaxs, bestMax)
	}

	v.log.WithFields(logrus.Fields{
		"or_group": dep.GroupDeps,
		"selected": bestDepID,
		"min":      bestMin,
		"max":      bestMax,
	}).Debug("Selected best dependency from OR group")

	return nil
}

// processSingleDependency processes a single (AND) dependency
func (v *dependencyValidator) processSingleDependency(ctx context.Context, depID string, bounds *dependencyBounds) error {
	depNode, err := v.dag.GetNode(depID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrDependencyNotFound, depID)
	}

	minDep, maxDep, err := v.getBoundsForNode(ctx, depNode, depID)
	if err != nil {
		return err
	}

	switch depNode.NodeType {
	case models.NodeTypeExternal:
		bounds.externalMins = append(bounds.externalMins, minDep)
		bounds.externalMaxs = append(bounds.externalMaxs, maxDep)

	case models.NodeTypeTransformation:
		// Check if transformation has been initialized (has data)
		if minDep == 0 && maxDep == 0 {
			// Transformation dependency has no data - cannot process
			return fmt.Errorf("%w: %s", ErrUninitializedTransformation, depID)
		}
		// Include transformation bounds
		bounds.transformationMins = append(bounds.transformationMins, minDep)
		bounds.transformationMaxs = append(bounds.transformationMaxs, maxDep)

	default:
		return fmt.Errorf("%w: %s", ErrInvalidDependencyType, depNode.NodeType)
	}

	return nil
}

// getBoundsForNode gets bounds for a dependency node (external or transformation)
func (v *dependencyValidator) getBoundsForNode(ctx context.Context, depNode models.Node, depID string) (minBound, maxBound uint64, err error) {
	switch depNode.NodeType {
	case models.NodeTypeExternal:
		return v.collectExternalBounds(ctx, depNode, depID)
	case models.NodeTypeTransformation:
		return v.collectTransformationBounds(ctx, depID)
	default:
		return 0, 0, fmt.Errorf("%w: %s", ErrInvalidDependencyType, depNode.NodeType)
	}
}

// calculateFinalRange calculates the final min/max from collected bounds
// min = MAX(MIN(external_mins), MAX(transformation_mins))
// max = MIN of all maxes (both external and transformation)
func (v *dependencyValidator) calculateFinalRange(bounds *dependencyBounds) (minPos, maxPos uint64) {
	// Calculate min position: MAX(MIN(external_mins), MAX(transformation_mins))
	// Special case: external models can start from their earliest data (they forward fill)
	// but transformations need ALL to have data (they may not backfill consistently)
	var finalMin uint64
	if len(bounds.externalMins) > 0 {
		finalMin = findMin(bounds.externalMins)
	}
	if len(bounds.transformationMins) > 0 {
		transformationMax := findMax(bounds.transformationMins)
		if transformationMax > finalMin {
			finalMin = transformationMax
		}
	}

	// Calculate max position: MIN of ALL dependency maxes
	// We must stop at the earliest endpoint of any dependency
	finalMax := ^uint64(0) // Start with max uint64

	// Combine all maxes and find the minimum
	allMaxes := make([]uint64, 0, len(bounds.externalMaxs)+len(bounds.transformationMaxs))
	allMaxes = append(allMaxes, bounds.externalMaxs...)
	allMaxes = append(allMaxes, bounds.transformationMaxs...)
	if len(allMaxes) > 0 {
		finalMax = findMin(allMaxes)
	}

	return finalMin, finalMax
}

// GetValidRange returns the valid position range [min, max] for a model based on its dependencies
// This is the single source of truth for calculating valid ranges
// min = MAX(MIN(external_mins), MAX(transformation_mins))
// max = MIN(all dependency maxes)
func (v *dependencyValidator) GetValidRange(ctx context.Context, modelID string) (minPos, maxPos uint64, err error) {
	v.log.WithField("model_id", modelID).Debug("GetValidRange called")

	// Get the model to check if it's a transformation
	node, err := v.dag.GetNode(modelID)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %s", ErrModelNotFound, modelID)
	}

	if node.NodeType != models.NodeTypeTransformation {
		return 0, 0, fmt.Errorf("%w: %s", ErrNotTransformationModel, modelID)
	}

	model, ok := node.Model.(models.Transformation)
	if !ok {
		return 0, 0, fmt.Errorf("%w: %s", ErrFailedModelCast, modelID)
	}

	// Get the model's configuration (not used directly anymore, dependencies from handler)

	// Check if model has dependencies through handler
	if !v.hasDependencies(model) {
		return 0, 0, nil
	}

	// Collect all dependency bounds (with OR group support)
	bounds, err := v.collectDependencyBoundsWithOR(ctx, model)
	if err != nil {
		return 0, 0, err
	}

	// Calculate the final range
	finalMin, finalMax := v.calculateFinalRange(bounds)

	// Apply configured limits from handler if any
	finalMin, finalMax = v.applyLimitsFromHandler(model.GetHandler(), modelID, finalMin, finalMax)

	// Ensure min <= max
	if finalMin > finalMax {
		// No valid range
		return 0, 0, nil
	}

	v.log.WithFields(logrus.Fields{
		"model_id":             modelID,
		"external_count":       len(bounds.externalMins),
		"transformation_count": len(bounds.transformationMins),
		"external_min":         findMin(bounds.externalMins),
		"external_max":         findMax(bounds.externalMaxs),
		"transformation_min":   findMin(bounds.transformationMins),
		"transformation_max":   findMax(bounds.transformationMaxs),
		"final_min":            finalMin,
		"final_max":            finalMax,
	}).Debug("Calculated valid range for model")

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
func (v *dependencyValidator) applyLimitsFromHandler(handler transformation.Handler, _ string, finalMin, finalMax uint64) (adjustedMin, adjustedMax uint64) {
	if handler == nil {
		return finalMin, finalMax
	}

	type limitsProvider interface {
		GetLimits() *struct {
			Min uint64
			Max uint64
		}
	}

	provider, ok := handler.(limitsProvider)
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

// Ensure dependencyValidator implements Validator interface
var _ Validator = (*dependencyValidator)(nil)
