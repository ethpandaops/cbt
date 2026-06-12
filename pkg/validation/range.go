package validation

import (
	"context"
	"fmt"
	"slices"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

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
	depProvider, ok := handler.(transformation.DependencyHandler)
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

	depProvider, ok := handler.(transformation.DependencyHandler)
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

	// Asserts only the GetFillBuffer method rather than the full transformation.FillHandler
	// so callers that supply just the buffer (e.g. internal test stubs) still match.
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
