// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// ExternalModelExecutor interface for getting external model bounds
type ExternalModelExecutor interface {
	GetMinMax(ctx context.Context, modelConfig *models.ModelConfig) (minPos, maxPos uint64, err error)
	HasDataInRange(ctx context.Context, modelConfig *models.ModelConfig, startPos, endPos uint64) (bool, error)
}

// DependencyValidatorImpl implements the DependencyValidator interface
type DependencyValidatorImpl struct {
	adminManager    AdminTableManagerInterface
	externalManager ExternalModelExecutor
	depManager      *dependencies.DependencyGraph
	logger          *logrus.Logger
}

// NewDependencyValidator creates a new dependency validator
func NewDependencyValidator(
	adminManager AdminTableManagerInterface,
	externalManager ExternalModelExecutor,
	depManager *dependencies.DependencyGraph,
	logger *logrus.Logger,
) *DependencyValidatorImpl {
	return &DependencyValidatorImpl{
		adminManager:    adminManager,
		externalManager: externalManager,
		depManager:      depManager,
		logger:          logger,
	}
}

// ValidateDependencies checks if all dependencies are satisfied for a model at a given position
func (v *DependencyValidatorImpl) ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error) {
	_, exists := v.depManager.GetModelConfig(modelID)
	if !exists {
		return Result{
			CanProcess: false,
			Errors:     []error{fmt.Errorf("%w: %s", models.ErrModelNotFound, modelID)},
		}, nil
	}

	deps := v.depManager.GetDependencies(modelID)
	if len(deps) == 0 {
		v.logger.WithField("model_id", modelID).Debug("No dependencies to validate")
		return Result{CanProcess: true}, nil
	}

	depStatuses := make([]DependencyStatus, 0, len(deps))
	var errors []error
	canProcess := true

	for _, depID := range deps {
		depConfig, exists := v.depManager.GetModelConfig(depID)
		if !exists {
			status := DependencyStatus{
				ModelID:   depID,
				Available: false,
				Error:     fmt.Errorf("%w: %s", ErrDependencyNotFound, depID),
			}
			depStatuses = append(depStatuses, status)
			errors = append(errors, status.Error)
			canProcess = false
			continue
		}

		status, err := v.validateSingleDependency(ctx, depID, &depConfig, position, interval)
		if err != nil {
			errors = append(errors, err)
			canProcess = false
		}
		if !status.Available {
			canProcess = false
		}
		depStatuses = append(depStatuses, status)
	}

	v.logger.WithFields(logrus.Fields{
		"model_id":    modelID,
		"position":    position,
		"interval":    interval,
		"can_process": canProcess,
		"dep_count":   len(deps),
	}).Debug("Dependency validation complete")

	return Result{
		CanProcess:   canProcess,
		Dependencies: depStatuses,
		Errors:       errors,
	}, nil
}

func (v *DependencyValidatorImpl) validateSingleDependency(ctx context.Context, depID string, depConfig *models.ModelConfig, position, interval uint64) (DependencyStatus, error) {
	if depConfig.External {
		return v.validateExternalDependency(ctx, depConfig, position, interval)
	}
	return v.validateTransformationDependency(ctx, depID, position, interval)
}

func (v *DependencyValidatorImpl) validateExternalDependency(ctx context.Context, depConfig *models.ModelConfig, position, interval uint64) (DependencyStatus, error) {
	status := DependencyStatus{
		ModelID: fmt.Sprintf("%s.%s", depConfig.Database, depConfig.Table),
	}

	minPos, maxPos, err := v.externalManager.GetMinMax(ctx, depConfig)
	if err != nil {
		status.Error = fmt.Errorf("failed to get external model bounds: %w", err)
		return status, err
	}

	status.MinPos = minPos
	status.MaxPos = maxPos

	// Check if required range is available
	requiredStart := position
	requiredEnd := position + interval

	// Quick check: if completely outside bounds, no need to query
	if requiredStart < minPos || requiredStart > maxPos {
		status.Available = false
		status.Error = fmt.Errorf("%w for model %s: required range [%d, %d), model has [%d, %d]",
			ErrRangeNotAvailable, status.ModelID, requiredStart, requiredEnd, minPos, maxPos)
		v.logger.WithFields(logrus.Fields{
			"dep_model": status.ModelID,
			"available": status.Available,
			"min":       minPos,
			"max":       maxPos,
			"req_start": requiredStart,
			"req_end":   requiredEnd,
		}).Debug("External dependency outside bounds")
		return status, nil
	}

	// For sparse data: Check if ANY data actually exists in this specific range
	hasData, err := v.externalManager.HasDataInRange(ctx, depConfig, requiredStart, requiredEnd)
	if err != nil {
		// Fall back to bounds check on error
		v.logger.WithError(err).Warn("Failed to check data existence in range, falling back to bounds check")
		status.Available = requiredStart >= minPos && requiredEnd <= maxPos
	} else {
		status.Available = hasData
	}

	if !status.Available {
		status.Error = fmt.Errorf("%w for model %s: no data in range [%d, %d)",
			ErrRangeNotAvailable, status.ModelID, requiredStart, requiredEnd)
	}

	v.logger.WithFields(logrus.Fields{
		"dep_model": status.ModelID,
		"available": status.Available,
		"min":       minPos,
		"max":       maxPos,
		"req_start": requiredStart,
		"req_end":   requiredEnd,
		"has_data":  status.Available,
	}).Debug("Validated external dependency")

	return status, nil
}

func (v *DependencyValidatorImpl) validateTransformationDependency(ctx context.Context, depID string, position, interval uint64) (DependencyStatus, error) {
	status := DependencyStatus{
		ModelID: depID,
	}

	// Check range coverage in admin table
	available, err := v.adminManager.GetCoverage(ctx, depID, position, position+interval)
	if err != nil {
		status.Error = fmt.Errorf("failed to check coverage for %s: %w", depID, err)
		return status, err
	}

	status.Available = available

	if !available {
		status.Error = fmt.Errorf("%w for model %s: range [%d, %d)",
			ErrRangeNotCovered, depID, position, position+interval)
	}

	v.logger.WithFields(logrus.Fields{
		"dep_model": depID,
		"available": available,
		"position":  position,
		"interval":  interval,
	}).Debug("Validated transformation dependency")

	return status, nil
}

// GetInitialPosition calculates the initial position for a model based on its dependencies
// Returns the earliest position where all dependencies have data available
func (v *DependencyValidatorImpl) GetInitialPosition(ctx context.Context, modelID string) (uint64, error) {
	v.logger.WithField("model_id", modelID).Debug("GetInitialPosition called")

	deps := v.depManager.GetDependencies(modelID)
	if len(deps) == 0 {
		return 0, nil // No dependencies, start from 0
	}

	var maxOfMins uint64

	for _, depID := range deps {
		depConfig, exists := v.depManager.GetModelConfig(depID)
		if !exists {
			return 0, fmt.Errorf("%w: %s", ErrDependencyNotFound, depID)
		}

		minPos, err := v.getMinPositionForDependency(ctx, &depConfig, depID)
		if err != nil {
			return 0, err
		}

		// Find the maximum of all minimums - this ensures all dependencies have data
		if minPos > maxOfMins {
			maxOfMins = minPos
		}
	}

	// Get the model's interval
	modelConfig, _ := v.depManager.GetModelConfig(modelID)

	// Round down to the nearest interval boundary
	if maxOfMins > 0 && modelConfig.Interval > 0 {
		// Align to interval boundary
		alignedPos := (maxOfMins / modelConfig.Interval) * modelConfig.Interval

		// If rounding down puts us before the data starts, round up instead
		if alignedPos < maxOfMins {
			v.logger.WithFields(logrus.Fields{
				"model_id":   modelID,
				"maxOfMins":  maxOfMins,
				"alignedPos": alignedPos,
				"interval":   modelConfig.Interval,
				"rounded_up": alignedPos + modelConfig.Interval,
			}).Debug("Rounding up initial position")
			alignedPos += modelConfig.Interval
		}

		v.logger.WithFields(logrus.Fields{
			"model_id":   modelID,
			"maxOfMins":  maxOfMins,
			"alignedPos": alignedPos,
			"interval":   modelConfig.Interval,
		}).Debug("Calculated initial position")

		return alignedPos, nil
	}

	return maxOfMins, nil
}

func (v *DependencyValidatorImpl) getMinPositionForDependency(ctx context.Context, depConfig *models.ModelConfig, depID string) (uint64, error) {
	if depConfig.External {
		// For external models, get the minimum position (earliest data)
		minPos, _, err := v.externalManager.GetMinMax(ctx, depConfig)
		if err != nil {
			return 0, fmt.Errorf("failed to get external model bounds for %s: %w", depID, err)
		}
		return minPos, nil
	}

	// For transformation models
	return v.getTransformationModelMinPosition(ctx, depID)
}

func (v *DependencyValidatorImpl) getTransformationModelMinPosition(ctx context.Context, depID string) (uint64, error) {
	// Get the first position
	minPos, err := v.adminManager.GetFirstPosition(ctx, depID)
	if err != nil {
		return 0, fmt.Errorf("failed to get first position for %s: %w", depID, err)
	}

	// If no data exists yet (minPos == 0), fall back to checking dependencies recursively
	if minPos == 0 {
		minPos, err = v.GetInitialPosition(ctx, depID)
		if err != nil {
			return 0, fmt.Errorf("failed to get initial position for dependency %s: %w", depID, err)
		}
	}

	return minPos, nil
}
