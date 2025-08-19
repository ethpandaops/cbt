// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// DependencyValidator implements the DependencyValidator interface
type DependencyValidator struct {
	log             *logrus.Logger
	admin           *admin.Service
	externalManager *ExternalModelValidator
	dag             *models.DependencyGraph
}

// DependencyStatus represents the status of a single dependency
type DependencyStatus struct {
	ModelID   string
	Available bool
	MinPos    uint64
	MaxPos    uint64
	Error     error
}

// Result contains the result of dependency validation
type Result struct {
	CanProcess   bool
	Dependencies []DependencyStatus
	Errors       []error
}

// Validation-specific errors
var (
	ErrModelNotFound          = errors.New("model not found")
	ErrDependencyNotFound     = errors.New("dependency model not found")
	ErrRangeNotAvailable      = errors.New("required range not available")
	ErrRangeNotCovered        = errors.New("range not fully covered")
	ErrNotTransformationModel = errors.New("model is not a transformation")
	ErrInvalidDependencyType  = errors.New("invalid dependency type")
	ErrInvalidModelType       = errors.New("invalid dependency model type")
	ErrFailedModelCast        = errors.New("failed to cast model to transformation")
)

// NewDependencyValidator creates a new dependency validator
func NewDependencyValidator(
	log *logrus.Logger,
	chClient clickhouse.ClientInterface,
	adminService *admin.Service,
	modelsService *models.Service,
) *DependencyValidator {
	externalManager := NewExternalModelExecutor(log, chClient, adminService, modelsService)

	return &DependencyValidator{
		log:             log,
		admin:           adminService,
		externalManager: externalManager,
		dag:             modelsService.GetDAG(),
	}
}

// ValidateDependencies checks if all dependencies are satisfied for a model at a given position
func (v *DependencyValidator) ValidateDependencies(ctx context.Context, modelID string, position, interval uint64) (Result, error) {
	_, err := v.dag.GetNode(modelID)
	if err != nil {
		return Result{
			CanProcess: false,
			Errors:     []error{fmt.Errorf("%w: %s", ErrModelNotFound, modelID)},
		}, nil
	}

	deps := v.dag.GetDependencies(modelID)
	if len(deps) == 0 {
		v.log.WithField("model_id", modelID).Debug("No dependencies to validate")
		return Result{CanProcess: true}, nil
	}

	depStatuses := make([]DependencyStatus, 0, len(deps))
	var errs []error
	canProcess := true

	for _, depID := range deps {
		depNode, err := v.dag.GetNode(depID)
		if err != nil {
			status := DependencyStatus{
				ModelID:   depID,
				Available: false,
				Error:     fmt.Errorf("%w: %s", ErrDependencyNotFound, depID),
			}
			depStatuses = append(depStatuses, status)
			errs = append(errs, status.Error)
			canProcess = false
			continue
		}

		var status DependencyStatus

		switch depNode.NodeType {
		case models.NodeTypeTransformation:
			model, ok := depNode.Model.(models.Transformation)
			if !ok {
				errs = append(errs, fmt.Errorf("%w: %T", ErrInvalidModelType, depNode.Model))
				canProcess = false
				continue
			}

			status, err = v.validateTransformationDependency(ctx, model, position, interval)
		case models.NodeTypeExternal:
			model, ok := depNode.Model.(models.External)
			if !ok {
				errs = append(errs, fmt.Errorf("%w: %T", ErrInvalidModelType, depNode.Model))
				canProcess = false
				continue
			}

			status, err = v.validateExternalDependency(ctx, model, position, interval)
		}

		if err != nil {
			errs = append(errs, err)
			canProcess = false
		}

		if !status.Available {
			canProcess = false
		}

		depStatuses = append(depStatuses, status)
	}

	v.log.WithFields(logrus.Fields{
		"model_id":    modelID,
		"position":    position,
		"interval":    interval,
		"can_process": canProcess,
		"dep_count":   len(deps),
	}).Debug("Dependency validation complete")

	return Result{
		CanProcess:   canProcess,
		Dependencies: depStatuses,
		Errors:       errs,
	}, nil
}

func (v *DependencyValidator) validateExternalDependency(ctx context.Context, model models.External, position, interval uint64) (DependencyStatus, error) {
	status := DependencyStatus{
		ModelID: model.GetID(),
	}

	minPos, maxPos, err := v.externalManager.GetMinMax(ctx, model)
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
		v.log.WithFields(logrus.Fields{
			"dep_model": status.ModelID,
			"available": status.Available,
			"min":       minPos,
			"max":       maxPos,
			"req_start": requiredStart,
			"req_end":   requiredEnd,
		}).Debug("External dependency outside bounds")

		return status, nil
	}

	status.Available = requiredStart >= minPos && requiredEnd <= maxPos

	if !status.Available {
		status.Error = fmt.Errorf("%w for model %s: no data in range [%d, %d)",
			ErrRangeNotAvailable, status.ModelID, requiredStart, requiredEnd)
	}

	v.log.WithFields(logrus.Fields{
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

func (v *DependencyValidator) validateTransformationDependency(ctx context.Context, model models.Transformation, position, interval uint64) (DependencyStatus, error) {
	status := DependencyStatus{
		ModelID: model.GetID(),
	}

	// Check range coverage in admin table
	available, err := v.admin.GetCoverage(ctx, model.GetID(), position, position+interval)
	if err != nil {
		status.Error = fmt.Errorf("failed to check coverage for %s: %w", model.GetID(), err)
		return status, err
	}

	status.Available = available

	if !available {
		status.Error = fmt.Errorf("%w for model %s: range [%d, %d)",
			ErrRangeNotCovered, model.GetID(), position, position+interval)
	}

	v.log.WithFields(logrus.Fields{
		"dep_model": model.GetID(),
		"available": available,
		"position":  position,
		"interval":  interval,
	}).Debug("Validated transformation dependency")

	return status, nil
}

// GetInitialPosition calculates the initial position for a model based on its dependencies
// Returns the earliest position where all dependencies have data available
func (v *DependencyValidator) GetInitialPosition(ctx context.Context, modelID string) (uint64, error) {
	v.log.WithField("model_id", modelID).Debug("GetInitialPosition called")

	deps := v.dag.GetDependencies(modelID)
	if len(deps) == 0 {
		return 0, nil // No dependencies, start from 0
	}

	var maxOfMins uint64

	for _, depID := range deps {
		minPos, err := v.getMinPositionForDependency(ctx, depID)
		if err != nil {
			return 0, err
		}

		// Find the maximum of all minimums - this ensures all dependencies have data
		if minPos > maxOfMins {
			maxOfMins = minPos
		}
	}

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
	interval := model.GetConfig().Interval

	// Round down to the nearest interval boundary
	if maxOfMins > 0 && interval > 0 {
		// Align to interval boundary
		alignedPos := (maxOfMins / interval) * interval

		// If rounding down puts us before the data starts, round up instead
		if alignedPos < maxOfMins {
			v.log.WithFields(logrus.Fields{
				"model_id":   modelID,
				"maxOfMins":  maxOfMins,
				"alignedPos": alignedPos,
				"interval":   interval,
				"rounded_up": alignedPos + interval,
			}).Debug("Rounding up initial position")
			alignedPos += interval
		}

		v.log.WithFields(logrus.Fields{
			"model_id":   modelID,
			"maxOfMins":  maxOfMins,
			"alignedPos": alignedPos,
			"interval":   interval,
		}).Debug("Calculated initial position")

		return alignedPos, nil
	}

	return maxOfMins, nil
}

func (v *DependencyValidator) getMinPositionForDependency(ctx context.Context, depID string) (uint64, error) {
	depNode, err := v.dag.GetNode(depID)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrDependencyNotFound, depID)
	}

	switch depNode.NodeType {
	case models.NodeTypeExternal:
		externalModel, ok := depNode.Model.(models.External)
		if !ok {
			return 0, fmt.Errorf("%w: %T", ErrInvalidModelType, depNode.Model)
		}
		minPos, _, err := v.externalManager.GetMinMax(ctx, externalModel)
		if err != nil {
			return 0, fmt.Errorf("failed to get external model bounds for %s: %w", depID, err)
		}
		return minPos, nil
	case models.NodeTypeTransformation:
		return v.getTransformationModelMinPosition(ctx, depID)
	}

	return 0, fmt.Errorf("%w: %s", ErrInvalidDependencyType, depNode.NodeType)
}

func (v *DependencyValidator) getTransformationModelMinPosition(ctx context.Context, depID string) (uint64, error) {
	// Get the first position
	minPos, err := v.admin.GetFirstPosition(ctx, depID)
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
