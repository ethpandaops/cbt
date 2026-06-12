// Package validation provides dependency validation for CBT models.
// It determines valid position ranges based on dependency data availability
// and validates whether positions can be processed.
package validation

//go:generate go tool mockgen -package mock -destination mock/validator.mock.go -source validator.go Validator

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
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
		return nil, fmt.Errorf("%w: %s: %w", ErrModelNotFound, modelID, err)
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

// NewDependencyValidator creates a new dependency validator with explicit dependencies.
func NewDependencyValidator(
	log logrus.FieldLogger,
	adminService admin.Service,
	externalValidator ExternalValidator,
	dag models.DAGReader,
) Validator {
	return &dependencyValidator{
		log:             log.WithField("service", "validator"),
		admin:           adminService,
		externalManager: externalValidator,
		dag:             dag,
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
		// Fail open: if the gap check itself errors (e.g. a transient admin query failure),
		// allow processing rather than blocking. This is an intentional
		// availability-over-strictness trade-off — the bounds check above already
		// guarantees the position is within the valid range, and a failed gap lookup
		// should not stall the pipeline.
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

// GetStartPosition calculates the starting position for a NEW model.
// Supports both head-first (from most recent data) and tail-first (from oldest data) strategies.
// Uses Intersection semantics to ensure the start position is where ALL dependencies have data.
func (v *dependencyValidator) GetStartPosition(ctx context.Context, modelID string) (uint64, error) {
	model, err := v.getTransformationModel(modelID)
	if err != nil {
		return 0, err
	}

	handler := model.GetHandler()

	// Get interval from handler
	var interval uint64
	if handler != nil {
		if intervalProvider, ok := handler.(transformation.IntervalHandler); ok {
			interval = intervalProvider.GetMaxInterval()
		}
	}

	// Get fill direction from handler (default: head-first)
	direction := "head"
	if handler != nil {
		if directionProvider, ok := handler.(transformation.FillHandler); ok {
			direction = directionProvider.GetFillDirection()
		}
	}

	// Use Intersection semantics: min = MAX(all dependency mins), max = MIN(all dependency maxes)
	// This ensures we start at a position where ALL dependencies have data available,
	// which is required for successful processing. Union semantics would give us the
	// earliest position where ANY dependency has data, but that position may not be
	// processable if other dependencies don't have data there yet.
	minPos, maxPos, err := v.GetValidRange(ctx, modelID, Intersection)
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
