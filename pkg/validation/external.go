// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

// ErrExternalNotInitialized is returned when external model bounds cache is not yet populated.
var ErrExternalNotInitialized = errors.New("external model bounds not initialized")

// ExternalModelValidator implements the ExternalValidator interface
type ExternalModelValidator struct {
	log   logrus.FieldLogger
	admin admin.Service
}

// NewExternalModelValidator creates a new external model validator
func NewExternalModelValidator(log logrus.FieldLogger, adminService admin.Service) *ExternalModelValidator {
	return &ExternalModelValidator{
		log:   log,
		admin: adminService,
	}
}

// applyLag applies lag adjustment to max position if configured
func (e *ExternalModelValidator) applyLag(model models.External, minPos, maxPos uint64) (adjustedMin, adjustedMax uint64) {
	lag := model.GetConfig().Lag
	if lag == 0 {
		return minPos, maxPos
	}

	if maxPos > lag {
		return minPos, maxPos - lag
	}

	// Lag exceeds max - no data available
	e.log.WithFields(logrus.Fields{
		"model": model.GetID(),
		"lag":   lag,
		"max":   maxPos,
	}).Warn("Lag exceeds available data range")

	return minPos, minPos
}

// GetMinMax retrieves the min and max position values for an external model
func (e *ExternalModelValidator) GetMinMax(ctx context.Context, model models.External) (minPos, maxPos uint64, err error) {
	modelID := model.GetID()

	cached, err := e.admin.GetExternalBounds(ctx, modelID)
	if err != nil || cached == nil {
		observability.RecordExternalCacheMiss(modelID)
		return 0, 0, fmt.Errorf("%w: %s", ErrExternalNotInitialized, modelID)
	}

	observability.RecordModelBounds(modelID, cached.Min, cached.Max)

	minPos, maxPos = e.applyLag(model, cached.Min, cached.Max)
	return minPos, maxPos, nil
}
