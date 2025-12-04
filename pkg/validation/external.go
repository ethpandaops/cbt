// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

var (
	// ErrInvalidUint64 is returned when a value cannot be unmarshaled as uint64
	ErrInvalidUint64 = errors.New("failed to unmarshal value as uint64")
	// ErrExternalNotInitialized is returned when external model bounds cache is not yet populated
	ErrExternalNotInitialized = errors.New("external model bounds not initialized")
)

// FlexUint64 is a custom type that can unmarshal from both string and number JSON values
// and scan from string or numeric database columns
type FlexUint64 uint64

// UnmarshalJSON implements json.Unmarshaler for FlexUint64
func (f *FlexUint64) UnmarshalJSON(data []byte) error {
	s := string(data)

	// Check for null value first
	if s == "null" {
		return fmt.Errorf("%w: received null value, which likely indicates missing data", ErrInvalidUint64)
	}

	// Remove quotes if present (handles both "123" and 123)
	s = strings.Trim(s, `"`)

	// Parse as uint64
	parsed, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: failed to parse %q as uint64: %w", ErrInvalidUint64, s, err)
	}

	*f = FlexUint64(parsed)

	return nil
}

// Scan implements sql.Scanner for FlexUint64 to handle database values.
// Supports string columns (e.g., '123') and numeric columns.
//
//nolint:gosec // bounds checked by caller context
func (f *FlexUint64) Scan(src interface{}) error {
	if src == nil {
		return fmt.Errorf("%w: received nil value", ErrInvalidUint64)
	}

	switch v := src.(type) {
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("%w: failed to parse %q: %w", ErrInvalidUint64, v, err)
		}
		*f = FlexUint64(parsed)
	case []byte:
		parsed, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return fmt.Errorf("%w: failed to parse %q: %w", ErrInvalidUint64, v, err)
		}
		*f = FlexUint64(parsed)
	case int64:
		*f = FlexUint64(v)
	case uint64:
		*f = FlexUint64(v)
	case int:
		*f = FlexUint64(v)
	case int8:
		*f = FlexUint64(v)
	case int16:
		*f = FlexUint64(v)
	case int32:
		*f = FlexUint64(v)
	case uint8:
		*f = FlexUint64(v)
	case uint16:
		*f = FlexUint64(v)
	case uint32:
		*f = FlexUint64(v)
	case float64:
		*f = FlexUint64(v)
	default:
		return fmt.Errorf("%w: unsupported type %T", ErrInvalidUint64, src)
	}
	return nil
}

// ExternalModelValidator implements the ExternalValidator interface
type ExternalModelValidator struct {
	log   logrus.FieldLogger
	admin admin.Service
}

// NewExternalModelExecutor creates a new external model executor
func NewExternalModelExecutor(log logrus.FieldLogger, adminService admin.Service) *ExternalModelValidator {
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
