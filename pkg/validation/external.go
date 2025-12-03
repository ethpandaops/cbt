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

// Scan implements sql.Scanner for FlexUint64 to handle database values
// This allows scanning from string columns (e.g., when SQL returns '123' as min)
// as well as numeric columns
func (f *FlexUint64) Scan(src interface{}) error {
	if src == nil {
		return fmt.Errorf("%w: received nil value", ErrInvalidUint64)
	}

	switch v := src.(type) {
	case string:
		return f.scanString(v)
	case []byte:
		return f.scanString(string(v))
	default:
		return f.scanNumeric(src)
	}
}

// scanString parses a string value as uint64
func (f *FlexUint64) scanString(s string) error {
	parsed, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: failed to parse string %q as uint64: %w", ErrInvalidUint64, s, err)
	}

	*f = FlexUint64(parsed)

	return nil
}

// scanNumeric handles all numeric types (int8-64, uint8-64, float64)
//
//nolint:gosec // bounds checked by caller context
func (f *FlexUint64) scanNumeric(src interface{}) error {
	switch v := src.(type) {
	case int64:
		*f = FlexUint64(v)
	case uint64:
		*f = FlexUint64(v)
	case int:
		*f = FlexUint64(v)
	case int8, int16, int32:
		// Use type switch to convert smaller signed ints
		switch n := v.(type) {
		case int8:
			*f = FlexUint64(n)
		case int16:
			*f = FlexUint64(n)
		case int32:
			*f = FlexUint64(n)
		}
	case uint8, uint16, uint32:
		// Use type switch to convert smaller unsigned ints
		switch n := v.(type) {
		case uint8:
			*f = FlexUint64(n)
		case uint16:
			*f = FlexUint64(n)
		case uint32:
			*f = FlexUint64(n)
		}
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
// Note: Bounds stored in cache are always raw (without lag applied)
func (e *ExternalModelValidator) applyLag(model models.External, minPos, maxPos uint64, fromCache bool) (adjustedMin, adjustedMax uint64) {
	modelConfig := model.GetConfig()

	if modelConfig.Lag == 0 {
		e.log.WithFields(logrus.Fields{
			"model":     model.GetID(),
			"min_pos":   minPos,
			"max_pos":   maxPos,
			"cache_hit": fromCache,
		}).Debug("Retrieved external model bounds")

		return minPos, maxPos
	}

	if maxPos > modelConfig.Lag {
		adjustedMax := maxPos - modelConfig.Lag
		e.log.WithFields(logrus.Fields{
			"model":        model.GetID(),
			"lag":          modelConfig.Lag,
			"original_max": maxPos,
			"adjusted_max": adjustedMax,
			"cache_hit":    fromCache,
		}).Debug("Applied lag to external model bounds")

		return minPos, adjustedMax
	}

	// If lag is greater than max, set max to min (no data available)
	e.log.WithFields(logrus.Fields{
		"model":     model.GetID(),
		"lag":       modelConfig.Lag,
		"cache_hit": fromCache,
		"min_pos":   minPos,
		"max_pos":   maxPos,
		"warning":   "lag exceeds max position, no data available",
	}).Warn("Lag exceeds available data range")

	return minPos, minPos
}

// tryGetFromCache attempts to retrieve bounds from cache
func (e *ExternalModelValidator) tryGetFromCache(ctx context.Context, model models.External) (minPos, maxPos uint64, found bool) {
	cached, err := e.admin.GetExternalBounds(ctx, model.GetID())
	if err != nil || cached == nil {
		observability.RecordExternalCacheMiss(model.GetID())
		return 0, 0, false
	}

	// Cache hit - no need to query database
	return cached.Min, cached.Max, true
}

// GetMinMax retrieves the min and max position values for an external model
func (e *ExternalModelValidator) GetMinMax(ctx context.Context, model models.External) (minPos, maxPos uint64, err error) {
	modelID := model.GetID()

	// Try to get from cache
	cachedMin, cachedMax, found := e.tryGetFromCache(ctx, model)
	if !found {
		// No cache - wait for executor's scheduled full scan to populate it
		return 0, 0, fmt.Errorf("%w: %s (waiting for initial scan)", ErrExternalNotInitialized, modelID)
	}

	// Record the raw bounds in metrics
	observability.RecordModelBounds(modelID, cachedMin, cachedMax)

	// Apply lag if configured
	minPos, maxPos = e.applyLag(model, cachedMin, cachedMax, true)

	return minPos, maxPos, nil
}
