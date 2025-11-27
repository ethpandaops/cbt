// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

var (
	// ErrNotSQLModel is returned when external model is not a SQL model
	ErrNotSQLModel = errors.New("external model is not a SQL model")
	// ErrInvalidUint64 is returned when a value cannot be unmarshaled as uint64
	ErrInvalidUint64 = errors.New("failed to unmarshal value as uint64")
)

// FlexUint64 is a custom type that can unmarshal from both string and number JSON values
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

// ExternalModelValidator implements the ExternalModelExecutor interface
type ExternalModelValidator struct {
	log      logrus.FieldLogger
	admin    admin.Service
	chClient clickhouse.ClientInterface
	models   models.Service
}

// NewExternalModelExecutor creates a new external model executor
// The cacheManager can be nil if caching is not desired
func NewExternalModelExecutor(log logrus.FieldLogger, chClient clickhouse.ClientInterface, adminService admin.Service, modelsService models.Service) *ExternalModelValidator {
	return &ExternalModelValidator{
		chClient: chClient,
		log:      log,
		admin:    adminService,
		models:   modelsService,
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

// storeInCache stores bounds in cache (persistent, no TTL)
func (e *ExternalModelValidator) storeInCache(ctx context.Context, model models.External, minPos, maxPos uint64) error {
	cache := &admin.BoundsCache{
		ModelID:   model.GetID(),
		Min:       minPos,
		Max:       maxPos,
		UpdatedAt: time.Now(),
		// Note: LastIncrementalScan and LastFullScan will be set by the bounds update task
	}

	err := e.admin.SetExternalBounds(ctx, cache)
	if err != nil {
		e.log.WithError(err).WithField("model", model.GetID()).Warn("Failed to cache external model bounds")
		return err
	}

	e.log.WithField("model", model.GetID()).Debug("Cached external model bounds")
	return nil
}

// GetMinMax retrieves the min and max position values for an external model
func (e *ExternalModelValidator) GetMinMax(ctx context.Context, model models.External) (minPos, maxPos uint64, err error) {
	modelID := model.GetID()

	// Try to get from cache
	if cachedMin, cachedMax, found := e.tryGetFromCache(ctx, model); found {
		// Record the raw bounds in metrics
		observability.RecordModelBounds(modelID, cachedMin, cachedMax)
		minPos, maxPos = e.applyLag(model, cachedMin, cachedMax, true)
		return minPos, maxPos, nil
	}

	if model.GetType() != external.ExternalTypeSQL {
		return 0, 0, fmt.Errorf("%w: %s", ErrNotSQLModel, modelID)
	}

	// Pass nil cache state for validation queries (not checking bounds)
	query, err := e.models.RenderExternal(model, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to render external model %s: %w", modelID, err)
	}

	var result struct {
		MinPos FlexUint64 `json:"min"`
		MaxPos FlexUint64 `json:"max"`
	}

	// Split by semicolon and take the first query statement
	// This handles cases where there might be newlines or multiple statements
	parts := strings.Split(query, ";")
	if len(parts) > 0 {
		query = strings.TrimSpace(parts[0])
	}

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		return 0, 0, fmt.Errorf("failed to get min/max for external model %s.%s: %w",
			model.GetConfig().Database, model.GetConfig().Table, err)
	}

	// Store in cache (store original values before lag adjustment)
	if err := e.storeInCache(ctx, model, uint64(result.MinPos), uint64(result.MaxPos)); err != nil {
		// Log error but don't fail the operation - cache is not critical
		e.log.WithError(err).WithField("model_id", model.GetID()).Debug("Failed to store in cache")
	}

	// Record the raw bounds in metrics
	observability.RecordModelBounds(modelID, uint64(result.MinPos), uint64(result.MaxPos))

	// Apply lag if configured
	minPos, maxPos = e.applyLag(model, uint64(result.MinPos), uint64(result.MaxPos), false)
	return minPos, maxPos, nil
}
