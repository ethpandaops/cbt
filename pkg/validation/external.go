// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"errors"
	"fmt"
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
)

// ExternalModelValidator implements the ExternalModelExecutor interface
type ExternalModelValidator struct {
	log      *logrus.Logger
	admin    *admin.Service
	chClient clickhouse.ClientInterface
	models   *models.Service
}

// NewExternalModelExecutor creates a new external model executor
// The cacheManager can be nil if caching is not desired
func NewExternalModelExecutor(log *logrus.Logger, chClient clickhouse.ClientInterface, adminService *admin.Service, modelsService *models.Service) *ExternalModelValidator {
	return &ExternalModelValidator{
		chClient: chClient,
		log:      log,
		admin:    adminService,
		models:   modelsService,
	}
}

// applyLag applies lag adjustment to max position if configured
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
	modelConfig := model.GetConfig()

	if modelConfig.TTL == nil || *modelConfig.TTL == 0 {
		return 0, 0, false
	}

	cached, err := e.admin.GetCacheManager().GetExternal(ctx, model.GetID())
	if err != nil || cached == nil {
		observability.RecordExternalCacheMiss(model.GetID())
		return 0, 0, false
	}

	return cached.Min, cached.Max, true
}

// storeInCache stores bounds in cache if TTL is configured
func (e *ExternalModelValidator) storeInCache(ctx context.Context, model models.External, minPos, maxPos uint64) error {
	modelConfig := model.GetConfig()

	if modelConfig.TTL == nil || *modelConfig.TTL == 0 {
		return nil
	}

	cache := admin.CacheExternal{
		ModelID:   model.GetID(),
		Min:       minPos,
		Max:       maxPos,
		UpdatedAt: time.Now(),
		TTL:       *modelConfig.TTL,
	}

	err := e.admin.GetCacheManager().SetExternal(ctx, cache)
	if err != nil {
		e.log.WithError(err).WithField("model", model.GetID()).Warn("Failed to cache external model bounds")

		return err
	}

	e.log.WithFields(logrus.Fields{
		"model": model.GetID(),
		"ttl":   modelConfig.TTL,
	}).Debug("Cached external model bounds")

	return nil
}

// GetMinMax retrieves the min and max position values for an external model
func (e *ExternalModelValidator) GetMinMax(ctx context.Context, model models.External) (minPos, maxPos uint64, err error) {
	modelID := model.GetID()

	// Try to get from cache
	if cachedMin, cachedMax, found := e.tryGetFromCache(ctx, model); found {
		minPos, maxPos = e.applyLag(model, cachedMin, cachedMax, true)
		return minPos, maxPos, nil
	}

	if model.GetType() != external.ExternalTypeSQL {
		return 0, 0, fmt.Errorf("%w: %s", ErrNotSQLModel, modelID)
	}

	query, err := e.models.RenderExternal(model)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to render external model %s: %w", modelID, err)
	}

	var result struct {
		MinPos uint64 `json:"min"`
		MaxPos uint64 `json:"max"`
	}

	// trim `;` and whitespace/newlines from end of query
	query = strings.TrimSpace(strings.TrimSuffix(query, ";"))

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		return 0, 0, fmt.Errorf("failed to get min/max for external model %s.%s: %w",
			model.GetConfig().Database, model.GetConfig().Table, err)
	}

	// Store in cache (store original values before lag adjustment)
	if err := e.storeInCache(ctx, model, result.MinPos, result.MaxPos); err != nil {
		// Log error but don't fail the operation - cache is not critical
		e.log.WithError(err).WithField("model_id", model.GetID()).Debug("Failed to store in cache")
	}

	// Apply lag if configured
	minPos, maxPos = e.applyLag(model, result.MinPos, result.MaxPos, false)
	return minPos, maxPos, nil
}
