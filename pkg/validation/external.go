// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/sirupsen/logrus"
)

// ExternalModelExecutorImpl implements the ExternalModelExecutor interface
type ExternalModelExecutorImpl struct {
	chClient     clickhouse.ClientInterface
	logger       *logrus.Logger
	cacheManager clickhouse.ExternalModelCacheManager
}

// NewExternalModelExecutor creates a new external model executor
// The cacheManager can be nil if caching is not desired
func NewExternalModelExecutor(chClient clickhouse.ClientInterface, logger *logrus.Logger, cacheManager clickhouse.ExternalModelCacheManager) *ExternalModelExecutorImpl {
	return &ExternalModelExecutorImpl{
		chClient:     chClient,
		logger:       logger,
		cacheManager: cacheManager,
	}
}

// applyLag applies lag adjustment to max position if configured
func (e *ExternalModelExecutorImpl) applyLag(modelConfig *models.ModelConfig, minPos, maxPos uint64, modelID string, fromCache bool) (adjustedMin, adjustedMax uint64) {
	if !modelConfig.External || modelConfig.Lag == 0 {
		e.logger.WithFields(logrus.Fields{
			"model":     modelID,
			"min_pos":   minPos,
			"max_pos":   maxPos,
			"cache_hit": fromCache,
		}).Debug("Retrieved external model bounds")
		return minPos, maxPos
	}

	if maxPos > modelConfig.Lag {
		adjustedMax := maxPos - modelConfig.Lag
		e.logger.WithFields(logrus.Fields{
			"model":        modelID,
			"lag":          modelConfig.Lag,
			"original_max": maxPos,
			"adjusted_max": adjustedMax,
			"cache_hit":    fromCache,
		}).Debug("Applied lag to external model bounds")
		return minPos, adjustedMax
	}

	// If lag is greater than max, set max to min (no data available)
	e.logger.WithFields(logrus.Fields{
		"model":     modelID,
		"lag":       modelConfig.Lag,
		"cache_hit": fromCache,
		"warning":   "lag exceeds max position, no data available",
	}).Warn("Lag exceeds available data range")
	return minPos, minPos
}

// tryGetFromCache attempts to retrieve bounds from cache
func (e *ExternalModelExecutorImpl) tryGetFromCache(ctx context.Context, modelConfig *models.ModelConfig, modelID string) (minPos, maxPos uint64, found bool) {
	if modelConfig.TTL == 0 || e.cacheManager == nil {
		return 0, 0, false
	}

	cached, err := e.cacheManager.Get(ctx, modelID)
	if err != nil || cached == nil {
		observability.RecordExternalCacheMiss(modelID)
		return 0, 0, false
	}

	observability.RecordExternalCacheHit(modelID)
	return cached.Min, cached.Max, true
}

// storeInCache stores bounds in cache if TTL is configured
func (e *ExternalModelExecutorImpl) storeInCache(ctx context.Context, modelConfig *models.ModelConfig, modelID string, minPos, maxPos uint64) {
	if modelConfig.TTL == 0 || e.cacheManager == nil {
		return
	}

	cache := clickhouse.ExternalModelCache{
		ModelID:   modelID,
		Min:       minPos,
		Max:       maxPos,
		UpdatedAt: time.Now(),
		TTL:       modelConfig.TTL,
	}

	if err := e.cacheManager.Set(ctx, cache); err != nil {
		e.logger.WithError(err).WithField("model", modelID).Warn("Failed to cache external model bounds")
	} else {
		e.logger.WithFields(logrus.Fields{
			"model": modelID,
			"ttl":   modelConfig.TTL,
		}).Debug("Cached external model bounds")
	}
}

// GetMinMax retrieves the min and max position values for an external model
func (e *ExternalModelExecutorImpl) GetMinMax(ctx context.Context, modelConfig *models.ModelConfig) (minPos, maxPos uint64, err error) {
	modelID := fmt.Sprintf("%s.%s", modelConfig.Database, modelConfig.Table)

	// Try to get from cache
	if cachedMin, cachedMax, found := e.tryGetFromCache(ctx, modelConfig, modelID); found {
		minPos, maxPos = e.applyLag(modelConfig, cachedMin, cachedMax, modelID, true)
		return minPos, maxPos, nil
	}

	// Build the query to get min/max from the external table
	// Convert DateTime to Unix timestamp if needed
	query := fmt.Sprintf(`
		SELECT 
			coalesce(toUnixTimestamp(min(%s)), 0) as min_pos,
			coalesce(toUnixTimestamp(max(%s)), 0) as max_pos
		FROM %s.%s
	`, modelConfig.Partition, modelConfig.Partition, modelConfig.Database, modelConfig.Table)

	var result struct {
		MinPos uint64 `json:"min_pos"`
		MaxPos uint64 `json:"max_pos"`
	}

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		return 0, 0, fmt.Errorf("failed to get min/max for external model %s.%s: %w",
			modelConfig.Database, modelConfig.Table, err)
	}

	// Store in cache (store original values before lag adjustment)
	e.storeInCache(ctx, modelConfig, modelID, result.MinPos, result.MaxPos)

	// Apply lag if configured
	minPos, maxPos = e.applyLag(modelConfig, result.MinPos, result.MaxPos, modelID, false)
	return minPos, maxPos, nil
}

// InvalidateCache removes cached bounds for a specific model
func (e *ExternalModelExecutorImpl) InvalidateCache(ctx context.Context, modelID string) error {
	if e.cacheManager == nil {
		return nil // No cache manager, nothing to invalidate
	}

	err := e.cacheManager.Invalidate(ctx, modelID)
	if err != nil {
		e.logger.WithError(err).WithField("model", modelID).Warn("Failed to invalidate cache")
		return fmt.Errorf("failed to invalidate cache for %s: %w", modelID, err)
	}

	e.logger.WithField("model", modelID).Info("Invalidated cache for external model")
	return nil
}

// InvalidateCacheForModel removes cached bounds for a model config
func (e *ExternalModelExecutorImpl) InvalidateCacheForModel(ctx context.Context, modelConfig *models.ModelConfig) error {
	modelID := fmt.Sprintf("%s.%s", modelConfig.Database, modelConfig.Table)
	return e.InvalidateCache(ctx, modelID)
}

// HasDataInRange checks if an external model has any data in the specified range
func (e *ExternalModelExecutorImpl) HasDataInRange(ctx context.Context, modelConfig *models.ModelConfig, startPos, endPos uint64) (bool, error) {
	// Check if ANY data exists in the specified range
	query := fmt.Sprintf(`
		SELECT count() > 0 as has_data
		FROM %s.%s
		WHERE toUnixTimestamp(%s) >= %d
		  AND toUnixTimestamp(%s) < %d
		LIMIT 1
	`, modelConfig.Database, modelConfig.Table,
		modelConfig.Partition, startPos,
		modelConfig.Partition, endPos)

	var result struct {
		HasData int `json:"has_data"`
	}

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		return false, fmt.Errorf("failed to check data existence: %w", err)
	}

	return result.HasData > 0, nil
}
