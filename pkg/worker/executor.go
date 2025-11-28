// Package worker implements the worker functionality for CBT
package worker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
)

// Define static errors
var (
	ErrInvalidTaskContext        = errors.New("invalid task context type")
	ErrInvalidTransformationType = errors.New("invalid transformation type")
	ErrTableDoesNotExist         = errors.New("table does not exist")
)

const (
	// ScanTypeIncremental is the incremental scan type
	ScanTypeIncremental = "incremental"
	// ScanTypeFull is the full scan type
	ScanTypeFull = "full"
)

// ModelExecutor implements the execution of model transformations
type ModelExecutor struct {
	log      logrus.FieldLogger
	chClient clickhouse.ClientInterface
	models   models.Service
	admin    admin.Service
}

// NewModelExecutor creates a new model executor
func NewModelExecutor(log logrus.FieldLogger, chClient clickhouse.ClientInterface, modelsService models.Service, adminManager admin.Service) *ModelExecutor {
	return &ModelExecutor{
		log:      log,
		chClient: chClient,
		models:   modelsService,
		admin:    adminManager,
	}
}

// UpdateBounds updates the external model bounds cache
func (e *ModelExecutor) UpdateBounds(ctx context.Context, modelID, scanType string) error {
	// Get the external model from DAG
	externalModel, err := e.models.GetDAG().GetExternalNode(modelID)
	if err != nil {
		e.log.WithError(err).WithField("model_id", modelID).Error("Failed to get external model")
		observability.RecordError("bounds-handler", "model_not_found")
		return fmt.Errorf("failed to get external model %s: %w", modelID, err)
	}

	// Get current cache entry
	cache, err := e.admin.GetExternalBounds(ctx, modelID)
	if err != nil {
		e.log.WithError(err).WithField("model_id", modelID).Warn("Failed to get cache bounds")
	}

	now := time.Now().UTC()

	// Validate scan preconditions
	if shouldSkipScan(e.log, modelID, scanType, cache, now) {
		return nil
	}

	// Build cache state for template rendering
	cacheState := buildCacheState(scanType, cache)

	e.log.WithFields(logrus.Fields{
		"model_id":            modelID,
		"scan_type":           scanType,
		"is_incremental_scan": cacheState["is_incremental_scan"],
		"is_full_scan":        cacheState["is_full_scan"],
		"cache_state":         cacheState,
	}).Debug("Processing external scan")

	// Query bounds with cache state
	minBound, maxBound, err := e.queryExternalBounds(ctx, modelID, externalModel, cacheState)
	if err != nil {
		return fmt.Errorf("failed to query external bounds for %s: %w", modelID, err)
	}

	// Handle no new data case for incremental scans
	isIncrementalScan := scanType == ScanTypeIncremental
	isFullScan := scanType == ScanTypeFull

	if isIncrementalScan && minBound == 0 && maxBound == 0 && cache != nil {
		// No new data found, keep existing bounds
		e.log.WithField("model_id", modelID).Debug("No new data found in incremental scan, keeping existing bounds")
		minBound = cache.Min
		maxBound = cache.Max
	}

	// Store raw bounds in cache without applying lag
	// Lag will be applied consistently by the validator when reading bounds
	if err := e.updateBoundsCache(ctx, modelID, minBound, maxBound, cache, isIncrementalScan, isFullScan, now); err != nil {
		return fmt.Errorf("failed to update bounds cache for %s: %w", modelID, err)
	}

	// Record the bounds in metrics
	observability.RecordModelBounds(modelID, minBound, maxBound)

	return nil
}

// shouldSkipScan checks if the scan should be skipped based on current state
func shouldSkipScan(log logrus.FieldLogger, modelID, scanType string, cache *admin.BoundsCache, now time.Time) bool {
	// Check if this is initial scan
	if scanType == ScanTypeIncremental && cache == nil {
		log.WithField("model_id", modelID).Warn("No cache for incremental scan, skipping until full scan completes")
		return true
	}

	// Check if initial scan is complete for incremental scans
	if scanType == ScanTypeIncremental && cache != nil && !cache.InitialScanComplete {
		// Check if initial scan is stuck (more than 30 minutes old)
		if cache.InitialScanStarted != nil && now.Sub(*cache.InitialScanStarted) > 30*time.Minute {
			log.WithField("model_id", modelID).Warn("Initial scan appears stuck, will retry on next full scan")
		}
		return true
	}

	return false
}

// buildCacheState builds the cache state for template rendering
func buildCacheState(scanType string, cache *admin.BoundsCache) map[string]interface{} {
	// Set scan type flags
	isIncrementalScan := scanType == ScanTypeIncremental
	isFullScan := scanType == ScanTypeFull

	// Build cache state for template rendering
	cacheState := map[string]interface{}{
		"is_incremental_scan": isIncrementalScan,
		"is_full_scan":        isFullScan,
	}

	// Add previous bounds if available
	if cache != nil {
		cacheState["previous_min"] = cache.PreviousMin
		cacheState["previous_max"] = cache.PreviousMax
	}

	return cacheState
}

// queryExternalBounds executes the query to get bounds for an external model
func (e *ModelExecutor) queryExternalBounds(ctx context.Context, modelID string, externalModel models.External, cacheState map[string]interface{}) (minBound, maxBound uint64, err error) {
	// Render the external model query with cache state
	query, err := e.models.RenderExternal(externalModel, cacheState)
	if err != nil {
		e.log.WithError(err).WithFields(logrus.Fields{
			"model_id":    modelID,
			"cache_state": cacheState,
		}).Error("Failed to render external model query")
		observability.RecordError("bounds-handler", "render_error")
		return 0, 0, fmt.Errorf("failed to render external model %s: %w", modelID, err)
	}

	// Execute the query to get bounds
	var result struct {
		Min validation.FlexUint64 `json:"min"`
		Max validation.FlexUint64 `json:"max"`
	}

	e.log.WithFields(logrus.Fields{
		"model_id":    modelID,
		"cache_state": cacheState,
		"query":       query,
	}).Debug("Executing bounds query")

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		e.log.WithError(err).WithFields(logrus.Fields{
			"model_id":    modelID,
			"cache_state": cacheState,
			"query":       query,
		}).Error("Failed to query bounds")
		observability.RecordError("bounds-handler", "query_error")
		return 0, 0, fmt.Errorf("failed to query bounds for %s: %w", modelID, err)
	}

	e.log.WithFields(logrus.Fields{
		"model_id": modelID,
		"min":      result.Min,
		"max":      result.Max,
	}).Debug("Bounds query successful")

	return uint64(result.Min), uint64(result.Max), nil
}

// updateBoundsCache updates the bounds cache with new values
func (e *ModelExecutor) updateBoundsCache(ctx context.Context, modelID string, minBound, maxBound uint64, existingCache *admin.BoundsCache, isIncrementalScan, isFullScan bool, now time.Time) error {
	// Preserve previous bounds from existing cache, or use new bounds if no cache exists
	prevMin := minBound
	prevMax := maxBound
	if existingCache != nil {
		prevMin = existingCache.Min
		prevMax = existingCache.Max
	}

	newCache := &admin.BoundsCache{
		ModelID:             modelID,
		Min:                 minBound,
		Max:                 maxBound,
		PreviousMin:         prevMin,
		PreviousMax:         prevMax,
		UpdatedAt:           now,
		InitialScanComplete: true, // Set to true after any successful scan
	}

	// Update cache timestamps based on scan type
	updateCacheTimestamps(newCache, existingCache, isIncrementalScan, isFullScan, now)

	if err := e.admin.SetExternalBounds(ctx, newCache); err != nil {
		e.log.WithError(err).WithField("model_id", modelID).Error("Failed to update cache")
		observability.RecordError("bounds-handler", "cache_update_error")
		return fmt.Errorf("failed to update cache for %s: %w", modelID, err)
	}

	return nil
}

// updateCacheTimestamps updates the cache timestamps based on scan type
func updateCacheTimestamps(newCache, existingCache *admin.BoundsCache, isIncrementalScan, isFullScan bool, now time.Time) {
	switch {
	case isIncrementalScan:
		newCache.LastIncrementalScan = now
		// Keep the previous full scan time and initial scan info
		if existingCache != nil {
			newCache.LastFullScan = existingCache.LastFullScan
			newCache.InitialScanStarted = existingCache.InitialScanStarted
		}
	case isFullScan:
		newCache.LastFullScan = now
		newCache.LastIncrementalScan = now
		// Set initial scan started time if this is the first scan
		if existingCache == nil {
			newCache.InitialScanStarted = &now
			newCache.InitialScanComplete = false // Will be set to true when scan completes
		} else if existingCache.InitialScanStarted != nil {
			newCache.InitialScanStarted = existingCache.InitialScanStarted
		}
	}
}

// Execute runs the model transformation
func (e *ModelExecutor) Execute(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}

	// Validate first
	if err := e.Validate(ctx, taskCtx); err != nil {
		return err
	}

	config := taskCtx.Transformation.GetConfig()

	e.log.WithFields(logrus.Fields{
		"model_id": fmt.Sprintf("%s.%s", config.Database, config.Table),
		"position": taskCtx.Position,
		"interval": taskCtx.Interval,
	}).Info("Executing model transformation")

	switch taskCtx.Transformation.GetType() {
	case transformation.TransformationTypeExec:
		if err := e.executeCommand(ctx, taskCtx); err != nil {
			return err
		}
	case transformation.TransformationTypeSQL:
		if err := e.executeSQL(ctx, taskCtx); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: %s", ErrInvalidTransformationType, taskCtx.Transformation.GetType())
	}

	return nil
}

// Validate checks if the model can be executed
func (e *ModelExecutor) Validate(ctx context.Context, taskCtxInterface interface{}) error {
	taskCtx, ok := taskCtxInterface.(*tasks.TaskContext)
	if !ok {
		return ErrInvalidTaskContext
	}

	config := taskCtx.Transformation.GetConfig()

	exists, err := clickhouse.TableExists(ctx, e.chClient, config.Database, config.Table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("%w: %s.%s", ErrTableDoesNotExist, config.Database, config.Table)
	}

	return nil
}

func (e *ModelExecutor) executeSQL(ctx context.Context, taskCtx *tasks.TaskContext) error {
	config := taskCtx.Transformation.GetConfig()

	renderedSQL, err := e.models.RenderTransformation(taskCtx.Transformation, taskCtx.Position, taskCtx.Interval, taskCtx.ExecutionTime)
	if err != nil {
		return fmt.Errorf("failed to render SQL template: %w", err)
	}

	// Simple split by semicolon
	statements := strings.Split(renderedSQL, ";")

	e.log.WithFields(logrus.Fields{
		"count":    len(statements),
		"model_id": fmt.Sprintf("%s.%s", config.Database, config.Table),
	}).Info("Split SQL into statements")

	// Execute each statement
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Log first 500 chars of SQL for visibility
		logSQL := stmt
		if len(stmt) > 500 {
			logSQL = stmt[:500] + "..."
		}

		e.log.WithFields(logrus.Fields{
			"statement_num": i + 1,
			"total":         len(statements),
			"model_id":      fmt.Sprintf("%s.%s", config.Database, config.Table),
			"sql_preview":   logSQL,
		}).Info("Executing SQL statement")

		if _, err := e.chClient.Execute(ctx, stmt); err != nil {
			e.log.WithFields(logrus.Fields{
				"statement": i + 1,
				"sql":       logSQL,
				"error":     err.Error(),
			}).Error("SQL execution failed")
			return fmt.Errorf("failed to execute statement %d: %w", i+1, err)
		}
	}

	e.log.WithFields(logrus.Fields{
		"model_id":   fmt.Sprintf("%s.%s", config.Database, config.Table),
		"position":   taskCtx.Position,
		"interval":   taskCtx.Interval,
		"statements": len(statements),
	}).Info("Model transformation completed successfully")

	return nil
}

func (e *ModelExecutor) executeCommand(ctx context.Context, taskCtx *tasks.TaskContext) error {
	config := taskCtx.Transformation.GetConfig()
	command := taskCtx.Transformation.GetValue()

	env, err := e.models.GetTransformationEnvironmentVariables(taskCtx.Transformation, taskCtx.Position, taskCtx.Interval, taskCtx.ExecutionTime)
	if err != nil {
		return fmt.Errorf("failed to render SQL template: %w", err)
	}

	// Execute command
	// #nosec G204 -- Model exec commands are defined by trusted model files
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Env = append(cmd.Env, *env...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		e.log.WithFields(logrus.Fields{
			"command": command,
			"output":  string(output),
			"error":   err,
		}).Error("Command execution failed")
		return fmt.Errorf("command execution failed: %w", err)
	}

	e.log.WithFields(logrus.Fields{
		"model_id": fmt.Sprintf("%s.%s", config.Database, config.Table),
		"position": taskCtx.Position,
		"interval": taskCtx.Interval,
	}).Info("Model command executed successfully")

	return nil
}
