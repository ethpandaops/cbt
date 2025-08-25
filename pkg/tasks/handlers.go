// Package tasks provides task handling and execution functionality
package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

var (
	// ErrModelConfigNotFound is returned when model configuration is not found
	ErrModelConfigNotFound = errors.New("model configuration not found")
	// ErrDependenciesNotSatisfied is returned when dependencies are not satisfied
	ErrDependenciesNotSatisfied = errors.New("dependencies not satisfied")
	// ErrModelIDNotFound is returned when model_id is not found in payload
	ErrModelIDNotFound = errors.New("model_id not found in payload")
	// ErrCacheManagerUnavailable is returned when cache manager is not available
	ErrCacheManagerUnavailable = errors.New("cache manager not available")
)

// getWorkerID returns the worker ID based on hostname
func getWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "worker-unknown"
	}
	return hostname
}

// TaskHandler handles task execution
type TaskHandler struct {
	log             logrus.FieldLogger
	chClient        clickhouse.ClientInterface
	admin           admin.Service
	validator       validation.Validator
	modelExecutor   Executor
	transformations map[string]models.Transformation
	modelsService   models.Service
}

// TaskContext contains all context needed for task execution
type TaskContext struct {
	Transformation models.Transformation
	Position       uint64
	Interval       uint64
	StartTime      time.Time
	Variables      map[string]interface{}
}

// Executor defines the interface for task executors
type Executor interface {
	Execute(ctx context.Context, taskCtx interface{}) error
	Validate(ctx context.Context, taskCtx interface{}) error
}

// NewTaskHandler creates a new task handler
func NewTaskHandler(
	logger logrus.FieldLogger,
	chClient clickhouse.ClientInterface,
	adminService admin.Service,
	validator validation.Validator,
	modelExecutor Executor,
	transformations []models.Transformation,
	modelsService models.Service,
) *TaskHandler {
	transformationsMap := make(map[string]models.Transformation, len(transformations)) // Add capacity hint
	for _, transformation := range transformations {
		transformationsMap[transformation.GetID()] = transformation
	}

	return &TaskHandler{
		log:             logger,
		chClient:        chClient,
		admin:           adminService,
		validator:       validator,
		modelExecutor:   modelExecutor,
		transformations: transformationsMap,
		modelsService:   modelsService,
	}
}

// HandleTransformation handles transformation tasks
func (h *TaskHandler) HandleTransformation(ctx context.Context, t *asynq.Task) error {
	var payload TaskPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		observability.RecordError("task-handler", "unmarshal_error")
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	h.log.WithFields(logrus.Fields{
		"model_id": payload.ModelID,
		"position": payload.Position,
		"interval": payload.Interval,
	}).Info("Starting transformation task")

	startTime := time.Now()

	// Get worker ID from hostname or use default
	workerID := getWorkerID()

	// Record task start
	observability.RecordTaskStart(payload.ModelID, workerID)

	// Get model configuration
	transformation, exists := h.transformations[payload.ModelID]
	if !exists {
		observability.RecordTaskComplete(payload.ModelID, workerID, "failed", time.Since(startTime).Seconds())
		observability.RecordError("task-handler", "model_not_found")
		return ErrModelConfigNotFound
	}

	// Validate dependencies
	h.log.WithField("model_id", payload.ModelID).Info("Validating dependencies")
	depStartTime := time.Now()
	validationResult, err := h.validator.ValidateDependencies(ctx, payload.ModelID, payload.Position, payload.Interval)
	depDuration := time.Since(depStartTime).Seconds()

	if err != nil {
		h.log.WithError(err).Error("Dependency validation error")
		observability.RecordDependencyValidation(payload.ModelID, "error", depDuration)
		observability.RecordTaskComplete(payload.ModelID, workerID, "failed", time.Since(startTime).Seconds())
		observability.RecordError("task-handler", "dependency_validation_error")
		return fmt.Errorf("dependency validation error: %w", err)
	}

	h.log.WithFields(logrus.Fields{
		"model_id":    payload.ModelID,
		"can_process": validationResult.CanProcess,
	}).Info("Validation result")

	if !validationResult.CanProcess {
		h.log.WithField("model_id", payload.ModelID).Warn("Dependencies not satisfied")
		observability.RecordDependencyValidation(payload.ModelID, "not_satisfied", depDuration)
		observability.RecordTaskComplete(payload.ModelID, workerID, "failed", time.Since(startTime).Seconds())
		return ErrDependenciesNotSatisfied
	}

	observability.RecordDependencyValidation(payload.ModelID, "satisfied", depDuration)

	h.log.WithField("model_id", payload.ModelID).Info("Dependencies satisfied, executing transformation")

	// Execute transformation
	taskCtx := &TaskContext{
		Transformation: transformation,
		Position:       payload.Position,
		Interval:       payload.Interval,
		StartTime:      startTime,
	}

	h.log.WithField("model_id", payload.ModelID).Info("Calling modelExecutor.Execute")
	if err := h.modelExecutor.Execute(ctx, taskCtx); err != nil {
		h.log.WithError(err).WithField("model_id", payload.ModelID).Error("Model execution failed")
		observability.RecordTaskComplete(payload.ModelID, workerID, "failed", time.Since(startTime).Seconds())
		observability.RecordError("task-handler", "execution_error")
		return fmt.Errorf("execution error: %w", err)
	}
	h.log.WithField("model_id", payload.ModelID).Info("Model execution completed")

	// Record completion in admin table
	if err := h.admin.RecordCompletion(ctx, payload.ModelID, payload.Position, payload.Interval); err != nil {
		observability.RecordTaskComplete(payload.ModelID, workerID, "failed", time.Since(startTime).Seconds())
		observability.RecordError("task-handler", "record_completion_error")
		return fmt.Errorf("failed to record completion: %w", err)
	}

	// Record successful completion
	observability.RecordTaskComplete(payload.ModelID, workerID, "success", time.Since(startTime).Seconds())

	// Update model position metrics
	observability.ModelLastPosition.WithLabelValues(payload.ModelID).Set(float64(payload.Position))
	// Calculate lag safely
	currentTimeUnix := time.Now().Unix()
	if currentTimeUnix > 0 {
		// Safe conversion: we know currentTimeUnix is positive
		currentTimeUint := uint64(currentTimeUnix) //nolint:gosec // checked positive above
		if currentTimeUint > payload.Position {
			lag := currentTimeUint - payload.Position
			observability.ModelPositionLag.WithLabelValues(payload.ModelID).Set(float64(lag))
		} else {
			observability.ModelPositionLag.WithLabelValues(payload.ModelID).Set(0)
		}
	} else {
		observability.ModelPositionLag.WithLabelValues(payload.ModelID).Set(0)
	}

	h.log.WithFields(logrus.Fields{
		"model_id": payload.ModelID,
		"position": payload.Position,
		"interval": payload.Interval,
		"duration": time.Since(startTime),
	}).Info("Task completed successfully")

	return nil
}

// determineScanType determines whether to perform an incremental or full scan
func (h *TaskHandler) determineScanType(modelID string, cache *admin.BoundsCache, config external.Config, now time.Time) (isIncrementalScan, isFullScan bool) {
	switch {
	case cache == nil:
		// No cache - do full scan
		isFullScan = true
		h.log.WithField("model_id", modelID).Debug("No cache entry, performing initial full scan")
	case config.Cache != nil && now.Sub(cache.LastFullScan) > config.Cache.FullScanInterval:
		// Time for full scan
		isFullScan = true
		h.log.WithFields(logrus.Fields{
			"model_id":       modelID,
			"last_full_scan": cache.LastFullScan,
			"interval":       config.Cache.FullScanInterval,
		}).Debug("Performing periodic full scan")
	default:
		// Incremental scan
		isIncrementalScan = true
		h.log.WithField("model_id", modelID).Debug("Performing incremental scan")
	}
	return isIncrementalScan, isFullScan
}

// queryExternalBounds executes the query to get bounds for an external model
func (h *TaskHandler) queryExternalBounds(ctx context.Context, modelID string, externalModel models.External, cacheState map[string]interface{}) (minBound, maxBound uint64, err error) {
	// Render the external model query with cache state
	query, err := h.modelsService.RenderExternal(externalModel, cacheState)
	if err != nil {
		h.log.WithError(err).WithFields(logrus.Fields{
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

	h.log.WithFields(logrus.Fields{
		"model_id":    modelID,
		"cache_state": cacheState,
		"query":       query,
	}).Debug("Executing bounds query")

	if err := h.chClient.QueryOne(ctx, query, &result); err != nil {
		h.log.WithError(err).WithFields(logrus.Fields{
			"model_id":    modelID,
			"cache_state": cacheState,
			"query":       query,
		}).Error("Failed to query bounds")
		observability.RecordError("bounds-handler", "query_error")
		return 0, 0, fmt.Errorf("failed to query bounds for %s: %w", modelID, err)
	}

	h.log.WithFields(logrus.Fields{
		"model_id": modelID,
		"min":      result.Min,
		"max":      result.Max,
	}).Debug("Bounds query successful")

	return uint64(result.Min), uint64(result.Max), nil
}

// updateBoundsCache updates the bounds cache with new values
func (h *TaskHandler) updateBoundsCache(ctx context.Context, cacheManager *admin.CacheManager, modelID string, minBound, maxBound uint64, existingCache *admin.BoundsCache, isIncrementalScan bool, now time.Time) error {
	newCache := &admin.BoundsCache{
		ModelID:     modelID,
		Min:         minBound,
		Max:         maxBound,
		PreviousMin: minBound,
		PreviousMax: maxBound,
		UpdatedAt:   now,
	}

	if isIncrementalScan {
		newCache.LastIncrementalScan = now
		// Keep the previous full scan time
		if existingCache != nil {
			newCache.LastFullScan = existingCache.LastFullScan
		}
	} else {
		newCache.LastFullScan = now
		newCache.LastIncrementalScan = now
	}

	if err := cacheManager.SetBounds(ctx, newCache); err != nil {
		h.log.WithError(err).WithField("model_id", modelID).Error("Failed to update cache")
		observability.RecordError("bounds-handler", "cache_update_error")
		return fmt.Errorf("failed to update cache for %s: %w", modelID, err)
	}

	return nil
}

// HandleBoundsCache handles bounds cache update tasks for external models
func (h *TaskHandler) HandleBoundsCache(ctx context.Context, t *asynq.Task) error {
	var payload map[string]string
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		observability.RecordError("bounds-handler", "unmarshal_error")
		return fmt.Errorf("failed to unmarshal bounds payload: %w", err)
	}

	modelID, ok := payload["model_id"]
	if !ok {
		observability.RecordError("bounds-handler", "missing_model_id")
		return ErrModelIDNotFound
	}

	h.log.WithField("model_id", modelID).Info("Starting bounds cache update")
	startTime := time.Now()

	// Get the external model from DAG
	dag := h.modelsService.GetDAG()
	externalModel, err := dag.GetExternalNode(modelID)
	if err != nil {
		h.log.WithError(err).WithField("model_id", modelID).Error("Failed to get external model")
		observability.RecordError("bounds-handler", "model_not_found")
		return fmt.Errorf("failed to get external model %s: %w", modelID, err)
	}

	// Get cache manager
	cacheManager := h.admin.GetCacheManager()
	if cacheManager == nil {
		h.log.Error("Cache manager not available")
		observability.RecordError("bounds-handler", "cache_manager_unavailable")
		return ErrCacheManagerUnavailable
	}

	// Get current cache entry to determine scan type
	cache, err := cacheManager.GetBounds(ctx, modelID)
	if err != nil {
		h.log.WithError(err).WithField("model_id", modelID).Error("Failed to get cache bounds")
	}

	config := externalModel.GetConfig()
	now := time.Now()

	// Determine scan type
	isIncrementalScan, isFullScan := h.determineScanType(modelID, cache, config, now)

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

	h.log.WithFields(logrus.Fields{
		"model_id":            modelID,
		"is_incremental_scan": isIncrementalScan,
		"is_full_scan":        isFullScan,
		"cache_state":         cacheState,
	}).Debug("Determined scan type and cache state")

	// Query bounds with cache state
	minBound, maxBound, err := h.queryExternalBounds(ctx, modelID, externalModel, cacheState)
	if err != nil {
		return err
	}

	// Handle no new data case for incremental scans
	if isIncrementalScan && minBound == 0 && maxBound == 0 && cache != nil {
		// No new data found, keep existing bounds
		h.log.WithField("model_id", modelID).Debug("No new data found in incremental scan, keeping existing bounds")
		minBound = cache.Min
		maxBound = cache.Max
	}

	// Apply lag if configured
	adjustedMin, adjustedMax := minBound, maxBound
	if config.Lag > 0 && adjustedMax > config.Lag {
		adjustedMax -= config.Lag
		h.log.WithFields(logrus.Fields{
			"model_id":     modelID,
			"lag":          config.Lag,
			"original_max": maxBound,
			"adjusted_max": adjustedMax,
		}).Debug("Applied lag to bounds")
	}

	// Update cache with new bounds
	if err := h.updateBoundsCache(ctx, cacheManager, modelID, adjustedMin, adjustedMax, cache, isIncrementalScan, now); err != nil {
		return err
	}

	h.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"min":       adjustedMin,
		"max":       adjustedMax,
		"scan_type": map[bool]string{true: "incremental", false: "full"}[isIncrementalScan],
		"duration":  time.Since(startTime),
	}).Info("Bounds cache updated successfully")

	return nil
}

// Routes returns the task handler routes for Asynq
func (h *TaskHandler) Routes() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		TypeModelTransformation: h.HandleTransformation,
		"bounds:cache":          h.HandleBoundsCache,
	}
}
