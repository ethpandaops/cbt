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
	UpdateBounds(ctx context.Context, modelID string) error
}

// NewTaskHandler creates a new task handler
func NewTaskHandler(
	logger logrus.FieldLogger,
	chClient clickhouse.ClientInterface,
	adminService admin.Service,
	validator validation.Validator,
	modelExecutor Executor,
	transformations []models.Transformation,
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
	}
}

// HandleTransformation handles transformation tasks
func (h *TaskHandler) HandleTransformation(ctx context.Context, t *asynq.Task) error {
	payload, err := h.parseTaskPayload(t.Payload())
	if err != nil {
		return err
	}

	taskContext, err := h.setupTaskContext(payload)
	if err != nil {
		return err
	}

	if err := h.validateAndExecute(ctx, payload, taskContext); err != nil {
		return err
	}

	h.recordTaskSuccess(ctx, payload, taskContext)
	return nil
}

// parseTaskPayload attempts to unmarshal the task payload
func (h *TaskHandler) parseTaskPayload(data []byte) (TaskPayload, error) {
	var incPayload IncrementalTaskPayload
	if err := json.Unmarshal(data, &incPayload); err == nil && (incPayload.Position > 0 || incPayload.Interval > 0) {
		return incPayload, nil
	}

	var schPayload ScheduledTaskPayload
	if err := json.Unmarshal(data, &schPayload); err != nil {
		observability.RecordError("task-handler", "unmarshal_error")
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return schPayload, nil
}

// taskContextData holds all the context for task execution
type taskContextData struct {
	transformation models.Transformation
	position       uint64
	interval       uint64
	executionTime  time.Time
	startTime      time.Time
	workerID       string
}

// setupTaskContext prepares the task execution context
func (h *TaskHandler) setupTaskContext(payload TaskPayload) (*taskContextData, error) {
	// Log task start
	switch p := payload.(type) {
	case IncrementalTaskPayload:
		h.log.WithFields(logrus.Fields{
			"model_id": p.ModelID,
			"position": p.Position,
			"interval": p.Interval,
		}).Info("Starting incremental transformation task")
	case ScheduledTaskPayload:
		h.log.WithFields(logrus.Fields{
			"model_id":       p.ModelID,
			"execution_time": p.ExecutionTime,
		}).Info("Starting scheduled transformation task")
	}

	startTime := time.Now()
	workerID := getWorkerID()
	observability.RecordTaskStart(payload.GetModelID(), workerID)

	// Get model configuration
	transformation, exists := h.transformations[payload.GetModelID()]
	if !exists {
		observability.RecordTaskComplete(payload.GetModelID(), workerID, "failed", time.Since(startTime).Seconds())
		observability.RecordError("task-handler", "model_not_found")
		return nil, ErrModelConfigNotFound
	}

	// Extract parameters based on payload type
	var position, interval uint64
	var executionTime time.Time
	switch p := payload.(type) {
	case IncrementalTaskPayload:
		position = p.Position
		interval = p.Interval
		executionTime = startTime
	case ScheduledTaskPayload:
		executionTime = p.ExecutionTime
	default:
		executionTime = startTime
	}

	return &taskContextData{
		transformation: transformation,
		position:       position,
		interval:       interval,
		executionTime:  executionTime,
		startTime:      startTime,
		workerID:       workerID,
	}, nil
}

// validateAndExecute handles dependency validation and task execution
func (h *TaskHandler) validateAndExecute(ctx context.Context, payload TaskPayload, taskCtx *taskContextData) error {
	// Validate dependencies
	h.log.WithField("model_id", payload.GetModelID()).Info("Validating dependencies")
	depStartTime := time.Now()
	validationResult, err := h.validator.ValidateDependencies(ctx, payload.GetModelID(), taskCtx.position, taskCtx.interval)
	depDuration := time.Since(depStartTime).Seconds()

	if err != nil {
		h.log.WithError(err).Error("Dependency validation error")
		observability.RecordDependencyValidation(payload.GetModelID(), "error", depDuration)
		observability.RecordTaskComplete(payload.GetModelID(), taskCtx.workerID, "failed", time.Since(taskCtx.startTime).Seconds())
		observability.RecordError("task-handler", "dependency_validation_error")
		return fmt.Errorf("dependency validation error: %w", err)
	}

	if !validationResult.CanProcess {
		h.log.WithField("model_id", payload.GetModelID()).Warn("Dependencies not satisfied")
		observability.RecordDependencyValidation(payload.GetModelID(), "not_satisfied", depDuration)
		observability.RecordTaskComplete(payload.GetModelID(), taskCtx.workerID, "failed", time.Since(taskCtx.startTime).Seconds())
		return ErrDependenciesNotSatisfied
	}

	observability.RecordDependencyValidation(payload.GetModelID(), "satisfied", depDuration)
	h.log.WithField("model_id", payload.GetModelID()).Info("Dependencies satisfied, executing transformation")

	// Execute transformation
	execCtx := &TaskContext{
		Transformation: taskCtx.transformation,
		Position:       taskCtx.position,
		Interval:       taskCtx.interval,
		StartTime:      taskCtx.executionTime,
	}

	h.log.WithField("model_id", payload.GetModelID()).Info("Calling modelExecutor.Execute")
	if err := h.modelExecutor.Execute(ctx, execCtx); err != nil {
		h.log.WithError(err).WithField("model_id", payload.GetModelID()).Error("Model execution failed")
		observability.RecordTaskComplete(payload.GetModelID(), taskCtx.workerID, "failed", time.Since(taskCtx.startTime).Seconds())
		observability.RecordError("task-handler", "execution_error")
		return fmt.Errorf("execution error: %w", err)
	}
	h.log.WithField("model_id", payload.GetModelID()).Info("Model execution completed")
	return nil
}

// recordTaskSuccess records metrics for successful task completion
func (h *TaskHandler) recordTaskSuccess(ctx context.Context, payload TaskPayload, taskCtx *taskContextData) {
	// Record transformation bounds in metrics (only for incremental transformations)
	if !taskCtx.transformation.GetConfig().IsScheduledType() {
		minPos, _ := h.admin.GetFirstPosition(ctx, payload.GetModelID())
		maxPos, _ := h.admin.GetLastProcessedEndPosition(ctx, payload.GetModelID())
		if minPos > 0 && maxPos > 0 {
			observability.RecordModelBounds(payload.GetModelID(), minPos, maxPos)
		}
	}

	// Record successful completion
	observability.RecordTaskComplete(payload.GetModelID(), taskCtx.workerID, "success", time.Since(taskCtx.startTime).Seconds())

	h.log.WithFields(logrus.Fields{
		"model_id": payload.GetModelID(),
		"position": taskCtx.position,
		"interval": taskCtx.interval,
		"duration": time.Since(taskCtx.startTime),
	}).Info("Task completed successfully")
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

	startTime := time.Now()

	workerID := getWorkerID()

	h.log.WithField("model_id", modelID).Debug("Calling modelExecutor.UpdateBounds")

	if err := h.modelExecutor.UpdateBounds(ctx, modelID); err != nil {
		h.log.WithError(err).WithField("model_id", modelID).Error("Model update bounds failed")
		observability.RecordTaskComplete(modelID, workerID, "failed", time.Since(startTime).Seconds())
		observability.RecordError("task-handler", "execution_error")

		return fmt.Errorf("execution error: %w", err)
	}

	h.log.WithField("model_id", modelID).Debug("Model execution completed")

	// Record successful completion
	observability.RecordTaskComplete(modelID, workerID, "success", time.Since(startTime).Seconds())

	h.log.WithFields(logrus.Fields{
		"model_id": modelID,
		"duration": time.Since(startTime),
	}).Info("Task completed successfully")

	return nil
}

// Routes returns the task handler routes for Asynq
func (h *TaskHandler) Routes() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		TypeModelTransformation: h.HandleTransformation,
		"bounds:cache":          h.HandleBoundsCache,
	}
}
