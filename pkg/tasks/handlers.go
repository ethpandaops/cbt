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
)

// getWorkerID returns the worker ID based on task metadata and hostname
func getWorkerID(ctx context.Context) string {
	// Asynq provides task metadata through helper functions
	// The task ID contains queue and other info but not the worker ID directly
	// Worker identification is typically done through hostname or config

	hostname, err := os.Hostname()
	if err != nil {
		return "worker-unknown"
	}

	// Include task ID to show which specific task instance
	// For now, just return hostname since asynq doesn't expose worker ID directly
	// Task ID is available but worker ID would need to be set during server creation
	_ = ctx // context might contain task metadata in future versions

	return hostname
}

// TaskHandler handles task execution
type TaskHandler struct {
	log             *logrus.Logger
	chClient        clickhouse.ClientInterface
	admin           *admin.Service
	validator       *validation.DependencyValidator
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
}

// NewTaskHandler creates a new task handler
func NewTaskHandler(
	logger *logrus.Logger,
	chClient clickhouse.ClientInterface,
	adminService *admin.Service,
	validator *validation.DependencyValidator,
	modelExecutor Executor,
	transformations []models.Transformation,
) *TaskHandler {
	transformationsMap := make(map[string]models.Transformation)
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
	workerID := getWorkerID(ctx)

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

// Routes returns the task handler routes for Asynq
func (h *TaskHandler) Routes() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		TypeModelTransformation: h.HandleTransformation,
	}
}
