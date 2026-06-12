package coordinator

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// maxProcessedTaskEntries is the maximum number of task entries to track before clearing.
// This prevents unbounded memory growth in the task tracker.
const maxProcessedTaskEntries = 10000

// taskOperation represents an operation on the processed tasks tracker
type taskOperation struct {
	taskID   string
	response chan bool // For check operations
}

// checkAndEnqueuePositionWithTrigger validates and enqueues a transformation task.
// It reports whether a task was actually enqueued.
func (s *service) checkAndEnqueuePositionWithTrigger(ctx context.Context, trans models.Transformation, position, interval uint64, direction string) bool {
	// Create task payload
	payload := tasks.IncrementalPayload{
		Type:       tasks.TypeIncremental,
		ModelID:    trans.GetID(),
		Position:   position,
		Interval:   interval,
		Direction:  direction,
		EnqueuedAt: time.Now(),
	}

	// Check if already enqueued or recently completed
	isPending, err := s.queueManager.IsTaskPendingOrRunning(payload)
	if err != nil {
		s.log.WithError(err).WithField("task_id", payload.UniqueID()).Error("Failed to check task status")

		observability.RecordError("coordinator", "task_status_check_error")

		return false
	}

	if isPending {
		s.log.WithField("task_id", payload.UniqueID()).Debug("Task already pending or running")

		return false
	}

	// Validate dependencies
	depStartTime := time.Now()

	validationResult, err := s.validator.ValidateDependencies(ctx, trans.GetID(), position, interval)
	depDuration := time.Since(depStartTime).Seconds()

	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"position": position,
		}).Error("Failed to validate dependencies")
		observability.RecordDependencyValidation(trans.GetID(), "error", depDuration)
		observability.RecordError("coordinator", "dependency_validation_error")
		return false
	}

	if !validationResult.CanProcess {
		s.log.WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"position": position,
		}).Debug("Dependencies not satisfied")
		observability.RecordDependencyValidation(trans.GetID(), "not_satisfied", depDuration)

		return false
	}

	observability.RecordDependencyValidation(trans.GetID(), "satisfied", depDuration)

	// Enqueue task
	if err := s.queueManager.EnqueueTransformation(payload); err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Error("Failed to enqueue task")

		observability.RecordError("coordinator", "enqueue_error")

		return false
	}

	// Record successful enqueue
	observability.RecordTaskEnqueued(trans.GetID())

	s.log.WithFields(logrus.Fields{
		"model_id": trans.GetID(),
		"position": position,
		"interval": interval,
	}).Info("Enqueued transformation task")

	return true
}

// pollCompletedTasks periodically checks for completed tasks
func (s *service) pollCompletedTasks() {
	defer s.wg.Done()

	interval := s.pollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.checkCompletedTasks()
		}
	}
}

// taskTracker manages task processing state using channels (ethPandaOps pattern).
// Tracked entries are kept in two generations: when the current generation
// exceeds maxProcessedTaskEntries it is rotated to the previous generation
// instead of being dropped, so recently completed tasks stay deduplicated
// while memory remains bounded at ~2x maxProcessedTaskEntries.
func (s *service) taskTracker() {
	defer s.wg.Done()

	currentGen := make(map[string]struct{}, 100)
	previousGen := make(map[string]struct{})

	isProcessed := func(taskID string) bool {
		if _, ok := currentGen[taskID]; ok {
			return true
		}

		_, ok := previousGen[taskID]

		return ok
	}

	for {
		select {
		case <-s.done:
			return
		case op := <-s.taskCheck:
			// Check if task is processed and respond
			select {
			case op.response <- isProcessed(op.taskID):
			case <-s.done:
				return
			}
		case taskID := <-s.taskMark:
			// Mark task as processed
			currentGen[taskID] = struct{}{}
			// Rotate generations once the current one grows too large
			if len(currentGen) > maxProcessedTaskEntries {
				previousGen = currentGen
				currentGen = make(map[string]struct{}, 100)
			}
		}
	}
}

// isTaskProcessed checks if a task has been processed (channel-based)
func (s *service) isTaskProcessed(taskID string) bool {
	response := make(chan bool, 1)
	select {
	case s.taskCheck <- taskOperation{taskID: taskID, response: response}:
		select {
		case processed := <-response:
			return processed
		case <-s.done:
			return false
		}
	case <-s.done:
		return false
	}
}

// markTaskProcessed marks a task as processed (channel-based)
func (s *service) markTaskProcessed(taskID string) {
	select {
	case s.taskMark <- taskID:
	case <-s.done:
	}
}

// checkCompletedTasks processes completed tasks and triggers dependents
func (s *service) checkCompletedTasks() {
	// Get all model queues
	for _, transformation := range s.dag.GetTransformationNodes() {
		// List completed tasks for this model's queue
		completedTasks, err := s.inspector.ListCompletedTasks(transformation.GetID(), asynq.PageSize(100))
		if err != nil {
			// Queue might not exist yet, that's ok
			continue
		}

		for _, taskInfo := range completedTasks {
			// Check if we've already processed this task (channel-based)
			if !s.isTaskProcessed(taskInfo.ID) {
				// Parse the task payload to get position and interval
				var payload tasks.Payload
				if err := json.Unmarshal(taskInfo.Payload, &payload); err == nil {
					ctx := context.Background()
					s.onTaskComplete(ctx, payload)
				}

				// Mark as processed (channel-based)
				s.markTaskProcessed(taskInfo.ID)
			}
		}
	}
}

// onTaskComplete handles post-task completion logic and triggers dependent tasks
func (s *service) onTaskComplete(ctx context.Context, payload tasks.Payload) {
	if payload == nil {
		return
	}

	logFields := logrus.Fields{
		"model_id": payload.GetModelID(),
	}

	// Add position for incremental tasks
	if incPayload, ok := payload.(tasks.IncrementalPayload); ok {
		logFields["position"] = incPayload.Position
	}

	s.log.WithFields(logFields).Debug("Task completed, checking dependents")

	// Get models that depend on this one
	dependents := s.dag.GetDependents(payload.GetModelID())

	for _, depModelID := range dependents {
		model, err := s.dag.GetTransformationNode(depModelID)
		if err != nil {
			continue
		}

		// Calculate next position for dependent
		lastPos, err := s.admin.GetNextUnprocessedPosition(ctx, depModelID)
		if err != nil {
			continue
		}

		nextPos := lastPos
		if nextPos == 0 {
			// First run - calculate initial position
			initialPos, err := s.validator.GetStartPosition(ctx, depModelID)
			if err != nil {
				continue
			}
			nextPos = initialPos
		}

		// Get interval from handler
		var interval uint64
		if handler := model.GetHandler(); handler != nil {
			if provider, ok := handler.(transformation.IntervalHandler); ok {
				interval = provider.GetMaxInterval()
			}
		}

		// Check if this completion unblocks the dependent
		// Use forward direction for dependent triggers
		s.checkAndEnqueuePositionWithTrigger(ctx, model, nextPos, interval, string(DirectionForward))
	}
}
