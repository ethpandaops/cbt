// Package coordinator handles task coordination and dependency management
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrShutdownErrors is returned when errors occur during shutdown
	ErrShutdownErrors = errors.New("errors during shutdown")
)

// Service defines the public interface for the coordinator
type Service interface {
	// Start initializes and starts the coordinator service
	Start(ctx context.Context) error

	// Stop gracefully shuts down the coordinator service
	Stop() error

	// Process handles transformation processing in the specified direction
	Process(transformation models.Transformation, direction Direction)
}

// PositionTracker defines the minimal interface needed from admin service
type PositionTracker interface {
	GetLastPosition(ctx context.Context, modelID string) (uint64, error)
	FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]admin.GapInfo, error)
}

// Direction represents the processing direction for tasks
type Direction string

const (
	// DirectionForward processes tasks in forward direction
	DirectionForward Direction = "forward"
	// DirectionBack processes tasks in backward direction
	DirectionBack Direction = "back"
)

// taskOperation represents an operation on the processed tasks tracker
type taskOperation struct {
	taskID   string
	response chan bool // For check operations
}

// service coordinates task processing and dependencies
type service struct {
	log logrus.FieldLogger

	// Synchronization - per ethPandaOps standards
	done chan struct{}  // Signal shutdown
	wg   sync.WaitGroup // Track goroutines

	// Channel-based task tracking (ethPandaOps: prefer channels over mutexes)
	taskCheck chan taskOperation // Check if task is processed
	taskMark  chan string        // Mark task as processed

	redisOpt  *redis.Options
	dag       models.DAGReader
	admin     PositionTracker
	validator validation.Validator

	queueManager   *tasks.QueueManager
	inspector      *asynq.Inspector
	archiveHandler ArchiveHandler
}

// NewService creates a new coordinator service
func NewService(log logrus.FieldLogger, redisOpt *redis.Options, dag models.DAGReader, adminService PositionTracker, validator validation.Validator) (Service, error) {
	return &service{
		log:       log.WithField("service", "coordinator"),
		redisOpt:  redisOpt,
		dag:       dag,
		admin:     adminService,
		validator: validator,
		done:      make(chan struct{}),
		taskCheck: make(chan taskOperation),
		taskMark:  make(chan string, 100), // Buffered to avoid blocking
	}, nil
}

// Start initializes and starts the coordinator service
func (s *service) Start(ctx context.Context) error {
	asynqRedis := r.NewAsynqRedisOptions(s.redisOpt)

	s.queueManager = tasks.NewQueueManager(asynqRedis)

	s.inspector = asynq.NewInspector(*asynqRedis)

	archiveHandler, err := NewArchiveHandler(s.log, s.redisOpt, s.dag)
	if err != nil {
		return fmt.Errorf("failed to create archive handler: %w", err)
	}

	s.archiveHandler = archiveHandler

	if err := s.archiveHandler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start archive handler: %w", err)
	}

	// Start task tracker goroutine (channel-based state management)
	s.wg.Add(1)
	go s.taskTracker()

	// Keep existing completed task polling
	s.wg.Add(1)
	go s.pollCompletedTasks()

	s.log.Info("Coordinator service started successfully")

	return nil
}

// Stop gracefully shuts down the coordinator service
func (s *service) Stop() error {
	var errs []error

	// Signal all goroutines to stop
	close(s.done)

	// Stop archive handler
	if s.archiveHandler != nil {
		if err := s.archiveHandler.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop archive handler: %w", err))
		}
	}

	// Wait for all goroutines to complete
	s.wg.Wait()

	if s.inspector != nil {
		if err := s.inspector.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close inspector: %w", err))
		}
	}

	if s.queueManager != nil {
		if err := s.queueManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close queue manager: %w", err))
		}
	}

	if len(errs) > 0 {
		// Combine all errors into a single error message
		var errStrs []string
		for _, err := range errs {
			errStrs = append(errStrs, err.Error())
		}
		return fmt.Errorf("%w: %s", ErrShutdownErrors, errStrs)
	}

	return nil
}

// Process handles transformation processing in the specified direction
func (s *service) Process(transformation models.Transformation, direction Direction) {
	switch direction {
	case DirectionForward:
		s.processForward(transformation)
	case DirectionBack:
		s.processBack(transformation)
	}
}

func (s *service) processForward(transformation models.Transformation) {
	ctx := context.Background()

	// Get last processed position
	lastPos, err := s.admin.GetLastPosition(ctx, transformation.GetID())
	if err != nil {
		s.log.WithError(err).WithField("model_id", transformation.GetID()).Error("Failed to get last position")

		return
	}

	s.log.WithFields(logrus.Fields{
		"model_id": transformation.GetID(),
		"last_pos": lastPos,
	}).Debug("Got last position from admin table")

	// If this is the first run, calculate initial position
	if lastPos == 0 {
		s.log.WithField("model_id", transformation.GetID()).Debug("Last position is 0, calculating initial position")

		initialPos, err := s.validator.GetInitialPosition(ctx, transformation.GetID())
		if err != nil {
			s.log.WithError(err).WithField("model_id", transformation.GetID()).Error("Failed to calculate initial position")
			return
		}

		s.log.WithFields(logrus.Fields{
			"model_id":    transformation.GetID(),
			"initial_pos": initialPos,
		}).Debug("Calculated initial position")

		lastPos = initialPos
	}

	// Check forward processing
	nextPos := lastPos

	config := transformation.GetConfig()

	// Don't process beyond max limit if configured
	if config.Limits != nil && config.Limits.Max > 0 && nextPos >= config.Limits.Max {
		s.log.WithFields(logrus.Fields{
			"model_id":   transformation.GetID(),
			"position":   nextPos,
			"limits_max": config.Limits.Max,
		}).Debug("Position is at or beyond max limit, skipping forward processing")
		return
	}

	// Adjust interval if it would exceed max limit
	interval := config.GetForwardInterval()
	if config.Limits != nil && config.Limits.Max > 0 && nextPos+interval > config.Limits.Max {
		// Reduce interval to stay within max limit
		interval = config.Limits.Max - nextPos
		s.log.WithFields(logrus.Fields{
			"model_id":          transformation.GetID(),
			"position":          nextPos,
			"original_interval": config.GetForwardInterval(),
			"adjusted_interval": interval,
			"limits_max":        config.Limits.Max,
		}).Debug("Adjusted interval to respect max limit")
	}

	s.checkAndEnqueuePositionWithTrigger(ctx, transformation, nextPos, interval)
}

func (s *service) processBack(transformation models.Transformation) {
	if !transformation.GetConfig().IsBackfillEnabled() {
		return
	}

	ctx := context.Background()
	s.checkBackfillOpportunities(ctx, transformation)
}

func (s *service) checkAndEnqueuePositionWithTrigger(ctx context.Context, transformation models.Transformation, position, interval uint64) {
	// Create task payload
	payload := tasks.TaskPayload{
		ModelID:    transformation.GetID(),
		Position:   position,
		Interval:   interval,
		EnqueuedAt: time.Now(),
	}

	// Check if already enqueued or recently completed
	isPending, err := s.queueManager.IsTaskPendingOrRunning(payload)
	if err != nil {
		s.log.WithError(err).WithField("task_id", payload.UniqueID()).Error("Failed to check task status")

		observability.RecordError("coordinator", "task_status_check_error")

		return
	}

	if isPending {
		s.log.WithField("task_id", payload.UniqueID()).Debug("Task already pending or running")

		return
	}

	// Validate dependencies
	depStartTime := time.Now()

	validationResult, err := s.validator.ValidateDependencies(ctx, transformation.GetID(), position, interval)
	depDuration := time.Since(depStartTime).Seconds()

	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id": transformation.GetID(),
			"position": position,
		}).Error("Failed to validate dependencies")
		observability.RecordDependencyValidation(transformation.GetID(), "error", depDuration)
		observability.RecordError("coordinator", "dependency_validation_error")
		return
	}

	if !validationResult.CanProcess {
		s.log.WithFields(logrus.Fields{
			"model_id": transformation.GetID(),
			"position": position,
		}).Debug("Dependencies not satisfied")
		observability.RecordDependencyValidation(transformation.GetID(), "not_satisfied", depDuration)

		return
	}

	observability.RecordDependencyValidation(transformation.GetID(), "satisfied", depDuration)

	// Enqueue task
	if err := s.queueManager.EnqueueTransformation(payload); err != nil {
		s.log.WithError(err).WithField("model_id", transformation.GetID()).Error("Failed to enqueue task")

		observability.RecordError("coordinator", "enqueue_error")

		return
	}

	// Record successful enqueue
	observability.RecordTaskEnqueued(transformation.GetID())

	s.log.WithFields(logrus.Fields{
		"model_id": transformation.GetID(),
		"position": position,
		"interval": interval,
	}).Info("Enqueued transformation task")
}

func (s *service) checkBackfillOpportunities(ctx context.Context, transformation models.Transformation) {
	config := transformation.GetConfig()

	// Get the range to check for gaps
	lastPos, err := s.admin.GetLastPosition(ctx, transformation.GetID())
	if err != nil {
		s.log.WithError(err).WithField("model_id", transformation.GetID()).Debug("Failed to get last position for gap scan")

		return
	}

	s.log.WithFields(logrus.Fields{
		"model_id": transformation.GetID(),
		"last_pos": lastPos,
		"interval": config.GetBackfillInterval(),
	}).Debug("Got last position for gap scanning")

	if lastPos < config.GetBackfillInterval() {
		s.log.WithField("model_id", transformation.GetID()).Debug("No data yet, skipping gap scan")

		return // No data yet
	}

	// Get initial position to determine scan range
	initialPos, err := s.validator.GetEarliestPosition(ctx, transformation.GetID())
	if err != nil {
		s.log.WithError(err).WithField("model_id", transformation.GetID()).Debug("Failed to get initial position for gap scan")

		return
	}

	// Use the maximum of the configured minimum and the calculated initial position
	if config.Limits != nil && config.Limits.Min > initialPos {
		initialPos = config.Limits.Min
		s.log.WithFields(logrus.Fields{
			"model_id":    transformation.GetID(),
			"initial_pos": initialPos,
			"limits_min":  config.Limits.Min,
		}).Debug("Using configured minimum position for gap scanning")
	} else {
		s.log.WithFields(logrus.Fields{
			"model_id":    transformation.GetID(),
			"initial_pos": initialPos,
		}).Debug("Got initial position for gap scanning")
	}

	// Apply max limit if configured
	maxPos := lastPos
	if config.Limits != nil && config.Limits.Max > 0 && config.Limits.Max < lastPos {
		maxPos = config.Limits.Max
		s.log.WithFields(logrus.Fields{
			"model_id":   transformation.GetID(),
			"last_pos":   lastPos,
			"limits_max": config.Limits.Max,
		}).Debug("Applying maximum position limit for gap scanning")
	}

	// Find all gaps in the processed data
	gaps, err := s.admin.FindGaps(ctx, transformation.GetID(), initialPos, maxPos, config.GetBackfillInterval())
	if err != nil {
		s.log.WithError(err).WithField("model_id", transformation.GetID()).Error("Failed to find gaps")

		return
	}

	// Process gaps - queue only one task per gap to gradually fill it
	for _, gap := range gaps {
		gapSize := gap.EndPos - gap.StartPos
		intervalToUse := config.GetBackfillInterval()

		// If gap is smaller than model interval, use gap size to avoid overlap
		if gapSize < config.GetBackfillInterval() {
			intervalToUse = gapSize
		}

		// Start from the end of the gap and work backwards (most recent first)
		// This ensures we fill the most recent part of each gap first
		pos := gap.EndPos - intervalToUse

		// Check if task is already pending before logging
		payload := tasks.TaskPayload{
			ModelID:  transformation.GetID(),
			Position: pos,
			Interval: intervalToUse,
		}

		isPending, err := s.queueManager.IsTaskPendingOrRunning(payload)
		if err == nil && !isPending {
			s.log.WithFields(logrus.Fields{
				"model_id":       transformation.GetID(),
				"gap_start":      gap.StartPos,
				"gap_end":        gap.EndPos,
				"position":       pos,
				"interval":       intervalToUse,
				"model_interval": config.GetBackfillInterval(),
				"gap_size":       gapSize,
			}).Info("Found backfill opportunity")

			s.checkAndEnqueuePositionWithTrigger(ctx, transformation, pos, intervalToUse)

			// Only queue one task per gap - the next task will be queued
			// after this one completes and the gap is re-scanned
			break
		}
	}
}

func (s *service) pollCompletedTasks() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
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

// taskTracker manages task processing state using channels (ethPandaOps pattern)
func (s *service) taskTracker() {
	defer s.wg.Done()

	processedTasks := make(map[string]bool, 100)

	for {
		select {
		case <-s.done:
			return
		case op := <-s.taskCheck:
			// Check if task is processed and respond
			processed := processedTasks[op.taskID]
			select {
			case op.response <- processed:
			case <-s.done:
				return
			}
		case taskID := <-s.taskMark:
			// Mark task as processed
			processedTasks[taskID] = true
			// Clean up old entries if map gets too large
			if len(processedTasks) > 10000 {
				// Keep recent entries only
				newMap := make(map[string]bool, 100)
				processedTasks = newMap
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
				var payload tasks.TaskPayload
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

func (s *service) onTaskComplete(ctx context.Context, payload tasks.TaskPayload) {
	s.log.WithFields(logrus.Fields{
		"model_id": payload.ModelID,
		"position": payload.Position,
	}).Debug("Task completed, checking dependents")

	// Update model position metrics
	observability.ModelLastPosition.WithLabelValues(payload.ModelID).Set(float64(payload.Position))

	// Get models that depend on this one
	dependents := s.dag.GetDependents(payload.ModelID)

	for _, depModelID := range dependents {
		model, err := s.dag.GetTransformationNode(depModelID)
		if err != nil {
			continue
		}

		config := model.GetConfig()

		// Calculate next position for dependent
		lastPos, err := s.admin.GetLastPosition(ctx, depModelID)
		if err != nil {
			continue
		}

		nextPos := lastPos
		if nextPos == 0 {
			// First run - calculate initial position
			initialPos, err := s.validator.GetInitialPosition(ctx, depModelID)
			if err != nil {
				continue
			}
			nextPos = initialPos
		}

		// Check if this completion unblocks the dependent
		s.checkAndEnqueuePositionWithTrigger(ctx, model, nextPos, config.GetForwardInterval())
	}
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
