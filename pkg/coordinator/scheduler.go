package coordinator

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

var (
	// ErrScheduleRegistrationFailed is returned when one or more scheduled tasks fail to register
	ErrScheduleRegistrationFailed = errors.New("failed to register scheduled tasks")
)

// initializeScheduler sets up the Asynq scheduler and server
func (c *Coordinator) initializeScheduler() error {
	// Create scheduler for managing cron jobs
	c.scheduler = asynq.NewScheduler(*c.redisOpt, &asynq.SchedulerOpts{
		Location: time.UTC,
		LogLevel: asynq.InfoLevel,
	})

	// Create server for processing scheduled tasks
	c.schedulerServer = asynq.NewServer(*c.redisOpt, asynq.Config{
		Queues: map[string]int{
			"scheduler": 10, // High priority for scheduler tasks
		},
		Concurrency: 10,
	})

	// Reconcile schedules with current configuration
	if err := c.reconcileSchedules(); err != nil {
		return fmt.Errorf("failed to reconcile schedules: %w", err)
	}

	return nil
}

// reconcileSchedules ensures scheduled tasks match current model configuration
func (c *Coordinator) reconcileSchedules() error {
	c.logger.Info("Reconciling scheduled tasks with current configuration")

	// Setup handlers for scheduled tasks
	mux := asynq.NewServeMux()

	// Since Asynq Scheduler doesn't provide a way to list existing entries,
	// we'll use a simple approach: register all tasks on startup.
	// The scheduler handles deduplication internally using task IDs.

	// Build desired state from current configuration
	desiredTasks := make(map[string]string) // taskType -> schedule

	transformationModels := c.depManager.GetTransformationModels()
	for _, modelID := range transformationModels {
		config, exists := c.depManager.GetModelConfig(modelID)
		if !exists {
			continue
		}

		// Forward fill task (always for transformation models)
		forwardTask := fmt.Sprintf("%s%s:forward", taskPrefix, modelID)
		desiredTasks[forwardTask] = config.Schedule
		// Register handler for this specific task type
		mux.HandleFunc(forwardTask, c.HandleScheduledForward)

		// Backfill task (only if enabled)
		if config.Backfill != nil && config.Backfill.Enabled {
			backfillTask := fmt.Sprintf("%s%s:backfill", taskPrefix, modelID)
			desiredTasks[backfillTask] = config.Backfill.Schedule
			// Register handler for this specific task type
			mux.HandleFunc(backfillTask, c.HandleScheduledBackfill)
		}
	}

	// Store the mux for later use
	c.schedulerMux = mux

	// Register all tasks
	var errs []error
	for taskType, schedule := range desiredTasks {
		if err := c.registerScheduledTask(taskType, schedule); err != nil {
			c.logger.WithError(err).WithField("task_type", taskType).Error("Failed to register scheduled task")
			errs = append(errs, fmt.Errorf("failed to register %s: %w", taskType, err))
		}
	}

	c.logger.WithField("task_count", len(desiredTasks)).Info("Schedule reconciliation complete")

	if len(errs) > 0 {
		return fmt.Errorf("%w: %d tasks failed", ErrScheduleRegistrationFailed, len(errs))
	}
	return nil
}

// registerScheduledTask registers a new scheduled task
func (c *Coordinator) registerScheduledTask(taskType, schedule string) error {
	// Extract model ID for logging
	modelID := extractModelID(taskType)
	operation := "forward"
	if strings.HasSuffix(taskType, ":backfill") {
		operation = "backfill"
	}

	// Create the task
	task := asynq.NewTask(taskType, nil)

	// Register with scheduler
	entryID, err := c.scheduler.Register(schedule, task,
		asynq.Queue("scheduler"),    // Use dedicated scheduler queue
		asynq.Unique(1*time.Minute), // Prevent duplicate triggers
	)

	if err != nil {
		return fmt.Errorf("failed to register %s with schedule %s: %w", taskType, schedule, err)
	}

	c.logger.WithFields(logrus.Fields{
		"task_type": taskType,
		"model_id":  modelID,
		"operation": operation,
		"schedule":  schedule,
		"entry_id":  entryID,
	}).Info("Registered scheduled task")

	// Update metrics
	observability.ScheduledTasksRegistered.WithLabelValues(modelID, operation).Set(1)

	return nil
}
