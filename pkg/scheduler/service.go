package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	// TaskPrefix is the prefix for all scheduled tasks
	TaskPrefix = "cbt:"
	// QueueName is the queue name for scheduler tasks
	QueueName = "scheduler"
)

var (
	// ErrScheduleRegistrationFailed is returned when one or more scheduled tasks fail to register
	ErrScheduleRegistrationFailed = errors.New("failed to register scheduled tasks")
)

// Service manages scheduled tasks for transformations
type Service struct {
	log *logrus.Logger
	cfg *Config

	redisOpt    *redis.Options
	dag         *models.DependencyGraph
	coordinator *coordinator.Service

	scheduler *asynq.Scheduler
	server    *asynq.Server
	mux       *asynq.ServeMux
	inspector *asynq.Inspector
}

// NewService creates a new scheduler service
func NewService(log *logrus.Logger, cfg *Config, redisOpt *redis.Options, dag *models.DependencyGraph, coord *coordinator.Service) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	asynqRedis := r.NewAsynqRedisOptions(redisOpt)

	// Create scheduler for managing cron jobs
	scheduler := asynq.NewScheduler(asynqRedis, &asynq.SchedulerOpts{
		Location: time.UTC,
		LogLevel: asynq.InfoLevel,
	})

	// Create server for processing scheduled tasks
	server := asynq.NewServer(asynqRedis, asynq.Config{
		Queues: map[string]int{
			QueueName: 10,
		},
		Concurrency: 10,
	})

	// Create inspector for managing tasks
	inspector := asynq.NewInspector(asynqRedis)

	return &Service{
		log: log,
		cfg: cfg,

		redisOpt:    redisOpt,
		dag:         dag,
		coordinator: coord,

		scheduler: scheduler,
		server:    server,
		inspector: inspector,
	}, nil
}

// Start initializes and starts the scheduler service
func (s *Service) Start() error {
	go func() {
		if runErr := s.scheduler.Run(); runErr != nil {
			s.log.WithError(runErr).Fatal("Failed to run scheduler")
		}
	}()

	// Reconcile schedules with current configuration
	if err := s.reconcileSchedules(); err != nil {
		return fmt.Errorf("failed to reconcile schedules: %w", err)
	}

	// Start server in background
	go func() {
		if runErr := s.server.Run(s.mux); runErr != nil {
			s.log.WithError(runErr).Fatal("Failed to run scheduler server")
		}
	}()

	// Start periodic cleanup of duplicate scheduled tasks
	go s.runPeriodicCleanup()

	s.log.Info("Scheduler service started successfully")

	return nil
}

// Stop gracefully shuts down the scheduler service
func (s *Service) Stop() error {
	if s.scheduler != nil {
		s.scheduler.Shutdown()
	}

	if s.server != nil {
		s.server.Shutdown()
	}

	return nil
}

// reconcileSchedules ensures scheduled tasks match current model configuration
func (s *Service) reconcileSchedules() error {
	s.log.Info("Reconciling scheduled tasks with current configuration")

	// Setup handlers for scheduled tasks
	mux := asynq.NewServeMux()

	// Since Asynq Scheduler doesn't provide a way to list existing entries,
	// we'll use a simple approach: register all tasks on startup.
	// The scheduler handles deduplication internally using task IDs.

	// Build desired state from current configuration
	desiredTasks := make(map[string]string) // taskType -> schedule

	for _, transformation := range s.dag.GetTransformationNodes() {
		config := transformation.GetConfig()

		// Forward fill task (always for transformation models)
		forwardTask := fmt.Sprintf("%s%s:%s", TaskPrefix, transformation.GetID(), coordinator.DirectionForward)
		desiredTasks[forwardTask] = config.Schedule
		// Register handler for this specific task type
		mux.HandleFunc(forwardTask, s.HandleScheduledForward)

		// Backfill task (only if enabled)
		if config.Backfill != nil && config.Backfill.Enabled {
			backfillTask := fmt.Sprintf("%s%s:%s", TaskPrefix, transformation.GetID(), coordinator.DirectionBack)
			desiredTasks[backfillTask] = config.Backfill.Schedule
			// Register handler for this specific task type
			mux.HandleFunc(backfillTask, s.HandleScheduledBackfill)
		}
	}

	// Store the mux for later use
	s.mux = mux

	// Register all tasks
	var errs []error
	for taskType, schedule := range desiredTasks {
		if err := s.registerScheduledTask(taskType, schedule); err != nil {
			s.log.WithError(err).WithField("task_type", taskType).Error("Failed to register scheduled task")
			errs = append(errs, fmt.Errorf("failed to register %s: %w", taskType, err))
		}
	}

	s.log.WithField("task_count", len(desiredTasks)).Info("Schedule reconciliation complete")

	if len(errs) > 0 {
		return fmt.Errorf("%w: %d tasks failed", ErrScheduleRegistrationFailed, len(errs))
	}

	return nil
}

// registerScheduledTask registers a new scheduled task
func (s *Service) registerScheduledTask(taskType, schedule string) error {
	// Extract model ID for logging
	modelID := extractModelID(taskType)
	operation := coordinator.DirectionForward
	if strings.HasSuffix(taskType, fmt.Sprintf(":%s", coordinator.DirectionBack)) {
		operation = coordinator.DirectionBack
	}

	// Create the task
	task := asynq.NewTask(taskType, nil)

	// Register with scheduler
	entryID, err := s.scheduler.Register(schedule, task,
		asynq.Queue(QueueName),      // Use dedicated scheduler queue
		asynq.Unique(1*time.Minute), // Prevent duplicate triggers
	)

	if err != nil {
		return fmt.Errorf("failed to register %s with schedule %s: %w", taskType, schedule, err)
	}

	s.log.WithFields(logrus.Fields{
		"task_type": taskType,
		"model_id":  modelID,
		"operation": operation,
		"schedule":  schedule,
		"entry_id":  entryID,
	}).Info("Registered scheduled task")

	// Update metrics
	observability.ScheduledTasksRegistered.WithLabelValues(modelID, string(operation)).Set(1)

	return nil
}

// extractModelID extracts the model ID from a task type
// Example: "cbt:analytics.block_propagation:forward" -> "analytics.block_propagation"
func extractModelID(taskType string) string {
	trimmed := strings.TrimPrefix(taskType, TaskPrefix)
	parts := strings.Split(trimmed, ":")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// HandleScheduledForward processes scheduled forward fill checks
func (s *Service) HandleScheduledForward(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	transformation, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")

		return err
	}

	s.log.WithField("model_id", modelID).Debug("Processing scheduled forward check")

	// This triggers the existing forward fill logic
	s.coordinator.Process(transformation, coordinator.DirectionForward)

	// Record metrics
	observability.ScheduledTaskExecutions.WithLabelValues(
		modelID, string(coordinator.DirectionForward), "success",
	).Inc()

	return nil
}

// runPeriodicCleanup periodically removes duplicate scheduled tasks
func (s *Service) runPeriodicCleanup() {
	for {
		// Random interval between 1-2 minutes
		interval := time.Duration(60+rand.Intn(60)) * time.Second // #nosec G404 - using weak RNG for non-security purpose
		time.Sleep(interval)

		// Get all scheduled task entries
		entries, err := s.inspector.SchedulerEntries()
		if err != nil {
			continue
		}

		// Group by task type to find duplicates
		taskGroups := make(map[string][]*asynq.SchedulerEntry)
		for _, entry := range entries {
			// Only process our tasks
			if strings.HasPrefix(entry.Task.Type(), TaskPrefix) {
				taskGroups[entry.Task.Type()] = append(taskGroups[entry.Task.Type()], entry)
			}
		}

		// Remove duplicates, keeping the first one
		for _, group := range taskGroups {
			if len(group) > 1 {
				// Keep first, remove rest
				for i := 1; i < len(group); i++ {
					_ = s.scheduler.Unregister(group[i].ID)
				}
			}
		}
	}
}

// HandleScheduledBackfill processes scheduled backfill scans
func (s *Service) HandleScheduledBackfill(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	transformation, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")

		return err
	}

	s.log.WithField("model_id", modelID).Debug("Processing scheduled backfill scan")

	// This triggers the existing backfill logic
	s.coordinator.Process(transformation, coordinator.DirectionBack)

	// Record metrics
	observability.ScheduledTaskExecutions.WithLabelValues(
		modelID, string(coordinator.DirectionBack), "success",
	).Inc()

	return nil
}
