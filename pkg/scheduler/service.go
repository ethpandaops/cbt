package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
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
	// TransformationTaskPrefix is the prefix for transformation scheduled tasks
	TransformationTaskPrefix = "transformation:"
	// ConsolidationTaskType is the task type for consolidation
	ConsolidationTaskType = "consolidation"
	// BoundsOrchestratorTaskType is the task type for bounds orchestration
	BoundsOrchestratorTaskType = "bounds:orchestrator"
	// QueueName is the queue name for scheduler tasks
	QueueName = "scheduler"
)

var (
	// ErrScheduleRegistrationFailed is returned when one or more scheduled tasks fail to register
	ErrScheduleRegistrationFailed = errors.New("failed to register scheduled tasks")
)

// Service defines the public interface for the scheduler
type Service interface {
	// Start initializes and starts the scheduler service
	Start(ctx context.Context) error

	// Stop gracefully shuts down the scheduler service
	Stop() error

	// Note: These methods are internal and used by asynq handlers
	// They are exposed in the interface for testing purposes
}

// CoordinatorClient defines the minimal interface needed from coordinator
type CoordinatorClient interface {
	Process(transformation models.Transformation, direction coordinator.Direction)
}

// service manages scheduled tasks for transformations
type service struct {
	log logrus.FieldLogger // Using FieldLogger interface per ethPandaOps
	cfg *Config

	// Synchronization - per ethPandaOps standards
	done chan struct{}  // Signal shutdown
	wg   sync.WaitGroup // Track goroutines

	redisOpt    *redis.Options
	dag         models.DAGReader
	coordinator coordinator.Service

	scheduler *asynq.Scheduler
	server    *asynq.Server
	mux       *asynq.ServeMux
	inspector *asynq.Inspector
}

// NewService creates a new scheduler service
func NewService(log logrus.FieldLogger, cfg *Config, redisOpt *redis.Options, dag models.DAGReader, coord coordinator.Service) (Service, error) {
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
		Concurrency: cfg.Concurrency,
	})

	// Create inspector for managing tasks
	inspector := asynq.NewInspector(asynqRedis)

	return &service{
		log:  log.WithField("service", "scheduler"), // Add service-specific field per ethPandaOps
		cfg:  cfg,
		done: make(chan struct{}),

		redisOpt:    redisOpt,
		dag:         dag,
		coordinator: coord,

		scheduler: scheduler,
		server:    server,
		inspector: inspector,
	}, nil
}

// Start initializes and starts the scheduler service
func (s *service) Start(_ context.Context) error {
	// Track scheduler goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if runErr := s.scheduler.Run(); runErr != nil {
			s.log.WithError(runErr).Error("Scheduler stopped with error")
		}
	}()

	// Reconcile schedules with current configuration
	if err := s.reconcileSchedules(); err != nil {
		return fmt.Errorf("failed to reconcile schedules: %w", err)
	}

	// Start server in background
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if runErr := s.server.Run(s.mux); runErr != nil {
			s.log.WithError(runErr).Error("Scheduler server stopped with error")
		}
	}()

	// Start periodic cleanup of duplicate scheduled tasks
	s.wg.Add(1)
	go s.runPeriodicCleanup()

	s.log.Info("Scheduler service started successfully")

	return nil
}

// Stop gracefully shuts down the scheduler service
func (s *service) Stop() error {
	// Signal all goroutines to stop
	close(s.done)

	// Shutdown scheduler and server
	if s.scheduler != nil {
		s.scheduler.Shutdown()
	}

	if s.server != nil {
		s.server.Shutdown()
	}

	// Wait for all goroutines to complete
	s.wg.Wait()

	s.log.Info("Scheduler service stopped successfully")

	return nil
}

// reconcileSchedules ensures scheduled tasks match current model configuration
func (s *service) reconcileSchedules() error {
	s.log.Info("Reconciling scheduled tasks with current configuration")

	// Setup handlers for scheduled tasks
	mux := asynq.NewServeMux()

	// Since Asynq Scheduler doesn't provide a way to list existing entries,
	// we'll use a simple approach: register all tasks on startup.
	// The scheduler handles deduplication internally using task IDs.

	// Build desired state from current configuration
	transformations := s.dag.GetTransformationNodes()
	desiredTasks := make(map[string]string, len(transformations)*2) // taskType -> schedule (forward + backfill)

	for _, transformation := range transformations {
		config := transformation.GetConfig()
		modelID := transformation.GetID()

		// Forward fill task (only if configured)
		if config.ForwardFill != nil && config.GetForwardSchedule() != "" {
			forwardTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionForward)
			desiredTasks[forwardTask] = config.GetForwardSchedule()
			// Register handler for this specific task type
			mux.HandleFunc(forwardTask, s.HandleScheduledForward)
			s.log.WithFields(logrus.Fields{
				"model_id": modelID,
				"schedule": config.GetForwardSchedule(),
			}).Debug("Registering forward fill task")
		}

		// Backfill task (only if configured)
		if config.IsBackfillEnabled() && config.Backfill.Schedule != "" {
			backfillTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionBack)
			desiredTasks[backfillTask] = config.Backfill.Schedule
			// Register handler for this specific task type
			mux.HandleFunc(backfillTask, s.HandleScheduledBackfill)
			s.log.WithFields(logrus.Fields{
				"model_id": modelID,
				"schedule": config.Backfill.Schedule,
			}).Debug("Registering backfill task")
		}

		// Warn if no tasks were registered for this transformation
		hasForward := config.ForwardFill != nil && config.GetForwardSchedule() != ""
		hasBackfill := config.IsBackfillEnabled() && config.Backfill.Schedule != ""
		if !hasForward && !hasBackfill {
			s.log.WithField("model_id", modelID).Warn("Transformation has no scheduled tasks (neither forward fill nor backfill configured)")
		}
	}

	// Register consolidation task
	mux.HandleFunc(ConsolidationTaskType, s.HandleConsolidation)
	// Use configured consolidation schedule, or default if not set
	consolidationSchedule := s.cfg.Consolidation
	desiredTasks[ConsolidationTaskType] = consolidationSchedule

	// Register bounds orchestrator task - runs every second
	mux.HandleFunc(BoundsOrchestratorTaskType, s.HandleBoundsOrchestrator)
	desiredTasks[BoundsOrchestratorTaskType] = "@every 1s"

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
func (s *service) registerScheduledTask(taskType, schedule string) error {
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

	// Check if this is the consolidation task (special case)
	if taskType == ConsolidationTaskType {
		s.log.WithFields(logrus.Fields{
			"task_type": taskType,
			"schedule":  schedule,
			"entry_id":  entryID,
		}).Info("Registered consolidation task")

		// Update metrics for consolidation
		observability.RecordScheduledTaskRegistered("consolidation", "maintenance")
	} else {
		// Regular transformation task
		modelID := extractModelID(taskType)
		operation := coordinator.DirectionForward
		if strings.HasSuffix(taskType, fmt.Sprintf(":%s", coordinator.DirectionBack)) {
			operation = coordinator.DirectionBack
		}

		s.log.WithFields(logrus.Fields{
			"task_type": taskType,
			"model_id":  modelID,
			"operation": operation,
			"schedule":  schedule,
			"entry_id":  entryID,
		}).Info("Registered scheduled task")

		// Update metrics
		observability.RecordScheduledTaskRegistered(modelID, string(operation))
	}

	return nil
}

// extractModelID extracts the model ID from a task type
// Example: "transformation:analytics.block_propagation:forward" -> "analytics.block_propagation"
func extractModelID(taskType string) string {
	// Only extract model ID from transformation tasks
	if !strings.HasPrefix(taskType, TransformationTaskPrefix) {
		// Handle tasks without the transformation prefix (like consolidation)
		if strings.Contains(taskType, ":") {
			// Legacy format without prefix: "test.model:forward"
			parts := strings.Split(taskType, ":")
			return parts[0]
		}
		// Not a transformation task (e.g., "consolidation")
		return ""
	}

	trimmed := strings.TrimPrefix(taskType, TransformationTaskPrefix)
	parts := strings.Split(trimmed, ":")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// HandleScheduledForward processes scheduled forward fill checks
func (s *service) HandleScheduledForward(_ context.Context, t *asynq.Task) error {
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
	observability.RecordScheduledTaskExecution(modelID, string(coordinator.DirectionForward), "success")

	return nil
}

// runPeriodicCleanup periodically removes duplicate scheduled tasks
func (s *service) runPeriodicCleanup() {
	defer s.wg.Done()

	for {
		// Random interval between 1-2 minutes
		interval := time.Duration(60+rand.Intn(60)) * time.Second // #nosec G404 - using weak RNG for non-security purpose
		timer := time.NewTimer(interval)

		select {
		case <-s.done:
			timer.Stop()
			return
		case <-timer.C:
			// Get all scheduled task entries
			entries, err := s.inspector.SchedulerEntries()
			if err != nil {
				continue
			}

			// Group by task type to find duplicates
			taskGroups := make(map[string][]*asynq.SchedulerEntry, 10) // Add capacity hint
			for _, entry := range entries {
				// Only process our tasks (transformation, consolidation, or bounds orchestration)
				taskType := entry.Task.Type()
				if strings.HasPrefix(taskType, TransformationTaskPrefix) ||
					taskType == ConsolidationTaskType ||
					taskType == BoundsOrchestratorTaskType {
					taskGroups[taskType] = append(taskGroups[taskType], entry)
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
}

// HandleBoundsOrchestrator processes the bounds orchestrator task
// This task runs every second and checks if external models need bounds updates
func (s *service) HandleBoundsOrchestrator(ctx context.Context, _ *asynq.Task) error {
	s.log.Debug("Running bounds orchestrator check")

	// Delegate to coordinator to handle bounds orchestration
	if boundsOrchestrator, ok := s.coordinator.(interface{ ProcessBoundsOrchestration(context.Context) }); ok {
		boundsOrchestrator.ProcessBoundsOrchestration(ctx)
	} else {
		s.log.Debug("Coordinator doesn't support bounds orchestration")
	}

	return nil
}

// HandleScheduledBackfill processes scheduled backfill scans
func (s *service) HandleScheduledBackfill(_ context.Context, t *asynq.Task) error {
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
	observability.RecordScheduledTaskExecution(modelID, string(coordinator.DirectionBack), "success")

	return nil
}

// HandleConsolidation triggers admin table consolidation
func (s *service) HandleConsolidation(ctx context.Context, _ *asynq.Task) error {
	s.log.Info("Running admin table consolidation")

	// Call the coordinator to actually run the consolidation
	// This ensures only one instance handles it at a time via asynq
	if consolidator, ok := s.coordinator.(interface{ RunConsolidation(context.Context) }); ok {
		consolidator.RunConsolidation(ctx)
		s.log.Info("Admin consolidation completed")
	} else {
		s.log.Debug("Coordinator doesn't support consolidation")
	}

	return nil
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
