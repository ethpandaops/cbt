package scheduler

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
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
	// ExternalTaskPrefix is the prefix for external model tasks
	ExternalTaskPrefix = "external:"
	// QueueName is the queue name for scheduler tasks
	QueueName = "scheduler"
)

// taskReconcileResult represents the result of reconciling a scheduled task
type taskReconcileResult int

const (
	taskReconcileSkipped taskReconcileResult = iota
	taskReconcileRegistered
	taskReconcileUpdated
)

var (
	// ErrScheduleRegistrationFailed is returned when one or more scheduled tasks fail to register
	ErrScheduleRegistrationFailed = errors.New("failed to register scheduled tasks")
	// ErrInvalidExternalTaskType is returned when the external task type format is invalid
	ErrInvalidExternalTaskType = errors.New("invalid external task type format")
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
	admin       admin.Service

	scheduler *asynq.Scheduler
	server    *asynq.Server
	mux       *asynq.ServeMux
	inspector *asynq.Inspector

	elector LeaderElector
}

// NewService creates a new scheduler service
func NewService(log logrus.FieldLogger, cfg *Config, redisOpt *redis.Options, dag models.DAGReader, coord coordinator.Service, adminService admin.Service) (Service, error) {
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

	// Create leader elector
	elector := NewLeaderElector(log, redisOpt)

	// Initialize ServeMux for task handlers (needed before server starts)
	mux := asynq.NewServeMux()

	return &service{
		log:  log.WithField("service", "scheduler"), // Add service-specific field per ethPandaOps
		cfg:  cfg,
		done: make(chan struct{}),

		redisOpt:    redisOpt,
		dag:         dag,
		coordinator: coord,
		admin:       adminService,

		scheduler: scheduler,
		server:    server,
		mux:       mux,
		inspector: inspector,
		elector:   elector,
	}, nil
}

// Start initializes and starts the scheduler service
func (s *service) Start(ctx context.Context) error {
	// Register all task handlers on this instance before starting
	// This ensures any instance can process scheduled tasks from the queue,
	// not just the leader. The leader is responsible for scheduling the tasks,
	// but all instances must be able to execute them when picked up from Redis.
	s.registerAllHandlers()

	// Start leader election
	if err := s.elector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start leader election: %w", err)
	}

	// Handle leader election events
	s.wg.Add(1)
	go s.handleLeaderElection(ctx)

	// Start server in background (always runs for processing scheduled tasks)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if runErr := s.server.Run(s.mux); runErr != nil {
			s.log.WithError(runErr).Error("Scheduler server stopped with error")
		}
	}()

	// Start periodic cleanup (only active when leader)
	s.wg.Add(1)
	go s.runPeriodicCleanup()

	s.log.Info("Scheduler service started (participating in leader election)")

	return nil
}

// Stop gracefully shuts down the scheduler service
func (s *service) Stop() error {
	// Signal all goroutines to stop
	close(s.done)

	// Stop leader election first
	if s.elector != nil {
		if err := s.elector.Stop(); err != nil {
			s.log.WithError(err).Warn("Failed to stop leader elector")
		}
	}

	// NOTE: With leader election in place, calling Shutdown() is safe and proper.
	// While Shutdown() does clear scheduler entries from Redis, this is acceptable because:
	// 1. Only ONE scheduler runs (the leader), so no cross-instance pollution
	// 2. Entries are ephemeral anyway - cleared when scheduler stops regardless
	// 3. The promoted follower will reconcile and recreate all entries immediately
	// 4. Shutdown() ensures proper cleanup of Redis client and cron scheduler
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

// handleLeaderElection manages scheduler lifecycle based on leader election events
func (s *service) handleLeaderElection(ctx context.Context) {
	defer s.wg.Done()

	// Access the promoted/demoted channels through type assertion
	// This is safe because we control the elector implementation
	type channelProvider interface {
		PromotedChan() <-chan struct{}
		DemotedChan() <-chan struct{}
	}

	provider, ok := s.elector.(channelProvider)
	if !ok {
		s.log.Error("Leader elector does not provide channels")
		return
	}

	promoted := provider.PromotedChan()
	demoted := provider.DemotedChan()

	var schedulerRunning bool
	var schedulerDone chan struct{}

	for {
		select {
		case <-s.done:
			return

		case <-ctx.Done():
			return

		case <-promoted:
			if schedulerRunning {
				s.log.Warn("Received promotion but scheduler already running")
				continue
			}

			s.log.Info("Promoted to scheduler leader - starting scheduler")

			// Start scheduler goroutine
			schedulerDone = make(chan struct{})
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer close(schedulerDone)
				if runErr := s.scheduler.Run(); runErr != nil {
					s.log.WithError(runErr).Error("Scheduler stopped with error")
				}
			}()
			schedulerRunning = true

			// Reconcile schedules as new leader (handlers already registered in s.mux)
			if err := s.reconcileSchedules(); err != nil {
				s.log.WithError(err).Error("Failed to reconcile schedules as leader")
				continue
			}

			// Trigger initial external scans
			s.triggerInitialExternalScans(ctx)

		case <-demoted:
			s.log.Info("Demoted from scheduler leader")
			// Scheduler will stop naturally when s.done is closed
			// We don't need to actively stop it here
			schedulerRunning = false
		}
	}
}

// reconcileSchedules ensures scheduled tasks match current model configuration
func (s *service) reconcileSchedules() error {
	s.log.Info("Reconciling scheduled tasks with current configuration")

	// Get existing scheduled tasks
	existingTasks := s.getExistingScheduledTasks()

	// Build desired state from current configuration
	desiredTasks := s.buildDesiredTasks()

	// Reconcile tasks
	stats, errs := s.reconcileTasks(desiredTasks, existingTasks)

	s.log.WithFields(logrus.Fields{
		"total_desired": len(desiredTasks),
		"registered":    stats.registered,
		"updated":       stats.updated,
		"skipped":       stats.skipped,
		"removed":       stats.removed,
		"errors":        len(errs),
	}).Info("Schedule reconciliation complete")

	if len(errs) > 0 {
		return fmt.Errorf("%w: %d tasks failed", ErrScheduleRegistrationFailed, len(errs))
	}

	return nil
}

// reconcileStats holds statistics for reconciliation results
type reconcileStats struct {
	registered int
	updated    int
	skipped    int
	removed    int
}

// getExistingScheduledTasks retrieves existing scheduled tasks from Asynq
func (s *service) getExistingScheduledTasks() map[string]*asynq.SchedulerEntry {
	// IMPORTANT: We must check for existing tasks to prevent multiple instances
	// from disrupting each other. When a new instance starts, it should NOT
	// delete and re-register existing scheduled tasks unless the schedule changed.
	existingTasks := make(map[string]*asynq.SchedulerEntry)
	entries, err := s.inspector.SchedulerEntries()
	if err != nil {
		s.log.WithError(err).Warn("Failed to get existing scheduled entries, will register all tasks")
		return existingTasks
	}

	for _, entry := range entries {
		taskType := entry.Task.Type()
		// Only track our tasks (transformation, consolidation, or external)
		if strings.HasPrefix(taskType, TransformationTaskPrefix) ||
			taskType == ConsolidationTaskType ||
			strings.HasPrefix(taskType, ExternalTaskPrefix) {
			existingTasks[taskType] = entry
			s.log.WithFields(logrus.Fields{
				"task_type": taskType,
				"schedule":  entry.Spec,
			}).Debug("Found existing scheduled task")
		}
	}
	s.log.WithField("existing_count", len(existingTasks)).Info("Found existing scheduled tasks")
	return existingTasks
}

// registerAllHandlers registers all task handlers on the mux (called once at startup)
func (s *service) registerAllHandlers() {
	transformations := s.dag.GetTransformationNodes()
	externalModels := s.dag.GetExternalNodes()

	// Register transformation task handlers
	for _, transformation := range transformations {
		s.registerTransformationHandlers(transformation)
	}

	// Register external model task handlers
	for _, node := range externalModels {
		if model, ok := node.Model.(models.External); ok {
			s.registerExternalHandlers(model)
		}
	}

	// Register system task handlers
	s.registerSystemHandlers()
}

// buildDesiredTasks builds the map of desired scheduled tasks (without registering handlers)
func (s *service) buildDesiredTasks() map[string]string {
	transformations := s.dag.GetTransformationNodes()
	externalModels := s.dag.GetExternalNodes()
	desiredTasks := make(map[string]string, len(transformations)*2+len(externalModels)*2) // taskType -> schedule

	// Build transformation task schedules
	for _, transformation := range transformations {
		s.buildTransformationTasks(transformation, desiredTasks)
	}

	// Build external model task schedules
	for _, node := range externalModels {
		if model, ok := node.Model.(models.External); ok {
			s.buildExternalTasks(model, desiredTasks)
		}
	}

	// Build system task schedules
	s.buildSystemTasks(desiredTasks)

	return desiredTasks
}

// registerTransformationHandlers registers task handlers for a single transformation
func (s *service) registerTransformationHandlers(transformation models.Transformation) {
	config := transformation.GetConfig()
	modelID := transformation.GetID()

	if config.IsForwardFillEnabled() {
		forwardTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionForward)
		s.mux.HandleFunc(forwardTask, s.HandleScheduledForward)
		s.log.WithField("model_id", modelID).Debug("Registered forward fill handler")
	}

	if config.IsBackfillEnabled() {
		backfillTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionBack)
		s.mux.HandleFunc(backfillTask, s.HandleScheduledBackfill)
		s.log.WithField("model_id", modelID).Debug("Registered backfill handler")
	}
}

// buildTransformationTasks builds desired task schedules for a single transformation
func (s *service) buildTransformationTasks(transformation models.Transformation, desiredTasks map[string]string) {
	config := transformation.GetConfig()
	modelID := transformation.GetID()

	if config.IsForwardFillEnabled() {
		forwardTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionForward)
		desiredTasks[forwardTask] = config.GetForwardSchedule()
	}

	if config.IsBackfillEnabled() {
		backfillTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionBack)
		desiredTasks[backfillTask] = config.GetBackfillSchedule()
	}
}

// registerExternalHandlers registers task handlers for a single external model
func (s *service) registerExternalHandlers(model models.External) {
	config := model.GetConfig()
	modelID := model.GetID()

	if config.Cache == nil {
		return
	}

	if config.Cache.IncrementalScanInterval > 0 {
		incrementalTask := fmt.Sprintf("%s%s:incremental", ExternalTaskPrefix, modelID)
		s.mux.HandleFunc(incrementalTask, s.HandleExternalIncremental)
		s.log.WithField("model_id", modelID).Debug("Registered external incremental handler")
	}

	if config.Cache.FullScanInterval > 0 {
		fullTask := fmt.Sprintf("%s%s:full", ExternalTaskPrefix, modelID)
		s.mux.HandleFunc(fullTask, s.HandleExternalFull)
		s.log.WithField("model_id", modelID).Debug("Registered external full scan handler")
	}
}

// buildExternalTasks builds desired task schedules for a single external model
func (s *service) buildExternalTasks(model models.External, desiredTasks map[string]string) {
	config := model.GetConfig()
	modelID := model.GetID()

	if config.Cache == nil {
		return
	}

	if config.Cache.IncrementalScanInterval > 0 {
		incrementalTask := fmt.Sprintf("%s%s:incremental", ExternalTaskPrefix, modelID)
		schedule := fmt.Sprintf("@every %s", config.Cache.IncrementalScanInterval.String())
		desiredTasks[incrementalTask] = schedule
	}

	if config.Cache.FullScanInterval > 0 {
		fullTask := fmt.Sprintf("%s%s:full", ExternalTaskPrefix, modelID)
		schedule := fmt.Sprintf("@every %s", config.Cache.FullScanInterval.String())
		desiredTasks[fullTask] = schedule
	}
}

// registerSystemHandlers registers system task handlers
func (s *service) registerSystemHandlers() {
	if s.cfg.Consolidation != "" {
		s.mux.HandleFunc(ConsolidationTaskType, s.HandleConsolidation)
		s.log.Debug("Registered consolidation handler")
	}
}

// buildSystemTasks builds desired task schedules for system tasks
func (s *service) buildSystemTasks(desiredTasks map[string]string) {
	if s.cfg.Consolidation != "" {
		desiredTasks[ConsolidationTaskType] = s.cfg.Consolidation
	}
}

// reconcileTasks reconciles desired tasks with existing ones
func (s *service) reconcileTasks(desiredTasks map[string]string, existingTasks map[string]*asynq.SchedulerEntry) (reconcileStats, []error) {
	var errs []error
	stats := reconcileStats{}

	// First, handle desired tasks (add/update)
	for taskType, schedule := range desiredTasks {
		existingEntry := existingTasks[taskType]

		result, err := s.reconcileTask(taskType, schedule, existingEntry)
		if err != nil {
			errs = append(errs, err)
		}

		switch result {
		case taskReconcileRegistered:
			stats.registered++
		case taskReconcileUpdated:
			stats.updated++
		case taskReconcileSkipped:
			stats.skipped++
		}
	}

	// Second, remove obsolete tasks (exist but not desired)
	stats.removed = s.removeObsoleteTasks(desiredTasks, existingTasks)

	return stats, errs
}

// reconcileTask handles reconciling a single task - registering, updating, or skipping as needed
func (s *service) reconcileTask(taskType, schedule string, existingEntry *asynq.SchedulerEntry) (taskReconcileResult, error) {
	// New task if no existing entry
	if existingEntry == nil {
		if err := s.registerScheduledTask(taskType, schedule); err != nil {
			s.log.WithError(err).WithField("task_type", taskType).Error("Failed to register scheduled task")
			return taskReconcileSkipped, fmt.Errorf("failed to register %s: %w", taskType, err)
		}
		return taskReconcileRegistered, nil
	}

	// Task exists - check if schedule changed
	if existingEntry.Spec == schedule {
		// Schedule unchanged - skip to avoid disruption
		s.log.WithFields(logrus.Fields{
			"task_type": taskType,
			"schedule":  schedule,
		}).Debug("Skipping unchanged scheduled task")
		return taskReconcileSkipped, nil
	}

	// Schedule changed - need to update
	s.log.WithFields(logrus.Fields{
		"task_type":    taskType,
		"old_schedule": existingEntry.Spec,
		"new_schedule": schedule,
	}).Info("Schedule changed, updating task")

	// Unregister old task
	if err := s.scheduler.Unregister(existingEntry.ID); err != nil {
		s.log.WithError(err).WithField("task_type", taskType).Error("Failed to unregister old scheduled task")
	}

	// Register with new schedule
	if err := s.registerScheduledTask(taskType, schedule); err != nil {
		s.log.WithError(err).WithField("task_type", taskType).Error("Failed to register updated scheduled task")
		return taskReconcileSkipped, fmt.Errorf("failed to update %s: %w", taskType, err)
	}

	return taskReconcileUpdated, nil
}

// registerScheduledTask registers a new scheduled task
func (s *service) registerScheduledTask(taskType, schedule string) error {
	// Safety check: don't register tasks with empty schedules
	if schedule == "" {
		s.log.WithField("task_type", taskType).Debug("Skipping registration of task with empty schedule")
		return nil
	}

	// Create the task
	task := asynq.NewTask(taskType, nil)

	// Calculate appropriate unique window based on schedule
	uniqueWindow := calculateUniqueWindow(schedule)

	// Register with scheduler
	entryID, err := s.scheduler.Register(schedule, task,
		asynq.Queue(QueueName),     // Use dedicated scheduler queue
		asynq.Unique(uniqueWindow), // Prevent duplicate triggers
		asynq.MaxRetry(0),
	)

	if err != nil {
		return fmt.Errorf("failed to register %s with schedule %s: %w", taskType, schedule, err)
	}

	s.log.WithFields(logrus.Fields{
		"task_type":     taskType,
		"schedule":      schedule,
		"unique_window": uniqueWindow.String(),
	}).Debug("Registered scheduled task with calculated unique window")

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
		intervalBig, _ := rand.Int(rand.Reader, big.NewInt(60))
		interval := time.Duration(60+intervalBig.Int64()) * time.Second
		timer := time.NewTimer(interval)

		select {
		case <-s.done:
			timer.Stop()
			return
		case <-timer.C:
			// Only perform cleanup if we're the leader
			if !s.elector.IsLeader() {
				continue
			}

			// Get all scheduled task entries
			entries, err := s.inspector.SchedulerEntries()
			if err != nil {
				continue
			}

			// Group by task type to find duplicates
			taskGroups := make(map[string][]*asynq.SchedulerEntry, 10) // Add capacity hint
			for _, entry := range entries {
				// Only process our tasks (transformation, consolidation, or external)
				taskType := entry.Task.Type()
				if strings.HasPrefix(taskType, TransformationTaskPrefix) ||
					taskType == ConsolidationTaskType ||
					strings.HasPrefix(taskType, ExternalTaskPrefix) {
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

// HandleExternalIncremental processes incremental scan for external model
func (s *service) HandleExternalIncremental(_ context.Context, t *asynq.Task) error {
	// Extract model ID from task type: external:{model_id}:incremental
	taskType := t.Type()
	parts := strings.Split(taskType, ":")
	if len(parts) != 3 {
		return fmt.Errorf("%w: %s", ErrInvalidExternalTaskType, taskType)
	}
	modelID := parts[1]

	s.log.WithField("model_id", modelID).Debug("Running incremental scan for external model")

	// Process the incremental scan
	s.coordinator.ProcessExternalScan(modelID, "incremental")
	return nil
}

// HandleExternalFull processes full scan for external model
func (s *service) HandleExternalFull(_ context.Context, t *asynq.Task) error {
	// Extract model ID from task type: external:{model_id}:full
	taskType := t.Type()
	parts := strings.Split(taskType, ":")
	if len(parts) != 3 {
		return fmt.Errorf("%w: %s", ErrInvalidExternalTaskType, taskType)
	}
	modelID := parts[1]

	s.log.WithField("model_id", modelID).Debug("Running full scan for external model")

	// Process the full scan
	s.coordinator.ProcessExternalScan(modelID, "full")
	return nil
}

// removeObsoleteTasks removes scheduled tasks that exist but are no longer desired
func (s *service) removeObsoleteTasks(desiredTasks map[string]string, existingTasks map[string]*asynq.SchedulerEntry) int {
	removed := 0

	for taskType, entry := range existingTasks {
		// Check if this task is one we manage
		isOurTask := strings.HasPrefix(taskType, TransformationTaskPrefix) ||
			strings.HasPrefix(taskType, ExternalTaskPrefix) ||
			taskType == ConsolidationTaskType

		if !isOurTask {
			// Skip tasks we don't manage
			continue
		}

		// Check if task is still desired
		if _, exists := desiredTasks[taskType]; !exists {
			// Task exists but is no longer desired - remove it
			s.log.WithFields(logrus.Fields{
				"task_type": taskType,
				"entry_id":  entry.ID,
			}).Info("Removing obsolete scheduled task")

			if err := s.scheduler.Unregister(entry.ID); err != nil {
				s.log.WithError(err).WithField("task_type", taskType).Error("Failed to unregister obsolete task")
			} else {
				removed++
			}
		}
	}

	return removed
}

// calculateUniqueWindow calculates an appropriate unique window based on the schedule
func calculateUniqueWindow(schedule string) time.Duration {
	// Default for cron expressions or unparseable schedules
	const defaultWindow = 30 * time.Second

	// Check if it's an @every format
	if !strings.HasPrefix(schedule, "@every ") {
		return defaultWindow
	}

	// Parse the duration
	durationStr := strings.TrimPrefix(schedule, "@every ")
	interval, err := time.ParseDuration(durationStr)
	if err != nil {
		return defaultWindow
	}

	// Calculate 80% of interval as unique window
	uniqueWindow := time.Duration(float64(interval) * 0.8)

	// Apply bounds: minimum 1 second, maximum 5 minutes
	if uniqueWindow < time.Second {
		return time.Second
	}
	if uniqueWindow > 5*time.Minute {
		return 5 * time.Minute
	}

	return uniqueWindow
}

// triggerInitialExternalScans checks for external models without cache and triggers initial scans
func (s *service) triggerInitialExternalScans(ctx context.Context) {
	externalNodes := s.dag.GetExternalNodes()
	if len(externalNodes) == 0 {
		s.log.Info("No external models found for initial scan check")
		return
	}

	s.log.WithField("count", len(externalNodes)).Info("Checking external models for initial scans")

	// Check each external model for cache
	for _, node := range externalNodes {
		model, ok := node.Model.(models.External)
		if !ok {
			s.log.WithField("node", node).Debug("Node is not an external model")
			continue
		}

		modelID := model.GetID()
		config := model.GetConfig()

		// Skip if cache config not defined
		if config.Cache == nil {
			s.log.WithField("model_id", modelID).Debug("Skipping model without cache config")
			continue
		}

		// Check if cache exists using admin service
		if s.admin != nil {
			// Try to get bounds - if nil or initial scan incomplete, trigger initial scan
			bounds, err := s.admin.GetExternalBounds(ctx, modelID)
			logFields := logrus.Fields{
				"model_id":              modelID,
				"bounds_nil":            bounds == nil,
				"initial_scan_complete": bounds != nil && bounds.InitialScanComplete,
			}
			if err != nil {
				logFields["error"] = err
			}
			s.log.WithFields(logFields).Debug("Checked external model bounds")

			if bounds == nil || !bounds.InitialScanComplete {
				// Add jitter to prevent thundering herd (0-10 seconds)
				jitterBig, _ := rand.Int(rand.Reader, big.NewInt(10))
				jitter := time.Duration(jitterBig.Int64()) * time.Second

				s.log.WithFields(logrus.Fields{
					"model_id": modelID,
					"jitter":   jitter,
				}).Info("Triggering initial full scan for external model")

				// Sleep with jitter
				time.Sleep(jitter)

				// Trigger full scan immediately
				s.coordinator.ProcessExternalScan(modelID, "full")
			}
		} else {
			s.log.Debug("Admin service is nil, cannot check external bounds")
		}
	}
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
