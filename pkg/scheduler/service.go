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
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/ethpandaops/cbt/pkg/observability"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
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
	// TransformationTypeScheduled is the transformation type for scheduled transformations
	TransformationTypeScheduled = "scheduled"
)

var (
	// ErrNotScheduledType is returned when transformation is not a scheduled type
	ErrNotScheduledType = errors.New("transformation is not a scheduled type")
	// ErrCoordinatorNoQueueSupport is returned when coordinator doesn't support queue management
	ErrCoordinatorNoQueueSupport = errors.New("coordinator doesn't support queue management")
	// ErrQueueManagerNil is returned when queue manager is nil
	ErrQueueManagerNil = errors.New("queue manager is nil")
	// ErrQueueNoTransformationSupport is returned when queue doesn't support transformation enqueueing
	ErrQueueNoTransformationSupport = errors.New("queue manager doesn't support transformation enqueueing")
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

	client    *asynq.Client
	server    *asynq.Server
	mux       *asynq.ServeMux
	inspector *asynq.Inspector

	tracker      scheduleTracker
	ticker       tickerService
	tickerCancel context.CancelFunc // To stop ticker when demoted

	elector LeaderElector
}

// NewService creates a new scheduler service
func NewService(log logrus.FieldLogger, cfg *Config, redisOpt *redis.Options, dag models.DAGReader, coord coordinator.Service, adminService admin.Service) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	asynqRedis := r.NewAsynqRedisOptions(redisOpt)

	// Create client for enqueueing tasks
	client := asynq.NewClient(asynqRedis)

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

	// Create Redis client for schedule tracker
	redisClient := redis.NewClient(redisOpt)

	// Create schedule tracker
	tracker := newScheduleTracker(log, redisClient)

	return &service{
		log:  log.WithField("service", "scheduler"), // Add service-specific field per ethPandaOps
		cfg:  cfg,
		done: make(chan struct{}),

		redisOpt:    redisOpt,
		dag:         dag,
		coordinator: coord,
		admin:       adminService,

		client:       client,
		server:       server,
		mux:          mux,
		inspector:    inspector,
		tracker:      tracker,
		ticker:       nil, // Created on leader promotion
		tickerCancel: nil,
		elector:      elector,
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

	// Shutdown server first
	if s.server != nil {
		s.server.Shutdown()
	}

	// Wait for all goroutines to complete BEFORE closing asynq client with timeout
	// This prevents "redis: client is closed" errors from ticker goroutines
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	timeout := time.Duration(s.cfg.ShutdownTimeout) * time.Second
	select {
	case <-done:
		s.log.Info("Scheduler goroutines stopped successfully")
	case <-time.After(timeout):
		s.log.Warnf("Scheduler shutdown timed out after %v, forcing shutdown", timeout)
	}

	// Now safe to close asynq client
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			s.log.WithError(err).Warn("Failed to close asynq client")
		}
	}

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

	for {
		select {
		case <-s.done:
			return

		case <-ctx.Done():
			return

		case <-promoted:
			s.log.Info("Promoted to leader, starting ticker service")

			// Build task list from current DAG config
			scheduledTasks := s.buildScheduledTasks()

			// Create ticker service with current task list
			s.ticker = newTickerService(s.log, s.tracker, s.client, scheduledTasks)

			// Start ticker in goroutine
			tickerCtx, cancel := context.WithCancel(ctx)
			s.tickerCancel = cancel
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				if err := s.ticker.Start(tickerCtx); err != nil && !errors.Is(err, context.Canceled) {
					s.log.WithError(err).Error("Ticker service error")
				}
			}()

			// Trigger initial external scans (keep this - still needed)
			s.triggerInitialExternalScans(ctx)

		case <-demoted:
			s.log.Info("Demoted from leader, stopping ticker service")

			// Stop ticker
			if s.tickerCancel != nil {
				s.tickerCancel()
				s.tickerCancel = nil
			}
			if s.ticker != nil {
				if err := s.ticker.Stop(); err != nil {
					s.log.WithError(err).Error("Failed to stop ticker")
				}
				s.ticker = nil
			}
		}
	}
}

// buildScheduledTasks constructs the list of scheduled tasks from DAG config
func (s *service) buildScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	// System tasks
	scheduledTasks = append(scheduledTasks, s.buildSystemScheduledTasks()...)

	// External model tasks
	scheduledTasks = append(scheduledTasks, s.buildExternalScheduledTasks()...)

	// Transformation tasks
	scheduledTasks = append(scheduledTasks, s.buildTransformationScheduledTasks()...)

	return scheduledTasks
}

// buildSystemScheduledTasks creates scheduled tasks for system operations
func (s *service) buildSystemScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	// Consolidation task
	if s.cfg.Consolidation != "" {
		interval, err := parseScheduleInterval(s.cfg.Consolidation)
		if err != nil {
			s.log.WithError(err).Error("Invalid consolidation schedule")
		} else {
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       ConsolidationTaskType,
				Schedule: s.cfg.Consolidation,
				Interval: interval,
				Task:     asynq.NewTask(ConsolidationTaskType, nil),
				Queue:    QueueName,
			})
		}
	}

	return scheduledTasks
}

// buildExternalScheduledTasks creates scheduled tasks for external model scans
func (s *service) buildExternalScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	for _, node := range s.dag.GetExternalNodes() {
		model, ok := node.Model.(models.External)
		if !ok {
			continue
		}

		config := model.GetConfig()
		if config.Cache == nil {
			continue
		}

		modelID := model.GetID()

		// Incremental scan
		if config.Cache.IncrementalScanInterval > 0 {
			schedule := fmt.Sprintf("@every %s", config.Cache.IncrementalScanInterval.String())
			taskID := fmt.Sprintf("%s%s:incremental", ExternalTaskPrefix, modelID)
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       taskID,
				Schedule: schedule,
				Interval: config.Cache.IncrementalScanInterval,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			})
		}

		// Full scan
		if config.Cache.FullScanInterval > 0 {
			schedule := fmt.Sprintf("@every %s", config.Cache.FullScanInterval.String())
			taskID := fmt.Sprintf("%s%s:full", ExternalTaskPrefix, modelID)
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       taskID,
				Schedule: schedule,
				Interval: config.Cache.FullScanInterval,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			})
		}
	}

	return scheduledTasks
}

// buildTransformationScheduledTasks creates scheduled tasks for transformations
func (s *service) buildTransformationScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	for _, node := range s.dag.GetTransformationNodes() {
		trans := node
		modelID := trans.GetID()
		handler := trans.GetHandler()
		config := trans.GetConfig()

		// Handle scheduled transformations
		if config.Type == TransformationTypeScheduled {
			type scheduleProvider interface {
				GetSchedule() string
			}
			provider, ok := handler.(scheduleProvider)
			if !ok || provider == nil {
				continue
			}

			schedule := provider.GetSchedule()
			if schedule == "" {
				continue
			}

			interval, err := parseScheduleInterval(schedule)
			if err != nil {
				s.log.WithError(err).
					WithField("model_id", modelID).
					Error("Invalid schedule")
				continue
			}

			taskID := fmt.Sprintf("%s%s:scheduled", TransformationTaskPrefix, modelID)
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       taskID,
				Schedule: schedule,
				Interval: interval,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			})
			continue
		}

		// Handle incremental transformations
		if handler == nil {
			continue
		}

		type configProvider interface {
			Config() interface{}
		}
		cfgProvider, ok := handler.(configProvider)
		if !ok {
			continue
		}

		cfg, ok := cfgProvider.Config().(*incremental.Config)
		if !ok || cfg.Schedules == nil {
			continue
		}

		// Forward fill
		if cfg.Schedules.ForwardFill != "" {
			interval, err := parseScheduleInterval(cfg.Schedules.ForwardFill)
			if err != nil {
				s.log.WithError(err).
					WithField("model_id", modelID).
					Error("Invalid forward fill schedule")
			} else {
				taskID := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionForward)
				scheduledTasks = append(scheduledTasks, scheduledTask{
					ID:       taskID,
					Schedule: cfg.Schedules.ForwardFill,
					Interval: interval,
					Task:     asynq.NewTask(taskID, nil),
					Queue:    QueueName,
				})
			}
		}

		// Backfill
		if cfg.Schedules.Backfill != "" {
			interval, err := parseScheduleInterval(cfg.Schedules.Backfill)
			if err != nil {
				s.log.WithError(err).
					WithField("model_id", modelID).
					Error("Invalid backfill schedule")
			} else {
				taskID := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionBack)
				scheduledTasks = append(scheduledTasks, scheduledTask{
					ID:       taskID,
					Schedule: cfg.Schedules.Backfill,
					Interval: interval,
					Task:     asynq.NewTask(taskID, nil),
					Queue:    QueueName,
				})
			}
		}
	}

	return scheduledTasks
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
	handler := transformation.GetHandler()

	// Handle scheduled transformations
	if config.Type == "scheduled" {
		scheduledTask := fmt.Sprintf("%s%s:scheduled", TransformationTaskPrefix, modelID)
		s.mux.HandleFunc(scheduledTask, s.HandleScheduledTransformation)
		s.log.WithField("model_id", modelID).Debug("Registered scheduled transformation handler")
		return
	}

	// Handle incremental transformations
	if handler == nil {
		return
	}

	cfg, ok := handler.Config().(*incremental.Config)
	if !ok || cfg.Schedules == nil {
		return
	}

	if cfg.Schedules.ForwardFill != "" {
		forwardTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionForward)
		s.mux.HandleFunc(forwardTask, s.HandleScheduledForward)
		s.log.WithField("model_id", modelID).Debug("Registered forward fill handler")
	}
	if cfg.Schedules.Backfill != "" {
		backfillTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionBack)
		s.mux.HandleFunc(backfillTask, s.HandleScheduledBackfill)
		s.log.WithField("model_id", modelID).Debug("Registered backfill handler")
	}
}

// buildTransformationTasks builds desired task schedules for a single transformation
func (s *service) buildTransformationTasks(transformation models.Transformation, desiredTasks map[string]string) {
	config := transformation.GetConfig()
	modelID := transformation.GetID()
	handler := transformation.GetHandler()

	// Handle scheduled transformations
	if config.Type == "scheduled" {
		s.buildScheduledTransformationTasks(modelID, handler, desiredTasks)
		return
	}

	// Handle incremental transformations
	s.buildIncrementalTransformationTasks(modelID, handler, desiredTasks)
}

// buildScheduledTransformationTasks builds tasks for scheduled transformations
func (s *service) buildScheduledTransformationTasks(modelID string, handler interface{}, desiredTasks map[string]string) {
	if handler == nil {
		return
	}

	type scheduleProvider interface {
		GetSchedule() string
	}
	provider, ok := handler.(scheduleProvider)
	if !ok {
		return
	}

	schedule := provider.GetSchedule()
	if schedule != "" {
		scheduledTask := fmt.Sprintf("%s%s:scheduled", TransformationTaskPrefix, modelID)
		desiredTasks[scheduledTask] = schedule
	}
}

// buildIncrementalTransformationTasks builds tasks for incremental transformations
func (s *service) buildIncrementalTransformationTasks(modelID string, handler interface{}, desiredTasks map[string]string) {
	if handler == nil {
		return
	}

	// Type assert handler to get config
	type configProvider interface {
		Config() interface{}
	}
	provider, ok := handler.(configProvider)
	if !ok {
		return
	}

	cfg, ok := provider.Config().(*incremental.Config)
	if !ok || cfg.Schedules == nil {
		return
	}

	if cfg.Schedules.ForwardFill != "" {
		forwardTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionForward)
		desiredTasks[forwardTask] = cfg.Schedules.ForwardFill
	}
	if cfg.Schedules.Backfill != "" {
		backfillTask := fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, coordinator.DirectionBack)
		desiredTasks[backfillTask] = cfg.Schedules.Backfill
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

// extractExternalTaskComponents extracts the model ID from external task types.
// External tasks follow the format: external:{model_id}:suffix
// Returns the model ID and an error if the format is invalid.
func extractExternalTaskComponents(taskType string) (modelID string, err error) {
	parts := strings.Split(taskType, ":")
	if len(parts) != 3 {
		return "", fmt.Errorf("%w: %s", ErrInvalidExternalTaskType, taskType)
	}

	return parts[1], nil
}

// HandleScheduledForward processes scheduled forward fill checks
func (s *service) HandleScheduledForward(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")

		return err
	}

	s.log.WithField("model_id", modelID).Debug("Processing scheduled forward check")

	// This triggers the existing forward fill logic
	s.coordinator.Process(trans, coordinator.DirectionForward)

	// Record metrics
	observability.RecordScheduledTaskExecution(modelID, string(coordinator.DirectionForward), "success")

	return nil
}

// HandleExternalIncremental processes incremental scan for external model
func (s *service) HandleExternalIncremental(_ context.Context, t *asynq.Task) error {
	modelID, err := extractExternalTaskComponents(t.Type())
	if err != nil {
		return err
	}

	s.log.WithField("model_id", modelID).Debug("Running incremental scan for external model")

	// Process the incremental scan
	s.coordinator.ProcessExternalScan(modelID, "incremental")
	return nil
}

// HandleExternalFull processes full scan for external model
func (s *service) HandleExternalFull(_ context.Context, t *asynq.Task) error {
	modelID, err := extractExternalTaskComponents(t.Type())
	if err != nil {
		return err
	}

	s.log.WithField("model_id", modelID).Debug("Running full scan for external model")

	// Process the full scan
	s.coordinator.ProcessExternalScan(modelID, "full")
	return nil
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

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")

		return err
	}

	s.log.WithField("model_id", modelID).Debug("Processing scheduled backfill scan")

	// This triggers the existing backfill logic
	s.coordinator.Process(trans, coordinator.DirectionBack)

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

// HandleScheduledTransformation handles the execution of scheduled (cron-based) transformations
func (s *service) HandleScheduledTransformation(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")
		return err
	}

	config := trans.GetConfig()

	// Verify this is a scheduled transformation
	if !config.IsScheduledType() {
		s.log.WithField("model_id", modelID).Error("Task is not a scheduled transformation")
		return fmt.Errorf("%w: %s", ErrNotScheduledType, modelID)
	}

	currentTime := time.Now()
	s.log.WithFields(logrus.Fields{
		"model_id":       modelID,
		"type":           "scheduled",
		"execution_time": currentTime.Format(time.RFC3339),
	}).Debug("Processing scheduled transformation")

	// For scheduled transformations, we create a scheduled task payload
	taskPayload := tasks.ScheduledTaskPayload{
		ModelID:       modelID,
		ExecutionTime: currentTime,
		EnqueuedAt:    currentTime,
	}

	// Try to enqueue through coordinator's queue manager
	err = s.enqueueScheduledTask(taskPayload)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to enqueue scheduled transformation task")
		return err
	}

	s.log.WithFields(logrus.Fields{
		"model_id":       modelID,
		"execution_time": currentTime.Format(time.RFC3339),
	}).Info("Enqueued scheduled transformation task")

	return nil
}

// enqueueScheduledTask attempts to enqueue a scheduled transformation task
func (s *service) enqueueScheduledTask(payload tasks.TaskPayload) error {
	// Check if coordinator has GetQueueManager method
	type queueManagerGetter interface {
		GetQueueManager() interface{}
	}

	qmGetter, ok := s.coordinator.(queueManagerGetter)
	if !ok {
		return ErrCoordinatorNoQueueSupport
	}

	queueManager := qmGetter.GetQueueManager()
	if queueManager == nil {
		return ErrQueueManagerNil
	}

	// Check if queue manager supports EnqueueTransformation
	type transformationEnqueuer interface {
		EnqueueTransformation(payload tasks.TaskPayload, opts ...asynq.Option) error
	}

	enqueuer, ok := queueManager.(transformationEnqueuer)
	if !ok {
		return ErrQueueNoTransformationSupport
	}

	// Both scheduled and incremental transformations use the same uniqueness strategy:
	// The unique ID is based on model_id:direction
	// For scheduled: just model_id (preventing overlapping runs of same scheduled task)
	// For incremental: model_id:direction (preventing duplicate forward/backfill tasks)
	// Use 1 second uniqueness - just prevents rapid-fire duplicates, task ID handles actual deduplication
	return enqueuer.EnqueueTransformation(payload, asynq.Unique(1*time.Second))
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
