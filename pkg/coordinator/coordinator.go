// Package coordinator implements the coordinator functionality for CBT
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// Coordinator manages task scheduling and dependency coordination
type Coordinator struct {
	queueManager *tasks.QueueManager
	depManager   *dependencies.DependencyGraph
	validator    validation.DependencyValidator
	adminManager *clickhouse.AdminTableManager
	inspector    *asynq.Inspector
	schedulers   map[string]*time.Timer
	mutex        sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	logger       *logrus.Logger
	// Track last processed task IDs to avoid duplicate processing
	processedTasks map[string]bool
	taskMutex      sync.RWMutex
}

// NewCoordinator creates a new coordinator
func NewCoordinator(
	queueManager *tasks.QueueManager,
	depManager *dependencies.DependencyGraph,
	validator validation.DependencyValidator,
	adminManager *clickhouse.AdminTableManager,
	inspector *asynq.Inspector,
	logger *logrus.Logger,
) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())

	return &Coordinator{
		queueManager:   queueManager,
		depManager:     depManager,
		validator:      validator,
		adminManager:   adminManager,
		inspector:      inspector,
		schedulers:     make(map[string]*time.Timer),
		processedTasks: make(map[string]bool),
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
	}
}

// Start begins the coordinator's scheduling and monitoring activities
func (c *Coordinator) Start() error {
	c.logger.Info("Starting coordinator...")

	// Start polling for completed tasks
	c.wg.Add(1)
	go c.pollCompletedTasks()

	// Start periodic backfill scanning
	c.wg.Add(1)
	go c.periodicBackfillScan()

	// Start schedulers for all transformation models
	transformationModels := c.depManager.GetTransformationModels()
	for _, modelID := range transformationModels {
		if err := c.startModelScheduler(modelID); err != nil {
			c.logger.WithError(err).WithField("model_id", modelID).Warn("Failed to start scheduler")
		}
	}

	c.logger.WithField("scheduler_count", len(c.schedulers)).Info("Coordinator started")
	return nil
}

// Stop gracefully shuts down the coordinator
func (c *Coordinator) Stop() error {
	c.logger.Info("Stopping coordinator...")

	c.cancel()

	// Stop all schedulers
	c.mutex.Lock()
	for modelID, timer := range c.schedulers {
		timer.Stop()
		c.logger.WithField("model_id", modelID).Debug("Stopped scheduler")
	}
	c.schedulers = make(map[string]*time.Timer)
	c.mutex.Unlock()

	// Wait for completion handler
	c.wg.Wait()

	c.logger.Info("Coordinator stopped")
	return nil
}

func (c *Coordinator) startModelScheduler(modelID string) error {
	modelConfig, exists := c.depManager.GetModelConfig(modelID)
	if !exists {
		return fmt.Errorf("%w: %s", models.ErrModelNotFound, modelID)
	}

	if modelConfig.External {
		return nil // External models don't need scheduling
	}

	// Parse schedule
	interval, err := c.parseSchedule(modelConfig.Schedule)
	if err != nil {
		return fmt.Errorf("invalid schedule for %s: %w", modelID, err)
	}

	// Create and start timer
	timer := time.NewTimer(interval)
	c.mutex.Lock()
	c.schedulers[modelID] = timer
	c.mutex.Unlock()

	c.wg.Add(1)
	go c.runModelScheduler(modelID, timer, interval)

	c.logger.WithFields(logrus.Fields{
		"model_id": modelID,
		"interval": interval,
	}).Info("Started model scheduler")

	// Record scheduler is active
	observability.SchedulerActive.WithLabelValues(modelID).Set(1)

	return nil
}

func (c *Coordinator) runModelScheduler(modelID string, timer *time.Timer, interval time.Duration) {
	defer c.wg.Done()
	defer func() {
		// Record scheduler is inactive when it stops
		observability.SchedulerActive.WithLabelValues(modelID).Set(0)
	}()

	// Run immediately
	c.checkAndEnqueueModel(modelID)

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			c.checkAndEnqueueModel(modelID)
			timer.Reset(interval)
		}
	}
}

func (c *Coordinator) checkAndEnqueueModel(modelID string) {
	modelConfig, exists := c.depManager.GetModelConfig(modelID)
	if !exists {
		c.logger.WithField("model_id", modelID).Warn("Model not found during scheduled check")
		return
	}

	// Get last processed position
	lastPos, err := c.adminManager.GetLastPosition(c.ctx, modelID)
	if err != nil {
		c.logger.WithError(err).WithField("model_id", modelID).Error("Failed to get last position")
		return
	}

	c.logger.WithFields(logrus.Fields{
		"model_id": modelID,
		"last_pos": lastPos,
	}).Debug("Got last position from admin table")

	// If this is the first run, calculate initial position
	if lastPos == 0 {
		c.logger.WithField("model_id", modelID).Debug("Last position is 0, calculating initial position")
		initialPos, err := c.validator.GetInitialPosition(c.ctx, modelID)
		if err != nil {
			c.logger.WithError(err).WithField("model_id", modelID).Error("Failed to calculate initial position")
			return
		}
		c.logger.WithFields(logrus.Fields{
			"model_id":    modelID,
			"initial_pos": initialPos,
		}).Debug("Calculated initial position")
		lastPos = initialPos
	}

	// Check forward processing
	nextPos := lastPos
	c.checkAndEnqueuePosition(modelID, nextPos, modelConfig.Interval)

	// Check backfill if enabled
	if modelConfig.Backfill {
		c.checkBackfillOpportunities(modelID, &modelConfig)
	}
}

func (c *Coordinator) checkAndEnqueuePositionWithTrigger(modelID string, position, interval uint64, trigger string) {
	// Create task payload
	payload := tasks.TaskPayload{
		ModelID:    modelID,
		Position:   position,
		Interval:   interval,
		EnqueuedAt: time.Now(),
	}

	// Check if already enqueued or recently completed
	isPending, err := c.queueManager.IsTaskPendingOrRunning(payload.UniqueID())
	if err != nil {
		c.logger.WithError(err).WithField("task_id", payload.UniqueID()).Error("Failed to check task status")
		observability.RecordError("coordinator", "task_status_check_error")
		return
	}

	if isPending {
		c.logger.WithField("task_id", payload.UniqueID()).Debug("Task already pending or running")
		return
	}

	// Validate dependencies
	depStartTime := time.Now()
	validationResult, err := c.validator.ValidateDependencies(c.ctx, modelID, position, interval)
	depDuration := time.Since(depStartTime).Seconds()

	if err != nil {
		c.logger.WithError(err).WithFields(logrus.Fields{
			"model_id": modelID,
			"position": position,
		}).Error("Failed to validate dependencies")
		observability.RecordDependencyValidation(modelID, "error", depDuration)
		observability.RecordError("coordinator", "dependency_validation_error")
		return
	}

	if !validationResult.CanProcess {
		c.logger.WithFields(logrus.Fields{
			"model_id": modelID,
			"position": position,
		}).Debug("Dependencies not satisfied")
		observability.RecordDependencyValidation(modelID, "not_satisfied", depDuration)
		return
	}

	observability.RecordDependencyValidation(modelID, "satisfied", depDuration)

	// Enqueue task
	if err := c.queueManager.EnqueueTransformation(payload); err != nil {
		c.logger.WithError(err).WithField("model_id", modelID).Error("Failed to enqueue task")
		observability.RecordError("coordinator", "enqueue_error")
		return
	}

	// Record successful enqueue with trigger type
	observability.RecordTaskEnqueued(modelID, trigger)

	c.logger.WithFields(logrus.Fields{
		"model_id": modelID,
		"position": position,
		"interval": interval,
	}).Info("Enqueued transformation task")
}

func (c *Coordinator) checkAndEnqueuePosition(modelID string, position, interval uint64) {
	// Default to "schedule" trigger for backwards compatibility
	c.checkAndEnqueuePositionWithTrigger(modelID, position, interval, "schedule")
}

func (c *Coordinator) checkBackfillOpportunities(modelID string, modelConfig *models.ModelConfig) {
	// Get the range to check for gaps
	lastPos, err := c.adminManager.GetLastPosition(c.ctx, modelID)
	if err != nil {
		c.logger.WithError(err).WithField("model_id", modelID).Debug("Failed to get last position for gap scan")
		return
	}

	c.logger.WithFields(logrus.Fields{
		"model_id": modelID,
		"last_pos": lastPos,
		"interval": modelConfig.Interval,
	}).Debug("Got last position for gap scanning")

	if lastPos < modelConfig.Interval {
		c.logger.WithField("model_id", modelID).Debug("No data yet, skipping gap scan")
		return // No data yet
	}

	// Get initial position to determine scan range
	initialPos, err := c.validator.GetInitialPosition(c.ctx, modelID)
	if err != nil {
		c.logger.WithError(err).WithField("model_id", modelID).Debug("Failed to get initial position for gap scan")
		return
	}

	c.logger.WithFields(logrus.Fields{
		"model_id":    modelID,
		"initial_pos": initialPos,
	}).Debug("Got initial position for gap scanning")

	// Find all gaps in the processed data
	gaps, err := c.adminManager.FindGaps(c.ctx, modelID, initialPos, lastPos, modelConfig.Interval)
	if err != nil {
		c.logger.WithError(err).WithField("model_id", modelID).Error("Failed to find gaps")
		return
	}

	// Process each gap
	for _, gap := range gaps {
		// Process the gap in intervals
		for pos := gap.StartPos; pos < gap.EndPos; {
			// Calculate remaining gap size
			gapSize := gap.EndPos - pos
			intervalToUse := modelConfig.Interval

			// If gap is smaller than model interval, use gap size to avoid overlap
			if gapSize < modelConfig.Interval {
				intervalToUse = gapSize
			}

			c.logger.WithFields(logrus.Fields{
				"model_id":       modelID,
				"gap_start":      gap.StartPos,
				"gap_end":        gap.EndPos,
				"position":       pos,
				"interval":       intervalToUse,
				"model_interval": modelConfig.Interval,
				"gap_size":       gapSize,
			}).Info("Found backfill opportunity")

			c.checkAndEnqueuePositionWithTrigger(modelID, pos, intervalToUse, "backfill")
			pos += intervalToUse
		}
	}
}

func (c *Coordinator) pollCompletedTasks() {
	defer c.wg.Done()

	ticker := time.NewTicker(5 * time.Second) // Poll every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkCompletedTasks()
		}
	}
}

func (c *Coordinator) periodicBackfillScan() {
	defer c.wg.Done()

	// Wait initial period for system to stabilize
	select {
	case <-c.ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}

	ticker := time.NewTicker(30 * time.Second) // Scan for gaps every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.scanAllModelsForGaps()
		}
	}
}

func (c *Coordinator) scanAllModelsForGaps() {
	transformationModels := c.depManager.GetTransformationModels()

	for _, modelID := range transformationModels {
		modelConfig, exists := c.depManager.GetModelConfig(modelID)
		if !exists || !modelConfig.Backfill {
			continue
		}

		c.logger.WithField("model_id", modelID).Debug("Scanning for gaps")
		c.checkBackfillOpportunities(modelID, &modelConfig)
	}
}

func (c *Coordinator) checkCompletedTasks() {
	// Get all model queues
	transformationModels := c.depManager.GetTransformationModels()

	for _, modelID := range transformationModels {
		// List completed tasks for this model's queue
		completedTasks, err := c.inspector.ListCompletedTasks(modelID, asynq.PageSize(100))
		if err != nil {
			// Queue might not exist yet, that's ok
			continue
		}

		for _, taskInfo := range completedTasks {
			// Check if we've already processed this task
			c.taskMutex.RLock()
			processed := c.processedTasks[taskInfo.ID]
			c.taskMutex.RUnlock()

			if !processed {
				// Parse the task payload to get position and interval
				var payload tasks.TaskPayload
				if err := json.Unmarshal(taskInfo.Payload, &payload); err == nil {
					c.onTaskComplete(payload.ModelID, payload.Position, payload.Interval)
				}

				// Mark as processed
				c.taskMutex.Lock()
				c.processedTasks[taskInfo.ID] = true
				// Clean up old entries if map gets too large
				if len(c.processedTasks) > 10000 {
					c.processedTasks = make(map[string]bool)
				}
				c.taskMutex.Unlock()
			}
		}
	}
}

func (c *Coordinator) onTaskComplete(modelID string, position, _ uint64) {
	c.logger.WithFields(logrus.Fields{
		"model_id": modelID,
		"position": position,
	}).Debug("Task completed, checking dependents")

	// Update model position metrics
	observability.ModelLastPosition.WithLabelValues(modelID).Set(float64(position))

	// Get models that depend on this one
	dependents := c.depManager.GetDependents(modelID)

	for _, depModelID := range dependents {
		depConfig, exists := c.depManager.GetModelConfig(depModelID)
		if !exists {
			continue
		}

		// Calculate next position for dependent
		lastPos, err := c.adminManager.GetLastPosition(c.ctx, depModelID)
		if err != nil {
			continue
		}

		nextPos := lastPos
		if nextPos == 0 {
			// First run - calculate initial position
			initialPos, err := c.validator.GetInitialPosition(c.ctx, depModelID)
			if err != nil {
				continue
			}
			nextPos = initialPos
		}

		// Check if this completion unblocks the dependent
		c.checkAndEnqueuePositionWithTrigger(depModelID, nextPos, depConfig.Interval, "dependency")
	}
}

func (c *Coordinator) parseSchedule(schedule string) (time.Duration, error) {
	// Handle numeric values (seconds)
	if duration, err := strconv.Atoi(schedule); err == nil {
		return time.Duration(duration) * time.Second, nil
	}

	// Handle descriptive schedules
	switch schedule {
	case "@hourly":
		return time.Hour, nil
	case "@daily":
		return 24 * time.Hour, nil
	case "@weekly":
		return 7 * 24 * time.Hour, nil
	}

	// Handle @every format
	if strings.HasPrefix(schedule, "@every ") {
		durationStr := strings.TrimPrefix(schedule, "@every ")
		return time.ParseDuration(durationStr)
	}

	// For cron expressions, default to 5 minutes
	if strings.Contains(schedule, "*") {
		return 5 * time.Minute, nil
	}

	return 0, fmt.Errorf("%w: %s", ErrUnsupportedSchedule, schedule)
}
