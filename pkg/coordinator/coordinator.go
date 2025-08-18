// Package coordinator implements the coordinator functionality for CBT
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
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
	// Asynq scheduler components
	scheduler       *asynq.Scheduler
	schedulerServer *asynq.Server
	schedulerMux    *asynq.ServeMux
	redisOpt        *asynq.RedisClientOpt

	// Core components
	queueManager *tasks.QueueManager
	depManager   *dependencies.DependencyGraph
	validator    validation.DependencyValidator
	adminManager *clickhouse.AdminTableManager
	inspector    *asynq.Inspector

	// Synchronization
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *logrus.Logger

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
	redisOpt *asynq.RedisClientOpt,
	logger *logrus.Logger,
) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())

	return &Coordinator{
		queueManager:   queueManager,
		depManager:     depManager,
		validator:      validator,
		adminManager:   adminManager,
		inspector:      inspector,
		redisOpt:       redisOpt,
		processedTasks: make(map[string]bool),
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
	}
}

// Start begins the coordinator's scheduling and monitoring activities
func (c *Coordinator) Start() error {
	c.logger.Info("Starting coordinator...")

	// Initialize scheduler and reconcile with current config
	if err := c.initializeScheduler(); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	// Start the scheduler (manages cron job registrations)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.logger.Info("Starting Asynq scheduler")
		if err := c.scheduler.Run(); err != nil {
			c.logger.WithError(err).Error("Asynq scheduler stopped with error")
		}
	}()

	// Start the server (processes scheduled trigger tasks) with the mux created during reconciliation
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.logger.Info("Starting scheduler task processor")
		if err := c.schedulerServer.Run(c.schedulerMux); err != nil {
			c.logger.WithError(err).Error("Scheduler task processor stopped with error")
		}
	}()

	// Keep existing completed task polling
	c.wg.Add(1)
	go c.pollCompletedTasks()

	c.logger.Info("Coordinator started successfully")
	return nil
}

// Stop gracefully shuts down the coordinator
func (c *Coordinator) Stop() error {
	c.logger.Info("Stopping coordinator...")

	// Cancel context to stop all goroutines
	c.cancel()

	// Shutdown scheduler gracefully
	if c.scheduler != nil {
		c.scheduler.Shutdown()
		c.logger.Info("Scheduler shutdown complete")
	}

	// Shutdown server gracefully
	if c.schedulerServer != nil {
		c.schedulerServer.Shutdown()
		c.logger.Info("Scheduler server shutdown complete")
	}

	// Wait for all goroutines to finish
	c.wg.Wait()

	c.logger.Info("Coordinator stopped")
	return nil
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
	if modelConfig.Backfill != nil && modelConfig.Backfill.Enabled {
		c.checkBackfillOpportunities(modelID, &modelConfig)
	}
}

func (c *Coordinator) checkAndEnqueuePositionWithTrigger(modelID string, position, interval uint64, trigger string) {
	// Create task payload
	// Mark as backfill if trigger is "backfill"
	isBackfill := trigger == "backfill"
	payload := tasks.TaskPayload{
		ModelID:    modelID,
		Position:   position,
		Interval:   interval,
		EnqueuedAt: time.Now(),
		IsBackfill: isBackfill,
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

	// Use the maximum of the configured minimum and the calculated initial position
	if modelConfig.Backfill != nil && modelConfig.Backfill.Minimum > initialPos {
		initialPos = modelConfig.Backfill.Minimum
		c.logger.WithFields(logrus.Fields{
			"model_id":         modelID,
			"initial_pos":      initialPos,
			"backfill_minimum": modelConfig.Backfill.Minimum,
		}).Debug("Using configured minimum position for gap scanning")
	} else {
		c.logger.WithFields(logrus.Fields{
			"model_id":    modelID,
			"initial_pos": initialPos,
		}).Debug("Got initial position for gap scanning")
	}

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

func (c *Coordinator) checkBackfillForModel(modelID string) {
	modelConfig, exists := c.depManager.GetModelConfig(modelID)
	if !exists || modelConfig.Backfill == nil || !modelConfig.Backfill.Enabled {
		return
	}

	c.logger.WithField("model_id", modelID).Debug("Scanning for gaps")
	c.checkBackfillOpportunities(modelID, &modelConfig)
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
