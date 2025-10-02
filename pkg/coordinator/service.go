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
	"github.com/ethpandaops/cbt/pkg/models/transformation"
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

	// ProcessExternalScan handles external model scan processing
	ProcessExternalScan(modelID, scanType string)
}

// Direction represents the processing direction for tasks
type Direction string

const (
	// DirectionForward processes tasks in forward direction
	DirectionForward Direction = "forward"
	// DirectionBack processes tasks in backward direction
	DirectionBack Direction = "back"

	// ExternalIncrementalTaskType is the task type for external incremental scan
	ExternalIncrementalTaskType = "external:incremental"
	// ExternalFullTaskType is the task type for external full scan
	ExternalFullTaskType = "external:full"
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
	admin     admin.Service
	validator validation.Validator

	queueManager   *tasks.QueueManager
	inspector      *asynq.Inspector
	archiveHandler ArchiveHandler
}

// NewService creates a new coordinator service
func NewService(log logrus.FieldLogger, redisOpt *redis.Options, dag models.DAGReader, adminService admin.Service, validator validation.Validator) (Service, error) {
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

	archiveHandler, err := NewArchiveHandler(s.log, s.redisOpt)
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
func (s *service) Process(trans models.Transformation, direction Direction) {
	switch direction {
	case DirectionForward:
		s.processForward(trans)
	case DirectionBack:
		s.processBack(trans)
	}
}

func (s *service) processForward(trans models.Transformation) {
	config := trans.GetConfig()

	if !config.IsForwardFillEnabled() {
		return
	}

	ctx := context.Background()
	modelID := trans.GetID()

	// Get starting position
	position, err := s.admin.GetNextUnprocessedPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get position")
		return
	}

	// Calculate initial position if needed
	if position == 0 {
		initialPos, err := s.validator.GetInitialPosition(ctx, modelID)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get initial position")
			return
		}
		position = initialPos
	}

	// Check limits
	if config.Limits != nil && config.Limits.Max > 0 && position >= config.Limits.Max {
		return
	}

	s.processForwardWithGapSkipping(ctx, trans, position)
}

// processForwardWithGapSkipping performs forward fill processing with the ability to skip
// over gaps in transformation dependencies. When a gap is detected, it jumps to the next
// position where all dependencies have data available, allowing forward fill to continue
// rather than stopping at the first gap.
func (s *service) processForwardWithGapSkipping(ctx context.Context, trans models.Transformation, startPos uint64) {
	config := trans.GetConfig()
	modelID := trans.GetID()
	maxInterval := config.GetMaxInterval()

	currentPos := startPos

	// Process forward until we reach configured limits or run out of dependency data
	for config.Limits == nil || config.Limits.Max == 0 || currentPos < config.Limits.Max {
		// Calculate processing interval, adjusting if it would exceed configured limits
		interval := maxInterval
		if config.Limits != nil && config.Limits.Max > 0 {
			if currentPos+interval > config.Limits.Max {
				interval = config.Limits.Max - currentPos
			}
		}

		// Check if dependencies have data for this range
		result, err := s.validator.ValidateDependencies(ctx, modelID, currentPos, interval)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Critical validation error")
			return
		}

		switch {
		case result.CanProcess:
			// All dependencies have data for this range - enqueue for processing
			s.checkAndEnqueuePositionWithTrigger(ctx, trans, currentPos, interval, "forward_fill")
			currentPos += interval

		case result.NextValidPos > currentPos:
			// Gap detected in dependencies - skip to the next position with data
			s.log.WithFields(logrus.Fields{
				"model_id":  modelID,
				"gap_start": currentPos,
				"gap_end":   result.NextValidPos,
			}).Info("Skipping gap in transformation dependencies")
			currentPos = result.NextValidPos

		default:
			// No more valid positions found (reached end of dependency data)
			s.log.WithFields(logrus.Fields{
				"model_id":      modelID,
				"last_position": currentPos,
			}).Debug("No more valid positions found - reached end of dependency data")
			return
		}
	}
}

// adjustIntervalForDependencies adjusts the interval based on dependency availability
// Returns the adjusted interval and whether processing should stop
func (s *service) adjustIntervalForDependencies(ctx context.Context, trans models.Transformation, nextPos, interval uint64) (uint64, bool) {
	// Get the valid range based on dependencies
	_, maxValid, err := s.validator.GetValidRange(ctx, trans.GetID())
	if err != nil || maxValid == 0 || nextPos >= maxValid {
		// Can't determine valid range or position already beyond range
		return interval, false
	}

	// Check if dependencies limit our interval
	availableInterval := maxValid - nextPos
	if availableInterval >= interval {
		// Full interval is available
		return interval, false
	}

	// Dependencies don't have enough data for full interval
	minInterval := trans.GetConfig().GetMinInterval()

	// Check if the available interval meets minimum requirements
	if availableInterval < minInterval {
		// Available interval is too small
		s.log.WithFields(logrus.Fields{
			"model_id":           trans.GetID(),
			"position":           nextPos,
			"available_interval": availableInterval,
			"min_interval":       minInterval,
		}).Debug("Available interval below minimum interval, waiting for more data")
		return interval, true // Signal to return/stop processing
	}

	// Use partial interval
	originalInterval := interval
	s.log.WithFields(logrus.Fields{
		"model_id":          trans.GetID(),
		"position":          nextPos,
		"original_interval": originalInterval,
		"adjusted_interval": availableInterval,
		"dependency_limit":  maxValid,
		"reason":            "dependency_availability",
	}).Info("Using partial interval due to dependency limits")

	return availableInterval, false
}

func (s *service) processBack(trans models.Transformation) {
	if !trans.GetConfig().IsBackfillEnabled() {
		return
	}

	ctx := context.Background()
	s.checkBackfillOpportunities(ctx, trans)
}

func (s *service) checkAndEnqueuePositionWithTrigger(ctx context.Context, trans models.Transformation, position, interval uint64, direction string) {
	// Create task payload
	payload := tasks.TaskPayload{
		ModelID:    trans.GetID(),
		Position:   position,
		Interval:   interval,
		Direction:  direction,
		EnqueuedAt: time.Now(),
	}

	// Skip queue checks if queueManager is nil
	if s.queueManager == nil {
		s.log.Debug("Queue manager not available, skipping task enqueue")

		return
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

	validationResult, err := s.validator.ValidateDependencies(ctx, trans.GetID(), position, interval)
	depDuration := time.Since(depStartTime).Seconds()

	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"position": position,
		}).Error("Failed to validate dependencies")
		observability.RecordDependencyValidation(trans.GetID(), "error", depDuration)
		observability.RecordError("coordinator", "dependency_validation_error")
		return
	}

	if !validationResult.CanProcess {
		s.log.WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"position": position,
		}).Debug("Dependencies not satisfied")
		observability.RecordDependencyValidation(trans.GetID(), "not_satisfied", depDuration)

		return
	}

	observability.RecordDependencyValidation(trans.GetID(), "satisfied", depDuration)

	// Enqueue task
	if err := s.queueManager.EnqueueTransformation(payload); err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Error("Failed to enqueue task")

		observability.RecordError("coordinator", "enqueue_error")

		return
	}

	// Record successful enqueue
	observability.RecordTaskEnqueued(trans.GetID())

	s.log.WithFields(logrus.Fields{
		"model_id": trans.GetID(),
		"position": position,
		"interval": interval,
	}).Info("Enqueued transformation task")
}

// backfillScanRange holds the range for backfill gap scanning
type backfillScanRange struct {
	initialPos uint64
	maxPos     uint64
}

// getBackfillBounds retrieves the last processed positions for backfill scanning
func (s *service) getBackfillBounds(ctx context.Context, modelID string) (lastPos, lastEndPos uint64, hasData bool) {
	lastEndPos, err := s.admin.GetLastProcessedEndPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to get last processed end position for gap scan")
		return 0, 0, false
	}

	// Also get the actual last position for clearer logging
	lastPos, _ = s.admin.GetLastProcessedPosition(ctx, modelID)

	return lastPos, lastEndPos, true
}

// calculateBackfillScanRange determines the range to scan for gaps
func (s *service) calculateBackfillScanRange(ctx context.Context, trans models.Transformation, lastEndPos uint64) (*backfillScanRange, error) {
	config := trans.GetConfig()

	// Get initial position based on dependencies
	initialPos, err := s.validator.GetEarliestPosition(ctx, trans.GetID())
	if err != nil {
		return nil, err
	}

	s.log.WithFields(logrus.Fields{
		"model_id":               trans.GetID(),
		"calculated_initial_pos": initialPos,
		"based_on":               "dependency analysis",
	}).Debug("Calculated earliest position from dependencies")

	// Apply minimum limit if configured
	initialPos = s.applyMinimumLimit(trans.GetID(), config, initialPos)

	// Apply maximum limit if configured
	maxPos := s.applyMaximumLimit(trans.GetID(), config, lastEndPos)

	return &backfillScanRange{
		initialPos: initialPos,
		maxPos:     maxPos,
	}, nil
}

// applyMinimumLimit applies the configured minimum limit to the initial position
func (s *service) applyMinimumLimit(modelID string, config *transformation.Config, initialPos uint64) uint64 {
	if config.Limits != nil && config.Limits.Min > initialPos {
		s.log.WithFields(logrus.Fields{
			"model_id":           modelID,
			"initial_pos_before": initialPos,
			"initial_pos_after":  config.Limits.Min,
			"limits_min":         config.Limits.Min,
			"reason":             "using configured minimum",
		}).Debug("Adjusted initial position based on configured limits")
		return config.Limits.Min
	}

	// Build log fields based on whether limits are configured
	logFields := logrus.Fields{
		"model_id":    modelID,
		"initial_pos": initialPos,
	}

	if config.Limits != nil {
		logFields["limits_min"] = config.Limits.Min
		logFields["reason"] = "calculated position is higher than limit"
	} else {
		logFields["limits_min"] = "not configured"
		logFields["reason"] = "no limits configured"
	}

	s.log.WithFields(logFields).Debug("Using calculated initial position for gap scanning")
	return initialPos
}

// applyMaximumLimit applies the configured maximum limit to the scan range
func (s *service) applyMaximumLimit(modelID string, config *transformation.Config, lastEndPos uint64) uint64 {
	if config.Limits != nil && config.Limits.Max > 0 && config.Limits.Max < lastEndPos {
		s.log.WithFields(logrus.Fields{
			"model_id":               modelID,
			"last_processed_end_pos": lastEndPos,
			"limits_max":             config.Limits.Max,
		}).Debug("Applying maximum position limit for gap scanning")
		return config.Limits.Max
	}
	return lastEndPos
}

// processSingleGap processes a single gap for backfill
func (s *service) processSingleGap(ctx context.Context, trans models.Transformation, gap admin.GapInfo, gapIndex int) bool {
	config := trans.GetConfig()
	gapSize := gap.EndPos - gap.StartPos
	intervalToUse := config.GetMaxInterval()

	// Adjust interval for small gaps
	if gapSize < config.GetMaxInterval() {
		intervalToUse = gapSize
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"gap_index":         gapIndex,
			"gap_size":          gapSize,
			"model_interval":    config.GetMaxInterval(),
			"adjusted_interval": intervalToUse,
		}).Debug("Adjusted interval for small gap")
	}

	// Calculate position (work backwards from gap end)
	pos := gap.EndPos - intervalToUse

	s.log.WithFields(logrus.Fields{
		"model_id":                 trans.GetID(),
		"gap_index":                gapIndex,
		"gap_start":                gap.StartPos,
		"gap_end":                  gap.EndPos,
		"gap_size":                 gapSize,
		"backfill_position":        pos,
		"backfill_interval":        intervalToUse,
		"will_process_range_start": pos,
		"will_process_range_end":   pos + intervalToUse,
	}).Debug("Processing gap for backfill")

	// Check if task is already pending
	payload := tasks.TaskPayload{
		ModelID:  trans.GetID(),
		Position: pos,
		Interval: intervalToUse,
	}

	isPending, err := s.queueManager.IsTaskPendingOrRunning(payload)
	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"position": pos,
			"interval": intervalToUse,
		}).Error("Failed to check if task is pending")
		return false
	}

	if isPending {
		s.log.WithFields(logrus.Fields{
			"model_id":  trans.GetID(),
			"gap_index": gapIndex,
			"position":  pos,
			"interval":  intervalToUse,
		}).Debug("Task already pending for gap, skipping")
		return false
	}

	s.log.WithFields(logrus.Fields{
		"model_id":       trans.GetID(),
		"gap_index":      gapIndex,
		"gap_start":      gap.StartPos,
		"gap_end":        gap.EndPos,
		"position":       pos,
		"interval":       intervalToUse,
		"model_interval": config.GetMaxInterval(),
		"gap_size":       gapSize,
	}).Info("Enqueueing backfill task for gap (processing from end backward)")

	s.checkAndEnqueuePositionWithTrigger(ctx, trans, pos, intervalToUse, string(DirectionBack))
	return true
}

func (s *service) checkBackfillOpportunities(ctx context.Context, trans models.Transformation) {
	config := trans.GetConfig()

	// Get last processed positions
	lastPos, lastEndPos, hasData := s.getBackfillBounds(ctx, trans.GetID())
	if !hasData {
		return
	}

	// Log the last processed range
	if lastEndPos > 0 {
		s.log.WithFields(logrus.Fields{
			"model_id":             trans.GetID(),
			"last_processed_start": lastPos,
			"last_processed_end":   lastEndPos,
			"backfill_interval":    config.GetMaxInterval(),
		}).Debug("Starting gap scan - found existing processed data")
	} else {
		s.log.WithFields(logrus.Fields{
			"model_id":          trans.GetID(),
			"backfill_interval": config.GetMaxInterval(),
		}).Debug("Starting gap scan - no existing data")
	}

	// Check if we have enough data to scan
	if lastEndPos < config.GetMaxInterval() {
		s.log.WithField("model_id", trans.GetID()).Debug("No data yet, skipping gap scan")
		return
	}

	// Calculate scan range
	scanRange, err := s.calculateBackfillScanRange(ctx, trans, lastEndPos)
	if err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Debug("Failed to calculate scan range")
		return
	}

	// Find all gaps in the processed data
	s.log.WithFields(logrus.Fields{
		"model_id":          trans.GetID(),
		"scan_min_pos":      scanRange.initialPos,
		"scan_max_pos":      scanRange.maxPos,
		"backfill_interval": config.GetMaxInterval(),
	}).Debug("Scanning for gaps in processed data")

	gaps, err := s.admin.FindGaps(ctx, trans.GetID(), scanRange.initialPos, scanRange.maxPos, config.GetMaxInterval())
	if err != nil {
		s.log.WithError(err).WithField("model_id", trans.GetID()).Error("Failed to find gaps")
		return
	}

	// Check if we found any gaps
	if len(gaps) == 0 {
		s.log.WithFields(logrus.Fields{
			"model_id": trans.GetID(),
			"min_pos":  scanRange.initialPos,
			"max_pos":  scanRange.maxPos,
		}).Debug("No gaps found in processed data")
		return
	}

	// Log gap summary
	s.log.WithFields(logrus.Fields{
		"model_id":  trans.GetID(),
		"gap_count": len(gaps),
		"min_pos":   scanRange.initialPos,
		"max_pos":   scanRange.maxPos,
		"interval":  config.GetMaxInterval(),
	}).Info("Found gaps in processed data")

	// Process gaps - queue only one task per scan
	for i, gap := range gaps {
		if s.processSingleGap(ctx, trans, gap, i) {
			// Only queue one task per gap scan
			s.log.WithFields(logrus.Fields{
				"model_id":       trans.GetID(),
				"remaining_gaps": len(gaps) - i - 1,
			}).Debug("Enqueued one backfill task, will re-scan for more gaps after completion")
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

	// Get models that depend on this one
	dependents := s.dag.GetDependents(payload.ModelID)

	for _, depModelID := range dependents {
		model, err := s.dag.GetTransformationNode(depModelID)
		if err != nil {
			continue
		}

		config := model.GetConfig()

		// Calculate next position for dependent
		lastPos, err := s.admin.GetLastProcessedEndPosition(ctx, depModelID)
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
		s.checkAndEnqueuePositionWithTrigger(ctx, model, nextPos, config.GetMaxInterval(), string(DirectionForward))
	}
}

// RunConsolidation performs admin table consolidation for all models
func (s *service) RunConsolidation(ctx context.Context) {
	transformations := s.dag.GetTransformationNodes()
	for _, transformation := range transformations {
		modelID := transformation.GetID()

		// Try to consolidate
		consolidated, err := s.admin.ConsolidateHistoricalData(ctx, modelID)
		if err != nil {
			if consolidated > 0 {
				s.log.WithError(err).WithField("model_id", modelID).Debug("Consolidation partially succeeded")
			}
			continue
		}

		if consolidated > 0 {
			s.log.WithFields(logrus.Fields{
				"model_id":          modelID,
				"rows_consolidated": consolidated,
			}).Info("Consolidated admin.cbt rows")
		}
	}
}

// ProcessExternalScan handles processing external model scans
// This is called by scheduled tasks for each external model
func (s *service) ProcessExternalScan(modelID, scanType string) {
	// Create unique task ID
	taskID := fmt.Sprintf("external:%s:%s", modelID, scanType)

	// Create task payload
	payload := map[string]string{
		"model_id":  modelID,
		"scan_type": scanType,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"model_id":  modelID,
			"scan_type": scanType,
		}).Error("Failed to marshal external scan task payload")
		return
	}

	// Use the appropriate task type based on scan type
	var taskType string
	if scanType == "incremental" {
		taskType = ExternalIncrementalTaskType
	} else {
		taskType = ExternalFullTaskType
	}

	// Create task with unique ID to prevent duplicates
	task := asynq.NewTask(taskType, payloadBytes,
		asynq.TaskID(taskID),
		asynq.Queue(modelID),
		asynq.MaxRetry(0),
		asynq.Timeout(30*time.Minute),
	)

	// Enqueue the task
	if _, err := s.queueManager.Enqueue(task); err != nil {
		// Check if task already exists (not an error, just skip)
		if err.Error() == "task ID already exists" {
			s.log.WithFields(logrus.Fields{
				"model_id":  modelID,
				"scan_type": scanType,
			}).Debug("External scan task already exists, skipping")
		} else {
			s.log.WithError(err).WithFields(logrus.Fields{
				"model_id":  modelID,
				"scan_type": scanType,
			}).Error("Failed to enqueue external scan task")
		}
		return
	}

	s.log.WithFields(logrus.Fields{
		"model_id":  modelID,
		"scan_type": scanType,
		"task_id":   taskID,
	}).Debug("Enqueued external scan task")
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
