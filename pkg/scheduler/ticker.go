package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/cbt/pkg/observability"
)

// tickerService manages periodic checking of scheduled tasks
type tickerService interface {
	// Start begins the ticker loop (should only run on leader)
	// Blocks until context is canceled
	Start(ctx context.Context) error

	// Stop gracefully shuts down the ticker
	Stop() error
}

type tickerServiceImpl struct {
	log         logrus.FieldLogger
	tracker     scheduleTracker
	queueClient *asynq.Client
	tasks       []scheduledTask // Populated from DAG config
	tasksMu     sync.RWMutex    // Protects tasks slice (specifically nextRun field)
	ticker      *time.Ticker
	done        chan struct{}
}

// scheduledTask represents a task that should run on a schedule
type scheduledTask struct {
	ID       string        // Unique identifier (e.g., "transformation:model:forward")
	Schedule string        // Cron expression (e.g., "@every 30s")
	Interval time.Duration // Parsed interval from schedule
	Task     *asynq.Task   // Asynq task to enqueue
	Queue    string        // Asynq queue name
	nextRun  *time.Time    // Cached next run time to avoid Redis lookups
}

// newTickerService creates a new ticker service
// tasks parameter should be built from DAG config by the scheduler service
func newTickerService(
	log logrus.FieldLogger,
	tracker scheduleTracker,
	queueClient *asynq.Client,
	tasks []scheduledTask,
) tickerService {
	// Record metrics for registered tasks
	for _, task := range tasks {
		modelID, operation := parseTaskIDForMetrics(task.ID)
		observability.RecordScheduledTaskRegistered(modelID, operation)
	}

	return &tickerServiceImpl{
		log:         log.WithField("component", "ticker"),
		tracker:     tracker,
		queueClient: queueClient,
		tasks:       tasks,
		done:        make(chan struct{}),
	}
}

func (t *tickerServiceImpl) Start(ctx context.Context) error {
	t.log.Info("Starting ticker service")
	t.ticker = time.NewTicker(1 * time.Second)
	defer t.ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.log.Info("Ticker context canceled, stopping")
			return ctx.Err()
		case <-t.done:
			t.log.Info("Ticker stopped via Stop()")
			return nil
		case <-t.ticker.C:
			t.checkSchedules(ctx)
		}
	}
}

func (t *tickerServiceImpl) checkSchedules(ctx context.Context) {
	now := time.Now().UTC()

	for i := range t.tasks {
		task := &t.tasks[i]

		// Fast path: skip if we already know the task isn't due yet.
		// This avoids a Redis call for tasks that are clearly not ready.
		t.tasksMu.RLock()
		cachedNextRun := task.nextRun
		t.tasksMu.RUnlock()

		if cachedNextRun != nil && now.Before(*cachedNextRun) {
			continue
		}

		// Get last run time from Redis (only when task might be due)
		lastRun, err := t.tracker.GetLastRun(ctx, task.ID)
		if err != nil {
			t.log.WithError(err).WithField("task_id", task.ID).Warn("Failed to get last run, will retry next tick")

			continue
		}

		// Calculate next run time and cache it
		nextRun := lastRun.Add(task.Interval)

		t.tasksMu.Lock()
		task.nextRun = &nextRun
		t.tasksMu.Unlock()

		// Check if interval has elapsed
		if now.Before(nextRun) {
			// Not due yet
			continue
		}

		// Task is due, enqueue it
		if err := t.enqueueTask(ctx, *task, now); err != nil {
			t.log.WithError(err).
				WithField("task_id", task.ID).
				Error("Failed to enqueue task")

			continue
		}

		// Update last run timestamp
		if err := t.tracker.SetLastRun(ctx, task.ID, now); err != nil {
			t.log.WithError(err).
				WithField("task_id", task.ID).
				Error("Failed to update last run timestamp")
		}

		// Update cached next run time based on successful enqueue
		updatedNextRun := now.Add(task.Interval)

		t.tasksMu.Lock()
		task.nextRun = &updatedNextRun
		t.tasksMu.Unlock()
	}
}

func (t *tickerServiceImpl) enqueueTask(ctx context.Context, task scheduledTask, enqueuedAt time.Time) error {
	opts := []asynq.Option{
		asynq.TaskID(task.ID),
		asynq.Queue(task.Queue),
		asynq.MaxRetry(0),
		asynq.Timeout(5 * time.Minute),
	}

	info, err := t.queueClient.EnqueueContext(ctx, task.Task, opts...)
	if err != nil {
		// Task already exists in queue - this is expected when processing is slow
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			t.log.WithField("task_id", task.ID).Debug("Task already queued, skipping")

			return nil
		}

		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	t.log.WithFields(logrus.Fields{
		"task_id":     task.ID,
		"queue":       task.Queue,
		"asynq_id":    info.ID,
		"enqueued_at": enqueuedAt,
	}).Info("Enqueued scheduled task")

	observability.RecordTaskEnqueued(task.ID)

	return nil
}

func (t *tickerServiceImpl) Stop() error {
	t.log.Info("Stopping ticker service")

	// Unregister metrics for scheduled tasks
	t.tasksMu.RLock()
	for _, task := range t.tasks {
		modelID, operation := parseTaskIDForMetrics(task.ID)
		observability.RecordScheduledTaskUnregistered(modelID, operation)
	}
	t.tasksMu.RUnlock()

	close(t.done)
	return nil
}

// parseScheduleInterval converts a cron schedule string to a duration
// Supports @every format (e.g., "@every 30s", "@every 5m")
// Returns error for unsupported formats
func parseScheduleInterval(schedule string) (time.Duration, error) {
	// Validate it's a valid cron expression
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(schedule)
	if err != nil {
		return 0, fmt.Errorf("invalid schedule format: %w", err)
	}

	// For @every format, extract the duration
	// Schedule string format: "@every 30s", "@every 5m", etc.
	if len(schedule) > 7 && schedule[:6] == "@every" {
		durationStr := schedule[7:] // Extract "30s", "5m", etc.
		duration, err := time.ParseDuration(durationStr)
		if err != nil {
			return 0, fmt.Errorf("failed to parse @every duration: %w", err)
		}
		return duration, nil
	}

	// For standard cron expressions, calculate next two runs and get interval
	now := time.Now()
	next1 := sched.Next(now)
	next2 := sched.Next(next1)
	interval := next2.Sub(next1)

	return interval, nil
}

// parseTaskIDForMetrics extracts model ID and operation from a task ID
// Task IDs have formats like:
//   - "transformation:db.table:forward" -> ("db.table", "forward")
//   - "transformation:db.table:back" -> ("db.table", "back")
//   - "transformation:db.table:scheduled" -> ("db.table", "scheduled")
//   - "external:db.table:incremental" -> ("db.table", "incremental")
//   - "external:db.table:full" -> ("db.table", "full")
//   - "system:consolidation" -> ("system", "consolidation")
func parseTaskIDForMetrics(taskID string) (modelID, operation string) {
	parts := strings.Split(taskID, ":")

	switch len(parts) {
	case 3:
		// Standard format: prefix:model:operation (e.g., "transformation:db.table:forward")
		return parts[1], parts[2]
	case 2:
		// System tasks: prefix:operation (e.g., "system:consolidation")
		return parts[0], parts[1]
	default:
		// Fallback for unknown formats
		return taskID, "unknown"
	}
}

// Verify interface compliance at compile time
var _ tickerService = (*tickerServiceImpl)(nil)
