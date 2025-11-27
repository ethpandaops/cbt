package scheduler

//go:generate mockgen -package mock -destination mock/tracker.mock.go -source tracker.go scheduleTracker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	scheduleKeyPrefix = "cbt:scheduler:task:" // Redis key prefix
	// Full key pattern: cbt:scheduler:task:{taskID}
	// Example: cbt:scheduler:task:external:mainnet.blocks:incremental
	// Example: cbt:scheduler:task:transformation:fct_blocks:forward
)

// scheduleTracker manages execution timestamps for scheduled tasks in Redis
type scheduleTracker interface {
	// GetLastRun retrieves the last execution timestamp for a task
	// Returns zero time if task has never run
	GetLastRun(ctx context.Context, taskID string) (time.Time, error)

	// SetLastRun updates the last execution timestamp for a task
	// Persists to Redis with no TTL (permanent storage)
	SetLastRun(ctx context.Context, taskID string, timestamp time.Time) error

	// DeleteLastRun removes the execution timestamp for a task
	// Used for cleanup when tasks are removed from config
	DeleteLastRun(ctx context.Context, taskID string) error

	// GetAllTaskIDs returns all task IDs currently tracked in Redis
	// Used for debugging and observability
	GetAllTaskIDs(ctx context.Context) ([]string, error)

	// Close releases resources held by the tracker
	Close() error
}

type redisScheduleTracker struct {
	log   logrus.FieldLogger
	redis *redis.Client
}

// newScheduleTracker creates a Redis-backed schedule tracker
func newScheduleTracker(log logrus.FieldLogger, redisClient *redis.Client) scheduleTracker {
	return &redisScheduleTracker{
		log:   log.WithField("component", "schedule_tracker"),
		redis: redisClient,
	}
}

func (r *redisScheduleTracker) GetLastRun(ctx context.Context, taskID string) (time.Time, error) {
	key := scheduleKeyPrefix + taskID
	val, err := r.redis.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Key doesn't exist, return zero time (not an error)
			r.log.WithField("task_id", taskID).Debug("No last run found for task")
			return time.Time{}, nil
		}
		r.log.WithError(err).WithField("task_id", taskID).Error("Failed to get last run from Redis")
		return time.Time{}, fmt.Errorf("failed to get last run for task %s: %w", taskID, err)
	}

	timestamp, err := time.Parse(time.RFC3339, val)
	if err != nil {
		r.log.WithError(err).
			WithFields(logrus.Fields{
				"task_id":   taskID,
				"raw_value": val,
			}).
			Error("Failed to parse timestamp")
		return time.Time{}, fmt.Errorf("failed to parse timestamp for task %s: %w", taskID, err)
	}

	r.log.WithFields(logrus.Fields{
		"task_id":  taskID,
		"last_run": timestamp,
	}).Debug("Retrieved last run for task")

	return timestamp, nil
}

func (r *redisScheduleTracker) SetLastRun(ctx context.Context, taskID string, timestamp time.Time) error {
	key := scheduleKeyPrefix + taskID
	val := timestamp.Format(time.RFC3339)

	err := r.redis.Set(ctx, key, val, 0).Err()
	if err != nil {
		r.log.WithError(err).
			WithFields(logrus.Fields{
				"task_id":   taskID,
				"timestamp": timestamp,
			}).
			Error("Failed to set last run in Redis")
		return fmt.Errorf("failed to set last run for task %s: %w", taskID, err)
	}

	r.log.WithFields(logrus.Fields{
		"task_id":   taskID,
		"timestamp": timestamp,
	}).Debug("Updated last run for task")

	return nil
}

func (r *redisScheduleTracker) DeleteLastRun(ctx context.Context, taskID string) error {
	key := scheduleKeyPrefix + taskID

	err := r.redis.Del(ctx, key).Err()
	if err != nil {
		r.log.WithError(err).
			WithField("task_id", taskID).
			Error("Failed to delete last run from Redis")
		return fmt.Errorf("failed to delete last run for task %s: %w", taskID, err)
	}

	r.log.WithField("task_id", taskID).Debug("Deleted last run for task")

	return nil
}

func (r *redisScheduleTracker) GetAllTaskIDs(ctx context.Context) ([]string, error) {
	pattern := scheduleKeyPrefix + "*"

	// Use SCAN instead of Keys() to avoid blocking Redis.
	// SCAN iterates incrementally and doesn't lock the server.
	// The count hint (100) is keys per iteration, not a total limit -
	// the iterator continues until all matching keys are retrieved.
	const scanBatchSize = 100

	var taskIDs []string

	iter := r.redis.Scan(ctx, 0, pattern, scanBatchSize).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		taskID := key[len(scheduleKeyPrefix):]
		taskIDs = append(taskIDs, taskID)
	}

	if err := iter.Err(); err != nil {
		r.log.WithError(err).Error("Failed to scan task IDs from Redis")

		return nil, fmt.Errorf("failed to scan task IDs: %w", err)
	}

	r.log.WithField("count", len(taskIDs)).Debug("Retrieved all tracked task IDs")

	return taskIDs, nil
}

func (r *redisScheduleTracker) Close() error {
	if r.redis != nil {
		return r.redis.Close()
	}

	return nil
}

// Verify interface compliance at compile time
var _ scheduleTracker = (*redisScheduleTracker)(nil)
