package tasks

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/hibiken/asynq"
)

const (
	// DefaultQueueName is the default queue name when none is specified
	DefaultQueueName = "default"
)

// QueueManager manages task queuing
type QueueManager struct {
	client    *asynq.Client
	inspector *asynq.Inspector
}

// NewQueueManager creates a new queue manager
func NewQueueManager(redisOpt *asynq.RedisClientOpt) *QueueManager {
	return &QueueManager{
		client:    asynq.NewClient(*redisOpt),
		inspector: asynq.NewInspector(*redisOpt),
	}
}

// EnqueueTransformation enqueues a transformation task
func (q *QueueManager) EnqueueTransformation(payload TaskPayload, opts ...asynq.Option) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	task := asynq.NewTask(TypeModelTransformation, data)

	// Default options
	defaultOpts := []asynq.Option{
		asynq.TaskID(payload.UniqueID()),
		asynq.Queue(payload.ModelID), // Model-specific queue
		asynq.MaxRetry(3),
		asynq.Timeout(30 * time.Minute),
	}

	allOpts := defaultOpts
	allOpts = append(allOpts, opts...)

	_, err = q.client.Enqueue(task, allOpts...)
	return err
}

// IsTaskPendingOrRunning checks if a task is pending or running
func (q *QueueManager) IsTaskPendingOrRunning(taskID string) (bool, error) {
	// Extract queue name from taskID (format: modelID:position:interval)
	parts := strings.Split(taskID, ":")
	queueName := DefaultQueueName
	if len(parts) > 0 {
		queueName = parts[0] // Use modelID as queue name
	}

	info, err := q.inspector.GetTaskInfo(queueName, taskID)
	if err != nil {
		if strings.Contains(err.Error(), "NOT FOUND") || strings.Contains(err.Error(), "queue not found") || strings.Contains(err.Error(), "task not found") {
			return false, nil
		}
		return false, err
	}

	return info.State == asynq.TaskStatePending ||
		info.State == asynq.TaskStateActive ||
		info.State == asynq.TaskStateRetry, nil
}

// WasRecentlyCompleted checks if a task was recently completed
func (q *QueueManager) WasRecentlyCompleted(taskID string, within time.Duration) (bool, error) {
	// Extract queue name from taskID (format: modelID:position:interval)
	parts := strings.Split(taskID, ":")
	queueName := DefaultQueueName
	if len(parts) > 0 {
		queueName = parts[0] // Use modelID as queue name
	}

	info, err := q.inspector.GetTaskInfo(queueName, taskID)
	if err != nil {
		if strings.Contains(err.Error(), "NOT FOUND") || strings.Contains(err.Error(), "queue not found") || strings.Contains(err.Error(), "task not found") {
			return false, nil
		}
		return false, err
	}

	if info.State != asynq.TaskStateCompleted {
		return false, nil
	}

	return time.Since(info.CompletedAt) <= within, nil
}

// GetQueueStats returns queue statistics
func (q *QueueManager) GetQueueStats(queueName string) (*asynq.QueueInfo, error) {
	return q.inspector.GetQueueInfo(queueName)
}

// Close closes the queue manager
func (q *QueueManager) Close() error {
	return q.client.Close()
}
