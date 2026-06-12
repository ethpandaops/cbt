package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
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

const (
	// TypeModelTransformation is the task type for model transformations
	TypeModelTransformation = "model:transformation"
)

// EnqueueTransformation enqueues a transformation task
func (q *QueueManager) EnqueueTransformation(payload Payload, opts ...asynq.Option) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal transformation payload for %s: %w", payload.GetModelID(), err)
	}

	task := asynq.NewTask(TypeModelTransformation, data)

	// Default options
	defaultOpts := []asynq.Option{
		asynq.TaskID(payload.UniqueID()),
		asynq.Queue(payload.GetModelID()), // Model-specific queue
		asynq.MaxRetry(3),
		asynq.Timeout(30 * time.Minute),
	}

	allOpts := defaultOpts
	allOpts = append(allOpts, opts...)

	if _, err := q.client.Enqueue(task, allOpts...); err != nil {
		return fmt.Errorf("failed to enqueue transformation task for %s: %w", payload.GetModelID(), err)
	}

	return nil
}

// IsTaskPendingOrRunning checks if a task is pending or running
func (q *QueueManager) IsTaskPendingOrRunning(task Payload) (bool, error) {
	info, err := q.inspector.GetTaskInfo(task.QueueName(), task.UniqueID())
	if err != nil {
		// A missing queue or task means there is nothing pending/running.
		// asynq.Inspector.GetTaskInfo wraps these sentinels with %w.
		if errors.Is(err, asynq.ErrQueueNotFound) || errors.Is(err, asynq.ErrTaskNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("failed to get task info for %s: %w", task.UniqueID(), err)
	}

	return info.State == asynq.TaskStatePending ||
		info.State == asynq.TaskStateActive ||
		info.State == asynq.TaskStateRetry, nil
}

// Enqueue enqueues a generic task
func (q *QueueManager) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	return q.client.Enqueue(task, opts...)
}

// Close closes the queue manager, releasing both the client and inspector.
func (q *QueueManager) Close() error {
	var errs []error

	if err := q.client.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close queue client: %w", err))
	}

	if err := q.inspector.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close queue inspector: %w", err))
	}

	return errors.Join(errs...)
}
