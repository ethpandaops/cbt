package tasks

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/hibiken/asynq"
)

// QueueManager manages task queuing
type QueueManager struct {
	client      *asynq.Client
	inspector   *asynq.Inspector
	taskTimeout time.Duration
}

// NewQueueManager creates a new queue manager
func NewQueueManager(redisOpt *asynq.RedisClientOpt, taskTimeout time.Duration) *QueueManager {
	return &QueueManager{
		client:      asynq.NewClient(*redisOpt),
		inspector:   asynq.NewInspector(*redisOpt),
		taskTimeout: taskTimeout,
	}
}

const (
	// TypeModelTransformation is the task type for model transformations
	TypeModelTransformation = "model:transformation"
)

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
		asynq.Queue(payload.GetModelID()), // Model-specific queue
		asynq.MaxRetry(3),
		asynq.Timeout(q.taskTimeout),
	}

	allOpts := defaultOpts
	allOpts = append(allOpts, opts...)

	_, err = q.client.Enqueue(task, allOpts...)
	return err
}

// IsTaskPendingOrRunning checks if a task is pending or running
func (q *QueueManager) IsTaskPendingOrRunning(task TaskPayload) (bool, error) {
	info, err := q.inspector.GetTaskInfo(task.QueueName(), task.UniqueID())
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

// Enqueue enqueues a generic task
func (q *QueueManager) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	return q.client.Enqueue(task, opts...)
}

// Close closes the queue manager
func (q *QueueManager) Close() error {
	return q.client.Close()
}
