package tasks

import (
	"fmt"
	"time"
)

const (
	// DirectionForward represents forward fill processing
	DirectionForward = "forward"
	// DirectionBack represents backfill processing
	DirectionBack = "back"
)

// TaskType indicates whether this is a scheduled or incremental task
type TaskType string

const (
	// TaskTypeIncremental represents incremental position-based tasks
	TaskTypeIncremental TaskType = "incremental"
	// TaskTypeScheduled represents scheduled cron-based tasks
	TaskTypeScheduled TaskType = "scheduled"
	// TaskTypeExternal represents external model scan tasks
	TaskTypeExternal TaskType = "external"
)

// TaskPayload is the common interface for all task payloads
type TaskPayload interface {
	GetModelID() string
	GetEnqueuedAt() time.Time
	GetType() TaskType
	UniqueID() string
	QueueName() string
}

// IncrementalTaskPayload represents a position-based incremental task
type IncrementalTaskPayload struct {
	Type       TaskType  `json:"type"`
	ModelID    string    `json:"model_id"`
	Position   uint64    `json:"position"`
	Interval   uint64    `json:"interval"`
	Direction  string    `json:"direction"` // DirectionForward or DirectionBack
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// GetModelID returns the model ID
func (p IncrementalTaskPayload) GetModelID() string { return p.ModelID }

// GetEnqueuedAt returns the enqueued time
func (p IncrementalTaskPayload) GetEnqueuedAt() time.Time { return p.EnqueuedAt }

// GetType returns the task type
func (p IncrementalTaskPayload) GetType() TaskType { return TaskTypeIncremental }

// QueueName returns the queue name for this task
func (p IncrementalTaskPayload) QueueName() string { return p.ModelID }

// UniqueID returns a unique identifier for this task
// Uses model.id:direction to ensure only one task per direction can run at a time.
// This prevents duplicate work when intervals expand (e.g., model:100:25 -> model:100:50)
// and leverages the natural separation between forward fill (frontier) and backfill (historical gaps).
func (p IncrementalTaskPayload) UniqueID() string {
	return fmt.Sprintf("%s:%s", p.ModelID, p.Direction)
}

// ScheduledTaskPayload represents a scheduled cron-based task
type ScheduledTaskPayload struct {
	Type          TaskType  `json:"type"`
	ModelID       string    `json:"model_id"`
	ExecutionTime time.Time `json:"execution_time"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
}

// GetModelID returns the model ID
func (p ScheduledTaskPayload) GetModelID() string { return p.ModelID }

// GetEnqueuedAt returns the enqueued time
func (p ScheduledTaskPayload) GetEnqueuedAt() time.Time { return p.EnqueuedAt }

// GetType returns the task type
func (p ScheduledTaskPayload) GetType() TaskType { return TaskTypeScheduled }

// QueueName returns the queue name for this task
func (p ScheduledTaskPayload) QueueName() string { return p.ModelID }

// UniqueID returns a unique identifier for this task
func (p ScheduledTaskPayload) UniqueID() string { return p.ModelID }
