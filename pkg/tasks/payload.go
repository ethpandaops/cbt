package tasks

import (
	"fmt"
	"time"
)

// TaskPayload represents the payload for a transformation task
type TaskPayload struct {
	ModelID    string    `json:"model_id"`
	Position   uint64    `json:"position"`
	Interval   uint64    `json:"interval"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// UniqueID returns a unique identifier for this task
func (p TaskPayload) UniqueID() string {
	return fmt.Sprintf("%s:%d:%d", p.ModelID, p.Position, p.Interval)
}

func (p TaskPayload) QueueName() string {
	return p.ModelID
}
