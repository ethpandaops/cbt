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

// TaskPayload represents the payload for a transformation task
type TaskPayload struct {
	ModelID    string    `json:"model_id"`
	Position   uint64    `json:"position"`
	Interval   uint64    `json:"interval"`
	Direction  string    `json:"direction"` // DirectionForward or DirectionBack
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// UniqueID returns a unique identifier for this task
func (p TaskPayload) UniqueID() string {
	return fmt.Sprintf("%s:%s", p.ModelID, p.Direction)
}

// QueueName returns the queue name for this task payload
func (p TaskPayload) QueueName() string {
	return p.ModelID
}
