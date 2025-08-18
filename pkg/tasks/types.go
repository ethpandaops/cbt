// Package tasks provides task queue management using Asynq
package tasks

import (
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
)

const (
	// TypeModelTransformation is the task type for model transformations
	TypeModelTransformation = "model:transformation"
	// TypeModelValidation is the task type for model validation
	TypeModelValidation = "model:validation"
)

// TaskPayload represents the payload for a transformation task
type TaskPayload struct {
	ModelID    string    `json:"model_id"`
	Position   uint64    `json:"position"`
	Interval   uint64    `json:"interval"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	IsBackfill bool      `json:"is_backfill"` // Whether this is a backfill task (lower priority)
}

// UniqueID returns a unique identifier for this task
func (p TaskPayload) UniqueID() string {
	return fmt.Sprintf("%s:%d:%d", p.ModelID, p.Position, p.Interval)
}

// TaskResult contains the result of task execution
type TaskResult struct {
	ModelID     string        `json:"model_id"`
	Position    uint64        `json:"position"`
	Interval    uint64        `json:"interval"`
	Duration    time.Duration `json:"duration"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
	CompletedAt time.Time     `json:"completed_at"`
}

// TaskContext contains all context needed for task execution
type TaskContext struct {
	ModelConfig models.ModelConfig
	Position    uint64
	Interval    uint64
	StartTime   time.Time
	Variables   map[string]interface{}
}
