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

	// ScanTypeIncremental is the incremental scan type for external models
	ScanTypeIncremental = "incremental"
	// ScanTypeFull is the full scan type for external models
	ScanTypeFull = "full"
)

// Type indicates whether this is a scheduled or incremental task
type Type string

const (
	// TypeIncremental represents incremental position-based tasks
	TypeIncremental Type = "incremental"
	// TypeScheduled represents scheduled cron-based tasks
	TypeScheduled Type = "scheduled"
	// TypeExternal represents external model scan tasks
	TypeExternal Type = "external"
)

// Payload is the common interface for all task payloads
type Payload interface {
	GetModelID() string
	GetEnqueuedAt() time.Time
	GetType() Type
	UniqueID() string
	QueueName() string
}

// IncrementalPayload represents a position-based incremental task
type IncrementalPayload struct {
	Type       Type      `json:"type"`
	ModelID    string    `json:"model_id"`
	Position   uint64    `json:"position"`
	Interval   uint64    `json:"interval"`
	Direction  string    `json:"direction"` // DirectionForward or DirectionBack
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// GetModelID returns the model ID
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p IncrementalPayload) GetModelID() string { return p.ModelID }

// GetEnqueuedAt returns the enqueued time
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p IncrementalPayload) GetEnqueuedAt() time.Time { return p.EnqueuedAt }

// GetType returns the task type
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p IncrementalPayload) GetType() Type { return TypeIncremental }

// QueueName returns the queue name for this task
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p IncrementalPayload) QueueName() string { return p.ModelID }

// UniqueID returns a unique identifier for this task
// Uses model.id:direction to ensure only one task per direction can run at a time.
// This prevents duplicate work when intervals expand (e.g., model:100:25 -> model:100:50)
// and leverages the natural separation between forward fill (frontier) and backfill (historical gaps).
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p IncrementalPayload) UniqueID() string {
	return fmt.Sprintf("%s:%s", p.ModelID, p.Direction)
}

// ScheduledPayload represents a scheduled cron-based task
type ScheduledPayload struct {
	Type          Type      `json:"type"`
	ModelID       string    `json:"model_id"`
	ExecutionTime time.Time `json:"execution_time"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
}

// GetModelID returns the model ID
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p ScheduledPayload) GetModelID() string { return p.ModelID }

// GetEnqueuedAt returns the enqueued time
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p ScheduledPayload) GetEnqueuedAt() time.Time { return p.EnqueuedAt }

// GetType returns the task type
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p ScheduledPayload) GetType() Type { return TypeScheduled }

// QueueName returns the queue name for this task
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p ScheduledPayload) QueueName() string { return p.ModelID }

// UniqueID returns a unique identifier for this task
//
//nolint:gocritic // value receiver is intentional for immutable payload semantics.
func (p ScheduledPayload) UniqueID() string { return p.ModelID }
