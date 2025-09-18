package transformation

import (
	"context"
	"time"
)

// AdminTable holds the configuration for admin tables
type AdminTable struct {
	Database string
	Table    string
}

// Handler defines the interface for transformation type handlers
type Handler interface {
	// Type returns the transformation type
	Type() Type

	// Config returns the typed configuration
	Config() any

	// Validate validates the configuration
	Validate() error

	// ShouldTrackPosition indicates if this type tracks positions
	ShouldTrackPosition() bool

	// GetTemplateVariables returns template variables for this transformation
	GetTemplateVariables(ctx context.Context, taskInfo TaskInfo) map[string]any

	// GetAdminTable returns the admin table configuration for this type
	GetAdminTable() AdminTable

	// RecordCompletion records the completion of a transformation
	RecordCompletion(ctx context.Context, adminService any, modelID string, taskInfo TaskInfo) error
}

// TaskInfo contains information about the current task execution
type TaskInfo struct {
	Position  uint64
	Interval  uint64
	Timestamp time.Time
	Direction string
}
