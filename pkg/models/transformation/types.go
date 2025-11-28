package transformation

//go:generate mockgen -package mock -destination mock/handler.mock.go github.com/ethpandaops/cbt/pkg/models/transformation Handler,IntervalHandler,ScheduleHandler,LimitsHandler,DependencyHandler,FillHandler

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

// Limits defines position limits for transformations.
type Limits struct {
	Min uint64
	Max uint64
}

// IntervalHandler provides interval configuration for transformations.
// Handlers that process data in intervals should implement this interface.
type IntervalHandler interface {
	GetMinInterval() uint64
	GetMaxInterval() uint64
	AllowsPartialIntervals() bool
	AllowGapSkipping() bool
}

// ScheduleHandler provides scheduling and limits configuration for transformations.
// Handlers that have directional processing should implement this interface.
type ScheduleHandler interface {
	IsBackfillEnabled() bool
	IsForwardFillEnabled() bool
	GetLimits() *Limits
}

// LimitsHandler provides position limits for transformations.
// This is a subset of ScheduleHandler for cases where only limits are needed.
type LimitsHandler interface {
	GetLimits() *Limits
}

// DependencyHandler provides dependency information for transformations.
type DependencyHandler interface {
	GetFlattenedDependencies() []string
	GetDependencies() []Dependency
}

// FillHandler provides fill configuration for transformations.
type FillHandler interface {
	GetFillDirection() string
	AllowGapSkipping() bool
	GetFillBuffer() uint64
}
