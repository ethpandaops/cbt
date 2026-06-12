package scheduled

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// Handler handles scheduled transformation type operations
type Handler struct {
	transformation.DependencyBase

	config *Config
}

// NewHandler creates a new handler for scheduled transformations
func NewHandler(data []byte, adminTable transformation.AdminTable) (*Handler, error) {
	config, err := transformation.DecodeStrict[Config](data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse scheduled config: %w", err)
	}

	return &Handler{
		DependencyBase: transformation.NewDependencyBase(
			adminTable,
			func() (database, table string) { return config.Database, config.Table },
			func() []string { return config.Tags },
			func() []transformation.Dependency { return config.Dependencies },
			func() []transformation.Dependency { return config.OriginalDependencies },
			func(deps []transformation.Dependency) { config.OriginalDependencies = deps },
		),
		config: config,
	}, nil
}

// Type returns the transformation type (scheduled)
func (h *Handler) Type() transformation.Type {
	return transformation.TypeScheduled
}

// Config returns the typed configuration
func (h *Handler) Config() any {
	return h.config
}

// Validate validates the configuration for scheduled transformations
func (h *Handler) Validate() error {
	if h.config.Database == "" {
		return transformation.ErrDatabaseRequired
	}

	if h.config.Table == "" {
		return transformation.ErrTableRequired
	}

	if h.config.Schedule == "" {
		return ErrScheduleRequired
	}

	if err := transformation.ValidateScheduleFormat(h.config.Schedule); err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}

	return nil
}

// ShouldTrackPosition returns false for scheduled transformations
func (h *Handler) ShouldTrackPosition() bool {
	return false
}

// GetTemplateVariables returns template variables for scheduled transformations
func (h *Handler) GetTemplateVariables(_ context.Context, taskInfo transformation.TaskInfo) map[string]any {
	return map[string]any{
		"execution": map[string]any{
			"timestamp": taskInfo.Timestamp.Unix(),
			"datetime":  taskInfo.Timestamp.Format(time.RFC3339),
		},
		"task": map[string]any{
			"direction": taskInfo.Direction,
		},
	}
}

// RecordCompletion records the completion of a scheduled transformation
func (h *Handler) RecordCompletion(ctx context.Context, adminService any, modelID string, taskInfo transformation.TaskInfo) error {
	type scheduledRecorder interface {
		RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error
	}

	recorder, ok := adminService.(scheduledRecorder)
	if !ok {
		return ErrAdminServiceInvalid
	}

	return recorder.RecordScheduledCompletion(ctx, modelID, taskInfo.Timestamp)
}

// GetSchedule returns the cron schedule expression
func (h *Handler) GetSchedule() string {
	return h.config.Schedule
}

// ApplyOverrides applies configuration overrides to this scheduled transformation handler.
func (h *Handler) ApplyOverrides(override *transformation.Override) {
	if override == nil {
		return
	}

	h.config.Schedule = transformation.ApplyScheduleOverride(h.config.Schedule, override.Schedule)
	h.config.Tags = transformation.ApplyTagsOverride(h.config.Tags, override.Tags)
}

// Snapshot holds a point-in-time snapshot of overridable config fields.
type Snapshot struct {
	Schedule string
	Tags     []string
}

// ToBaseConfigJSON serializes the snapshot to a JSON representation
// suitable for the management API's base_config response.
func (s *Snapshot) ToBaseConfigJSON() (json.RawMessage, error) {
	return json.Marshal(map[string]any{
		"schedule": s.Schedule,
		"tags":     s.Tags,
	})
}

// SnapshotConfig captures the current overridable config values.
func (h *Handler) SnapshotConfig() any {
	return &Snapshot{
		Schedule: h.config.Schedule,
		Tags:     transformation.CopyTags(h.config.Tags),
	}
}

// RestoreConfig restores overridable config values from a snapshot.
func (h *Handler) RestoreConfig(snapshot any) {
	s, ok := snapshot.(*Snapshot)
	if !ok {
		return
	}

	h.config.Schedule = s.Schedule
	h.config.Tags = transformation.CopyTags(s.Tags)
}
