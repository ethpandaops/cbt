package scheduled

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"gopkg.in/yaml.v3"
)

// Handler handles scheduled transformation type operations
type Handler struct {
	config     *Config
	adminTable transformation.AdminTable
}

// NewHandler creates a new handler for scheduled transformations
func NewHandler(data []byte, adminTable transformation.AdminTable) (*Handler, error) {
	var config Config

	// Use strict unmarshaling to detect invalid fields like dependencies
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse scheduled config: %w", err)
	}

	return &Handler{
		config:     &config,
		adminTable: adminTable,
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

	if err := ValidateScheduleFormat(h.config.Schedule); err != nil {
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

// GetAdminTable returns the admin table configuration
func (h *Handler) GetAdminTable() transformation.AdminTable {
	return h.adminTable
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

// GetID returns the unique identifier for the transformation model
func (h *Handler) GetID() string {
	return fmt.Sprintf("%s.%s", h.config.Database, h.config.Table)
}

// GetSchedule returns the cron schedule expression
func (h *Handler) GetSchedule() string {
	return h.config.Schedule
}

// GetTags returns the tags for this transformation
func (h *Handler) GetTags() []string {
	return h.config.Tags
}
