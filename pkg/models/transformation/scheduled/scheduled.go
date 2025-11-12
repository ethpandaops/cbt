package scheduled

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
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

// GetDependencies returns the dependencies (after placeholder substitution)
func (h *Handler) GetDependencies() []transformation.Dependency {
	return h.config.Dependencies
}

// GetOriginalDependencies returns the original dependencies before placeholder substitution
func (h *Handler) GetOriginalDependencies() []transformation.Dependency {
	return h.config.OriginalDependencies
}

// GetFlattenedDependencies returns all dependencies as a flat string array
func (h *Handler) GetFlattenedDependencies() []string {
	result := []string{}
	for _, dep := range h.config.Dependencies {
		result = append(result, dep.GetAllDependencies()...)
	}
	return result
}

// SubstituteDependencyPlaceholders replaces {{external}} and {{transformation}} placeholders
func (h *Handler) SubstituteDependencyPlaceholders(externalDefaultDB, transformationDefaultDB string) {
	// Deep copy original dependencies before substitution
	h.config.OriginalDependencies = make([]transformation.Dependency, len(h.config.Dependencies))
	for i := range h.config.Dependencies {
		origDep := transformation.Dependency{
			IsGroup:   h.config.Dependencies[i].IsGroup,
			SingleDep: h.config.Dependencies[i].SingleDep,
		}
		if h.config.Dependencies[i].IsGroup {
			origDep.GroupDeps = make([]string, len(h.config.Dependencies[i].GroupDeps))
			copy(origDep.GroupDeps, h.config.Dependencies[i].GroupDeps)
		}
		h.config.OriginalDependencies[i] = origDep
	}

	for i := range h.config.Dependencies {
		h.config.Dependencies[i] = h.substituteDependency(h.config.Dependencies[i], externalDefaultDB, transformationDefaultDB)
	}
}

func (h *Handler) substituteDependency(dep transformation.Dependency, externalDB, transformationDB string) transformation.Dependency {
	if dep.IsGroup {
		for j := range dep.GroupDeps {
			dep.GroupDeps[j] = h.substitutePlaceholders(dep.GroupDeps[j], externalDB, transformationDB)
		}
	} else {
		dep.SingleDep = h.substitutePlaceholders(dep.SingleDep, externalDB, transformationDB)
	}
	return dep
}

func (h *Handler) substitutePlaceholders(s, externalDB, transformationDB string) string {
	if externalDB != "" {
		s = strings.ReplaceAll(s, "{{external}}", externalDB)
	}
	if transformationDB != "" {
		s = strings.ReplaceAll(s, "{{transformation}}", transformationDB)
	}
	return s
}

// ApplyOverrides applies configuration overrides to this scheduled transformation handler
// Uses reflection to avoid circular dependency with models package
func (h *Handler) ApplyOverrides(override interface{}) {
	v := reflect.ValueOf(override)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}

	h.applyScheduleOverride(v)
	h.applyTagsOverride(v)
}

func (h *Handler) applyScheduleOverride(v reflect.Value) {
	scheduleField := v.FieldByName("Schedule")
	if !scheduleField.IsValid() || scheduleField.IsNil() {
		return
	}

	newSchedule := scheduleField.Elem().String()
	h.config.Schedule = newSchedule
}

func (h *Handler) applyTagsOverride(v reflect.Value) {
	tagsField := v.FieldByName("Tags")
	if !tagsField.IsValid() || tagsField.Len() == 0 {
		return
	}

	// Append override tags to existing tags
	for i := 0; i < tagsField.Len(); i++ {
		tag := tagsField.Index(i).String()
		h.config.Tags = append(h.config.Tags, tag)
	}
}
