package incremental

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"gopkg.in/yaml.v3"
)

// Handler handles incremental transformation type operations
type Handler struct {
	config     *Config
	adminTable transformation.AdminTable
}

// NewHandler creates a new handler for incremental transformations
func NewHandler(data []byte, adminTable transformation.AdminTable) (*Handler, error) {
	var config Config

	// Use strict unmarshaling to detect invalid fields
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse incremental config: %w", err)
	}

	return &Handler{
		config:     &config,
		adminTable: adminTable,
	}, nil
}

// Type returns the transformation type (incremental)
func (h *Handler) Type() transformation.Type {
	return transformation.TypeIncremental
}

// Config returns the typed configuration
func (h *Handler) Config() any {
	return h.config
}

// Validate validates the configuration for incremental transformations
func (h *Handler) Validate() error {
	if h.config.Database == "" {
		return transformation.ErrDatabaseRequired
	}

	if h.config.Table == "" {
		return transformation.ErrTableRequired
	}

	if h.config.Interval == nil {
		return ErrIntervalRequired
	}

	if err := h.config.Interval.Validate(); err != nil {
		return fmt.Errorf("interval validation failed: %w", err)
	}

	if h.config.Schedules == nil || (h.config.Schedules.ForwardFill == "" && h.config.Schedules.Backfill == "") {
		return ErrNoSchedulesConfig
	}

	if err := h.config.Schedules.Validate(); err != nil {
		return fmt.Errorf("schedules validation failed: %w", err)
	}

	if len(h.config.Dependencies) == 0 {
		return ErrDependenciesRequired
	}

	if h.config.Fill != nil {
		if err := h.config.Fill.Validate(); err != nil {
			return fmt.Errorf("fill validation failed: %w", err)
		}
	}

	return nil
}

// ShouldTrackPosition returns true for incremental transformations
func (h *Handler) ShouldTrackPosition() bool {
	return true
}

// GetTemplateVariables returns template variables for incremental transformations
func (h *Handler) GetTemplateVariables(_ context.Context, taskInfo transformation.TaskInfo) map[string]any {
	return map[string]any{
		"bounds": map[string]any{
			"start": taskInfo.Position,
			"end":   taskInfo.Position + taskInfo.Interval,
		},
		"task": map[string]any{
			"start":     taskInfo.Timestamp.Unix(),
			"direction": taskInfo.Direction,
		},
	}
}

// GetAdminTable returns the admin table configuration
func (h *Handler) GetAdminTable() transformation.AdminTable {
	return h.adminTable
}

// RecordCompletion records the completion of an incremental transformation
func (h *Handler) RecordCompletion(ctx context.Context, adminService any, modelID string, taskInfo transformation.TaskInfo) error {
	type adminRecorder interface {
		RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error
	}

	recorder, ok := adminService.(adminRecorder)
	if !ok {
		return ErrAdminServiceInvalid
	}

	return recorder.RecordCompletion(ctx, modelID, taskInfo.Position, taskInfo.Interval)
}

// GetID returns the unique identifier for the transformation model
func (h *Handler) GetID() string {
	return modelid.Format(h.config.Database, h.config.Table)
}

// GetMaxInterval returns the maximum interval size
func (h *Handler) GetMaxInterval() uint64 {
	if h.config.Interval != nil {
		return h.config.Interval.Max
	}
	return 0
}

// GetMinInterval returns the minimum interval size
func (h *Handler) GetMinInterval() uint64 {
	if h.config.Interval != nil {
		return h.config.Interval.Min
	}
	return 0
}

// GetIntervalType returns the interval type for this incremental transformation
func (h *Handler) GetIntervalType() string {
	if h.config.Interval != nil {
		return h.config.Interval.Type
	}
	return ""
}

// GetDependencies returns the dependencies (after placeholder substitution)
func (h *Handler) GetDependencies() []transformation.Dependency {
	return h.config.Dependencies
}

// GetFlattenedDependencies returns all dependencies as a flat string array
func (h *Handler) GetFlattenedDependencies() []string {
	result := []string{}
	for _, dep := range h.config.Dependencies {
		result = append(result, dep.GetAllDependencies()...)
	}
	return result
}

// GetOriginalDependencies returns the original dependencies before placeholder substitution
func (h *Handler) GetOriginalDependencies() []transformation.Dependency {
	return h.config.OriginalDependencies
}

// SubstituteDependencyPlaceholders replaces {{external}} and {{transformation}} placeholders
func (h *Handler) SubstituteDependencyPlaceholders(externalDefaultDB, transformationDefaultDB string) {
	h.config.OriginalDependencies = transformation.SubstituteDependencyPlaceholders(
		h.config.Dependencies,
		externalDefaultDB,
		transformationDefaultDB,
	)
}

// IsForwardFillEnabled returns true if forward fill schedule is configured
func (h *Handler) IsForwardFillEnabled() bool {
	return h.config.Schedules != nil && h.config.Schedules.ForwardFill != ""
}

// IsBackfillEnabled returns true if backfill schedule is configured
func (h *Handler) IsBackfillEnabled() bool {
	return h.config.Schedules != nil && h.config.Schedules.Backfill != ""
}

// GetFillDirection returns the configured fill direction ("head" or "tail")
func (h *Handler) GetFillDirection() string {
	if h.config.Fill != nil && h.config.Fill.Direction != "" {
		return h.config.Fill.Direction
	}
	return "head" // default
}

// AllowGapSkipping returns whether gap skipping is allowed during forward fill
func (h *Handler) AllowGapSkipping() bool {
	if h.config.Fill != nil && h.config.Fill.AllowGapSkipping != nil {
		return *h.config.Fill.AllowGapSkipping
	}
	return true // default: allow gap skipping
}

// GetFillBuffer returns the configured fill buffer (how far behind dependencies to stay)
func (h *Handler) GetFillBuffer() uint64 {
	if h.config.Fill != nil {
		return h.config.Fill.Buffer
	}
	return 0
}

// AllowsPartialIntervals returns true if min interval is 0 (allows partial processing)
func (h *Handler) AllowsPartialIntervals() bool {
	return h.config.Interval != nil && h.config.Interval.Min == 0
}

// GetLimits returns the position limits configuration
func (h *Handler) GetLimits() *transformation.Limits {
	if h.config.Limits == nil {
		return nil
	}

	return &transformation.Limits{
		Min: h.config.Limits.Min,
		Max: h.config.Limits.Max,
	}
}

// GetInterval returns the min and max interval sizes (API handler interface)
func (h *Handler) GetInterval() (minInterval, maxInterval uint64) {
	return h.GetMinInterval(), h.GetMaxInterval()
}

// GetSchedules returns the forwardfill and backfill schedules (API handler interface)
func (h *Handler) GetSchedules() (forwardfill, backfill string) {
	if h.config.Schedules == nil {
		return "", ""
	}
	return h.config.Schedules.ForwardFill, h.config.Schedules.Backfill
}

// GetTags returns the tags for this transformation (API handler interface)
func (h *Handler) GetTags() []string {
	return h.config.Tags
}

// GetFlatDependencies returns dependencies as string slice (API handler interface)
func (h *Handler) GetFlatDependencies() []string {
	return h.GetFlattenedDependencies()
}

// ApplyOverrides applies configuration overrides to this incremental transformation handler
// Uses reflection to avoid circular dependency with models package
func (h *Handler) ApplyOverrides(override interface{}) {
	v := reflect.ValueOf(override)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}

	// Apply interval override (only if interval config exists)
	if h.config.Interval != nil {
		h.config.Interval.Min, h.config.Interval.Max, _ = transformation.ApplyMinMaxOverride(
			"Interval", h.config.Interval.Min, h.config.Interval.Max, v,
		)
	}

	// Apply schedules override (only if schedules config exists)
	if h.config.Schedules != nil {
		h.config.Schedules.ForwardFill, h.config.Schedules.Backfill = transformation.ApplySchedulesOverride(
			h.config.Schedules.ForwardFill, h.config.Schedules.Backfill, v,
		)
	}

	// Apply limits override (creates LimitsConfig if needed)
	if minVal, maxVal, found := transformation.ApplyMinMaxOverride("Limits", 0, 0, v); found {
		if h.config.Limits == nil {
			h.config.Limits = &LimitsConfig{}
		}
		h.config.Limits.Min = minVal
		h.config.Limits.Max = maxVal
	}

	h.config.Tags = transformation.ApplyTagsOverride(h.config.Tags, v)
}
