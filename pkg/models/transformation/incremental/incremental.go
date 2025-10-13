package incremental

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"

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
	return fmt.Sprintf("%s.%s", h.config.Database, h.config.Table)
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

// IsForwardFillEnabled returns true if forward fill schedule is configured
func (h *Handler) IsForwardFillEnabled() bool {
	return h.config.Schedules != nil && h.config.Schedules.ForwardFill != ""
}

// IsBackfillEnabled returns true if backfill schedule is configured
func (h *Handler) IsBackfillEnabled() bool {
	return h.config.Schedules != nil && h.config.Schedules.Backfill != ""
}

// AllowsPartialIntervals returns true if min interval is 0 (allows partial processing)
func (h *Handler) AllowsPartialIntervals() bool {
	return h.config.Interval != nil && h.config.Interval.Min == 0
}

// GetLimits returns the position limits configuration
func (h *Handler) GetLimits() *struct{ Min, Max uint64 } {
	if h.config.Limits == nil {
		return nil
	}
	return &struct{ Min, Max uint64 }{
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

	h.applyIntervalOverride(v)
	h.applySchedulesOverride(v)
	h.applyLimitsOverride(v)
	h.applyTagsOverride(v)
}

func (h *Handler) applyIntervalOverride(v reflect.Value) {
	intervalField := v.FieldByName("Interval")
	if !intervalField.IsValid() || intervalField.IsNil() || h.config.Interval == nil {
		return
	}

	intervalVal := intervalField.Elem()
	if maxField := intervalVal.FieldByName("Max"); maxField.IsValid() && !maxField.IsNil() {
		h.config.Interval.Max = maxField.Elem().Uint()
	}
	if minField := intervalVal.FieldByName("Min"); minField.IsValid() && !minField.IsNil() {
		h.config.Interval.Min = minField.Elem().Uint()
	}
}

func (h *Handler) applySchedulesOverride(v reflect.Value) {
	schedulesField := v.FieldByName("Schedules")
	if !schedulesField.IsValid() || schedulesField.IsNil() || h.config.Schedules == nil {
		return
	}

	schedulesVal := schedulesField.Elem()
	if ffField := schedulesVal.FieldByName("ForwardFill"); ffField.IsValid() && !ffField.IsNil() {
		h.config.Schedules.ForwardFill = ffField.Elem().String()
	}
	if bfField := schedulesVal.FieldByName("Backfill"); bfField.IsValid() && !bfField.IsNil() {
		h.config.Schedules.Backfill = bfField.Elem().String()
	}
}

func (h *Handler) applyLimitsOverride(v reflect.Value) {
	limitsField := v.FieldByName("Limits")
	if !limitsField.IsValid() || limitsField.IsNil() {
		return
	}

	limitsVal := limitsField.Elem()
	if h.config.Limits == nil {
		h.config.Limits = &LimitsConfig{}
	}

	if minField := limitsVal.FieldByName("Min"); minField.IsValid() && !minField.IsNil() {
		h.config.Limits.Min = minField.Elem().Uint()
	}
	if maxField := limitsVal.FieldByName("Max"); maxField.IsValid() && !maxField.IsNil() {
		h.config.Limits.Max = maxField.Elem().Uint()
	}
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
