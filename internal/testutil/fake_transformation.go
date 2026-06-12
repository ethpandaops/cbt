package testutil

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// FakeTransformation is a configurable fake implementing models.Transformation for tests.
//
// The zero value returns empty/zero values from every getter. Set Config to control the
// returned config (GetConfig returns &Config so callers may mutate it); set Handler to
// control the returned handler. Type defaults to "transformation" when empty.
//
// When Handler is nil but Tags is non-nil, GetHandler synthesizes a FakeHandler carrying
// those tags so tag-based filtering (which reads handler.GetTags()) sees them.
type FakeTransformation struct {
	ID      string
	Type    string
	Value   string
	Config  transformation.Config
	Handler transformation.Handler
	Tags    []string

	// SetDefaultDatabaseFn overrides SetDefaultDatabase when set.
	SetDefaultDatabaseFn func(defaultDB string)
}

// GetID returns the configured ID.
func (f *FakeTransformation) GetID() string { return f.ID }

// GetConfig returns a pointer to the configured Config.
func (f *FakeTransformation) GetConfig() *transformation.Config { return &f.Config }

// GetHandler returns the configured Handler. When Handler is nil but Tags is set, it
// returns a FakeHandler carrying those tags; otherwise it returns the Handler (may be nil).
func (f *FakeTransformation) GetHandler() transformation.Handler {
	if f.Handler == nil && f.Tags != nil {
		return &FakeHandler{Tags: f.Tags}
	}

	return f.Handler
}

// GetValue returns the configured Value.
func (f *FakeTransformation) GetValue() string { return f.Value }

// GetType returns the configured Type, defaulting to "transformation".
func (f *FakeTransformation) GetType() string {
	if f.Type != "" {
		return f.Type
	}

	return "transformation"
}

// SetDefaultDatabase applies defaultDB to the config when the database is empty.
func (f *FakeTransformation) SetDefaultDatabase(defaultDB string) {
	if f.SetDefaultDatabaseFn != nil {
		f.SetDefaultDatabaseFn(defaultDB)

		return
	}

	if f.Config.Database == "" {
		f.Config.Database = defaultDB
	}
}

// FakeExternal is a configurable fake implementing models.External for tests.
//
// The zero value returns empty/zero values. Config backs both GetConfig (by value) and
// GetConfigMutable (by pointer). SetDefaults applies the default database when empty.
type FakeExternal struct {
	ID     string
	Type   string
	Value  string
	Config external.Config

	// SetDefaultsFn overrides SetDefaults when set.
	SetDefaultsFn func(defaultCluster, defaultDB string)
}

// GetID returns the configured ID.
func (f *FakeExternal) GetID() string { return f.ID }

// GetConfig returns the configured Config by value.
func (f *FakeExternal) GetConfig() external.Config { return f.Config }

// GetConfigMutable returns a pointer to the configured Config.
func (f *FakeExternal) GetConfigMutable() *external.Config { return &f.Config }

// GetValue returns the configured Value.
func (f *FakeExternal) GetValue() string { return f.Value }

// GetType returns the configured Type, defaulting to "sql".
func (f *FakeExternal) GetType() string {
	if f.Type != "" {
		return f.Type
	}

	return external.TypeSQL
}

// SetDefaults applies the default database to the config when the database is empty.
func (f *FakeExternal) SetDefaults(defaultCluster, defaultDB string) {
	if f.SetDefaultsFn != nil {
		f.SetDefaultsFn(defaultCluster, defaultDB)

		return
	}

	if f.Config.Database == "" && defaultDB != "" {
		f.Config.Database = defaultDB
	}
}

// FakeHandler is a configurable fake implementing transformation.Handler (plus the common
// optional sub-interfaces used in tests) for unit tests.
//
// The zero value reports type "incremental", tracks positions, and returns empty/nil from
// the variable getters. Fields drive the schedule, interval and dependency getters so the
// same fake can stand in for incremental, scheduled and templated handler scenarios.
type FakeHandler struct {
	HandlerType   transformation.Type
	HandlerConfig any
	// TrackPosition overrides ShouldTrackPosition. When nil, ShouldTrackPosition returns true.
	TrackPosition *bool

	AdminTable transformation.AdminTable

	// Schedule / fill flags consumed by the scheduler tests.
	ForwardFillEnabled bool
	ForwardSchedule    string
	BackfillEnabled    bool
	BackfillSchedule   string
	Schedule           string

	// Dependency / interval metadata consumed by the models DAG tests.
	Dependencies []string
	IntervalType string

	// Tags consumed by the worker tag-filtering tests (read via GetTags).
	Tags []string

	// TemplateVariables overrides the value returned by GetTemplateVariables.
	TemplateVariables map[string]any
}

// Type returns the configured handler type, defaulting to TypeIncremental.
func (f *FakeHandler) Type() transformation.Type {
	if f.HandlerType != "" {
		return f.HandlerType
	}

	return transformation.TypeIncremental
}

// Config returns the configured handler config (may be nil).
func (f *FakeHandler) Config() any { return f.HandlerConfig }

// Validate always succeeds.
func (f *FakeHandler) Validate() error { return nil }

// ShouldTrackPosition returns *TrackPosition when set, otherwise true.
func (f *FakeHandler) ShouldTrackPosition() bool {
	if f.TrackPosition != nil {
		return *f.TrackPosition
	}

	return true
}

// GetTemplateVariables returns the configured TemplateVariables, or an empty map.
func (f *FakeHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	if f.TemplateVariables != nil {
		return f.TemplateVariables
	}

	return map[string]any{}
}

// GetAdminTable returns the configured AdminTable.
func (f *FakeHandler) GetAdminTable() transformation.AdminTable { return f.AdminTable }

// RecordCompletion is a no-op that always succeeds.
func (f *FakeHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

// GetFlattenedDependencies returns the configured Dependencies.
func (f *FakeHandler) GetFlattenedDependencies() []string { return f.Dependencies }

// GetIntervalType returns the configured IntervalType.
func (f *FakeHandler) GetIntervalType() string { return f.IntervalType }

// GetTags returns the configured Tags.
func (f *FakeHandler) GetTags() []string { return f.Tags }

// GetSchedule returns the configured Schedule.
func (f *FakeHandler) GetSchedule() string { return f.Schedule }

// IsForwardFillEnabled returns the configured ForwardFillEnabled flag.
func (f *FakeHandler) IsForwardFillEnabled() bool { return f.ForwardFillEnabled }

// GetForwardSchedule returns the configured ForwardSchedule.
func (f *FakeHandler) GetForwardSchedule() string { return f.ForwardSchedule }

// IsBackfillEnabled returns the configured BackfillEnabled flag.
func (f *FakeHandler) IsBackfillEnabled() bool { return f.BackfillEnabled }

// GetBackfillSchedule returns the configured BackfillSchedule.
func (f *FakeHandler) GetBackfillSchedule() string { return f.BackfillSchedule }

var (
	_ models.Transformation  = (*FakeTransformation)(nil)
	_ models.External        = (*FakeExternal)(nil)
	_ transformation.Handler = (*FakeHandler)(nil)
)
