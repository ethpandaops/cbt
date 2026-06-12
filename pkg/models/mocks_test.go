package models

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// mockHandler is a shared transformation.Handler fake for the models package tests.
//
// dependencies / intervalType / shouldTrackPos drive the optional sub-interfaces that
// dag.go type-asserts (GetFlattenedDependencies, GetIntervalType, ShouldTrackPosition).
type mockHandler struct {
	dependencies   []string
	intervalType   string
	shouldTrackPos bool
}

func (h *mockHandler) GetFlattenedDependencies() []string { return h.dependencies }
func (h *mockHandler) GetIntervalType() string            { return h.intervalType }
func (h *mockHandler) Type() transformation.Type          { return "incremental" }
func (h *mockHandler) Config() any                        { return nil }
func (h *mockHandler) Validate() error                    { return nil }
func (h *mockHandler) ShouldTrackPosition() bool          { return h.shouldTrackPos }
func (h *mockHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *mockHandler) GetAdminTable() transformation.AdminTable { return transformation.AdminTable{} }
func (h *mockHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

var _ transformation.Handler = (*mockHandler)(nil)

// mockTransformation is a shared models.Transformation fake for the models package tests.
// value backs GetValue so template tests can supply a template body.
type mockTransformation struct {
	id      string
	config  transformation.Config
	handler transformation.Handler
	value   string
}

func (m *mockTransformation) GetID() string                      { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config  { return &m.config }
func (m *mockTransformation) GetType() string                    { return "transformation" }
func (m *mockTransformation) GetValue() string                   { return m.value }
func (m *mockTransformation) GetHandler() transformation.Handler { return m.handler }
func (m *mockTransformation) SetDefaultDatabase(defaultDB string) {
	m.config.SetDefaults(defaultDB)
}

var _ Transformation = (*mockTransformation)(nil)

// mockExternal is a shared models.External fake for the models package tests.
// value backs GetValue (template tests); intervalType backs the GetIntervalType
// sub-interface that dag.go type-asserts.
type mockExternal struct {
	id           string
	config       external.Config
	typ          string
	intervalType string
	value        string
}

func (m *mockExternal) GetID() string                      { return m.id }
func (m *mockExternal) GetConfig() external.Config         { return m.config }
func (m *mockExternal) GetConfigMutable() *external.Config { return &m.config }
func (m *mockExternal) GetType() string                    { return m.typ }
func (m *mockExternal) GetValue() string                   { return m.value }
func (m *mockExternal) GetIntervalType() string            { return m.intervalType }
func (m *mockExternal) SetDefaults(defaultCluster, defaultDB string) {
	m.config.SetDefaults(defaultCluster, defaultDB)
}

var _ External = (*mockExternal)(nil)
