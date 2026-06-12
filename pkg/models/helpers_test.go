package models

import (
	"context"
	"os"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/ethpandaops/cbt/pkg/models/transformation/scheduled"
)

// TestMain registers the transformation handlers once for the whole package so
// direct NewTransformation/NewSQL tests work regardless of -shuffle ordering.
// Registration is idempotent (it overwrites the global registry entry).
func TestMain(m *testing.M) {
	incremental.Register()
	scheduled.Register()
	os.Exit(m.Run())
}

// noDepProviderHandler is a full transformation.Handler that deliberately does NOT
// implement the optional GetFlattenedDependencies sub-interface. It exercises the
// type-assert-failure branches in addModelDependencies, buildDependencyVariables and
// GetTransformationEnvironmentVariables.
type noDepProviderHandler struct{}

func (noDepProviderHandler) Type() transformation.Type { return "scheduled" }
func (noDepProviderHandler) Config() any               { return nil }
func (noDepProviderHandler) Validate() error           { return nil }
func (noDepProviderHandler) ShouldTrackPosition() bool { return false }
func (noDepProviderHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (noDepProviderHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (noDepProviderHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

var _ transformation.Handler = noDepProviderHandler{}
