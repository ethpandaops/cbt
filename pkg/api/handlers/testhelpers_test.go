package handlers

import (
	"context"
	"errors"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
)

// errInjected is a generic error used to exercise error branches in handlers.
var errInjected = errors.New("injected failure")

// newTestServer builds a Server wired with the supplied fakes and a quiet logger.
func newTestServer(dag *testutil.FakeDAGReader, admin *adminfake.FakeAdminService) *Server {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	svc := &testutil.FakeModelsService{DAG: dag}

	return NewServer(svc, admin, IntervalTypesConfig{}, log)
}

// incrementalTestHandler implements the capability interfaces the handlers assert
// against for incremental transformations: intervalProvider, schedulesProvider,
// tagsProvider, incrementalProvider, intervalTypeProvider, positionTracker,
// transformation.LimitsHandler and transformation.FillHandler.
//
// It also satisfies the minimal transformation.Handler surface so it can be
// returned from a FakeTransformation.
type incrementalTestHandler struct {
	minInterval  uint64
	maxInterval  uint64
	forwardfill  string
	backfill     string
	tags         []string
	intervalType string

	deps []string

	limits *transformation.Limits

	fillDirection    string
	allowGapSkipping bool
	fillBuffer       uint64

	trackPosition    bool
	trackPositionSet bool
}

func (h *incrementalTestHandler) Type() transformation.Type { return transformation.TypeIncremental }
func (h *incrementalTestHandler) Config() any               { return nil }
func (h *incrementalTestHandler) Validate() error           { return nil }
func (h *incrementalTestHandler) ShouldTrackPosition() bool {
	if h.trackPositionSet {
		return h.trackPosition
	}
	return true
}

func (h *incrementalTestHandler) GetTemplateVariables(
	_ context.Context, _ transformation.TaskInfo,
) map[string]any {
	return map[string]any{}
}
func (h *incrementalTestHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (h *incrementalTestHandler) RecordCompletion(
	_ context.Context, _ any, _ string, _ transformation.TaskInfo,
) error {
	return nil
}

func (h *incrementalTestHandler) GetInterval() (minInterval, maxInterval uint64) {
	return h.minInterval, h.maxInterval
}

func (h *incrementalTestHandler) GetSchedules() (forwardfill, backfill string) {
	return h.forwardfill, h.backfill
}
func (h *incrementalTestHandler) GetTags() []string                  { return h.tags }
func (h *incrementalTestHandler) GetFlattenedDependencies() []string { return h.deps }
func (h *incrementalTestHandler) GetIntervalType() string            { return h.intervalType }
func (h *incrementalTestHandler) GetLimits() *transformation.Limits  { return h.limits }
func (h *incrementalTestHandler) GetFillDirection() string           { return h.fillDirection }
func (h *incrementalTestHandler) AllowGapSkipping() bool             { return h.allowGapSkipping }
func (h *incrementalTestHandler) GetFillBuffer() uint64              { return h.fillBuffer }

// scheduledTestHandler implements scheduleProvider (GetSchedule + GetTags) and the
// minimal transformation.Handler surface.
type scheduledTestHandler struct {
	schedule string
	tags     []string
}

func (h *scheduledTestHandler) Type() transformation.Type { return transformation.TypeScheduled }
func (h *scheduledTestHandler) Config() any               { return nil }
func (h *scheduledTestHandler) Validate() error           { return nil }
func (h *scheduledTestHandler) ShouldTrackPosition() bool { return false }

func (h *scheduledTestHandler) GetTemplateVariables(
	_ context.Context, _ transformation.TaskInfo,
) map[string]any {
	return map[string]any{}
}
func (h *scheduledTestHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (h *scheduledTestHandler) RecordCompletion(
	_ context.Context, _ any, _ string, _ transformation.TaskInfo,
) error {
	return nil
}
func (h *scheduledTestHandler) GetSchedule() string { return h.schedule }
func (h *scheduledTestHandler) GetTags() []string   { return h.tags }

// structuredDepTestHandler implements structuredDepProvider: it exposes structured
// dependencies via the exact anonymous-struct shape the handlers assert against.
type structuredDepTestHandler struct {
	deps []structuredDep
}

func (h *structuredDepTestHandler) Type() transformation.Type { return transformation.TypeIncremental }
func (h *structuredDepTestHandler) Config() any               { return nil }
func (h *structuredDepTestHandler) Validate() error           { return nil }
func (h *structuredDepTestHandler) ShouldTrackPosition() bool { return true }

func (h *structuredDepTestHandler) GetTemplateVariables(
	_ context.Context, _ transformation.TaskInfo,
) map[string]any {
	return map[string]any{}
}
func (h *structuredDepTestHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (h *structuredDepTestHandler) RecordCompletion(
	_ context.Context, _ any, _ string, _ transformation.TaskInfo,
) error {
	return nil
}
func (h *structuredDepTestHandler) GetDependencies() []structuredDep { return h.deps }

// flatDepTestHandler implements flatDepProvider only (no GetDependencies), forcing
// the handlers to take the flat-dependency fallback path.
type flatDepTestHandler struct {
	deps []string
}

func (h *flatDepTestHandler) Type() transformation.Type { return transformation.TypeIncremental }
func (h *flatDepTestHandler) Config() any               { return nil }
func (h *flatDepTestHandler) Validate() error           { return nil }
func (h *flatDepTestHandler) ShouldTrackPosition() bool { return true }

func (h *flatDepTestHandler) GetTemplateVariables(
	_ context.Context, _ transformation.TaskInfo,
) map[string]any {
	return map[string]any{}
}
func (h *flatDepTestHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (h *flatDepTestHandler) RecordCompletion(
	_ context.Context, _ any, _ string, _ transformation.TaskInfo,
) error {
	return nil
}
func (h *flatDepTestHandler) GetFlattenedDependencies() []string { return h.deps }

// incrementalNoExtrasHandler implements incrementalProvider (GetInterval,
// GetSchedules, GetTags, GetFlattenedDependencies) but deliberately does NOT
// implement transformation.LimitsHandler or transformation.FillHandler, so the
// limits/fill populators take their early-return (!ok) branches.
type incrementalNoExtrasHandler struct{}

func (h *incrementalNoExtrasHandler) Type() transformation.Type {
	return transformation.TypeIncremental
}
func (h *incrementalNoExtrasHandler) Config() any               { return nil }
func (h *incrementalNoExtrasHandler) Validate() error           { return nil }
func (h *incrementalNoExtrasHandler) ShouldTrackPosition() bool { return true }

func (h *incrementalNoExtrasHandler) GetTemplateVariables(
	_ context.Context, _ transformation.TaskInfo,
) map[string]any {
	return map[string]any{}
}
func (h *incrementalNoExtrasHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (h *incrementalNoExtrasHandler) RecordCompletion(
	_ context.Context, _ any, _ string, _ transformation.TaskInfo,
) error {
	return nil
}
func (h *incrementalNoExtrasHandler) GetInterval() (minInterval, maxInterval uint64) { return 1, 2 }
func (h *incrementalNoExtrasHandler) GetSchedules() (forwardfill, backfill string)   { return "", "" }
func (h *incrementalNoExtrasHandler) GetTags() []string                              { return nil }
func (h *incrementalNoExtrasHandler) GetFlattenedDependencies() []string             { return nil }

// intervalOnlyHandler implements only intervalProvider (GetInterval) plus the
// minimal Handler surface, used to exercise autoDetectInterval's interval lookup.
type intervalOnlyHandler struct {
	minInterval uint64
	maxInterval uint64
}

func (h *intervalOnlyHandler) Type() transformation.Type { return transformation.TypeIncremental }
func (h *intervalOnlyHandler) Config() any               { return nil }
func (h *intervalOnlyHandler) Validate() error           { return nil }
func (h *intervalOnlyHandler) ShouldTrackPosition() bool { return true }

func (h *intervalOnlyHandler) GetTemplateVariables(
	_ context.Context, _ transformation.TaskInfo,
) map[string]any {
	return map[string]any{}
}
func (h *intervalOnlyHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}

func (h *intervalOnlyHandler) RecordCompletion(
	_ context.Context, _ any, _ string, _ transformation.TaskInfo,
) error {
	return nil
}

func (h *intervalOnlyHandler) GetInterval() (minInterval, maxInterval uint64) {
	return h.minInterval, h.maxInterval
}
