package validation

import (
	"context"
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	adminmock "github.com/ethpandaops/cbt/pkg/admin/mock"
	"github.com/ethpandaops/cbt/pkg/models"
	modelsmock "github.com/ethpandaops/cbt/pkg/models/mock"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	errDAG      = errors.New("dag lookup failed")
	errAdminOp  = errors.New("admin operation failed")
	errRangeGen = errors.New("range generation failed")
)

func quietLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l.WithField("test", "internal")
}

// depHandler implements transformation.Handler + DependencyHandler with a
// configurable ShouldTrackPosition value.
type depHandler struct {
	deps  []transformation.Dependency
	track bool
}

func (h *depHandler) Type() transformation.Type { return "dep" }
func (h *depHandler) Config() any               { return nil }
func (h *depHandler) Validate() error           { return nil }
func (h *depHandler) ShouldTrackPosition() bool { return h.track }
func (h *depHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *depHandler) GetAdminTable() transformation.AdminTable { return transformation.AdminTable{} }
func (h *depHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}
func (h *depHandler) GetDependencies() []transformation.Dependency { return h.deps }
func (h *depHandler) GetFlattenedDependencies() []string {
	out := make([]string, 0, len(h.deps))
	for i := range h.deps {
		out = append(out, h.deps[i].GetAllDependencies()...)
	}
	return out
}

// bareHandler implements only transformation.Handler.
type bareHandler struct{ track bool }

func (h *bareHandler) Type() transformation.Type { return "bare" }
func (h *bareHandler) Config() any               { return nil }
func (h *bareHandler) Validate() error           { return nil }
func (h *bareHandler) ShouldTrackPosition() bool { return h.track }
func (h *bareHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *bareHandler) GetAdminTable() transformation.AdminTable { return transformation.AdminTable{} }
func (h *bareHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

// TestCollectDependencyBoundsWithOR_NilHandler covers the nil-handler early return.
func TestCollectDependencyBoundsWithOR_NilHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockTrans := modelsmock.NewMockTransformation(ctrl)
	mockTrans.EXPECT().GetHandler().Return(nil)

	v := &dependencyValidator{log: quietLogger()}
	bounds, err := v.collectDependencyBoundsWithOR(context.Background(), mockTrans)
	require.NoError(t, err)
	require.NotNil(t, bounds)
	assert.Empty(t, bounds.externalMins)
}

// TestProcessDependenciesFromHandler_NotDependencyHandler covers the non-cast path.
func TestProcessDependenciesFromHandler_NotDependencyHandler(t *testing.T) {
	v := &dependencyValidator{log: quietLogger()}
	bounds := &dependencyBounds{}
	err := v.processDependenciesFromHandler(context.Background(), &bareHandler{track: true}, bounds)
	require.NoError(t, err)
	assert.Empty(t, bounds.externalMins)
}

// TestApplyLimitsFromHandler_NilHandler covers the nil-handler branch.
func TestApplyLimitsFromHandler_NilHandler(t *testing.T) {
	v := &dependencyValidator{log: quietLogger()}
	minP, maxP := v.applyLimitsFromHandler(nil, 100, 200)
	assert.Equal(t, uint64(100), minP)
	assert.Equal(t, uint64(200), maxP)
}

// TestApplyLimitsFromHandler_NotLimitsHandler covers the non-cast branch.
func TestApplyLimitsFromHandler_NotLimitsHandler(t *testing.T) {
	v := &dependencyValidator{log: quietLogger()}
	minP, maxP := v.applyLimitsFromHandler(&bareHandler{track: true}, 100, 200)
	assert.Equal(t, uint64(100), minP)
	assert.Equal(t, uint64(200), maxP)
}

// TestApplyFillBuffer_NilHandler covers the nil-handler branch.
func TestApplyFillBuffer_NilHandler(t *testing.T) {
	v := &dependencyValidator{log: quietLogger()}
	result := v.applyFillBuffer(nil, "model.test", 100, 200)
	assert.Equal(t, uint64(200), result)
}

// TestApplyFillBuffer_NotBufferProvider covers the non-cast branch.
func TestApplyFillBuffer_NotBufferProvider(t *testing.T) {
	v := &dependencyValidator{log: quietLogger()}
	result := v.applyFillBuffer(&bareHandler{track: true}, "model.test", 100, 200)
	assert.Equal(t, uint64(200), result)
}

// TestGetTransformationModel_Errors covers the non-transformation and cast paths.
func TestGetTransformationModel_Errors(t *testing.T) {
	t.Run("node is not a transformation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		dag.EXPECT().GetNode("ext.model").Return(models.Node{
			NodeType: models.NodeTypeExternal,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		_, err := v.getTransformationModel("ext.model")
		require.ErrorIs(t, err, ErrNotTransformationModel)
	})

	t.Run("node typed transformation but model cast fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		// NodeType transformation but Model is an External (does not satisfy Transformation).
		ext := modelsmock.NewMockExternal(ctrl)
		dag.EXPECT().GetNode("model.bad").Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    ext,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		_, err := v.getTransformationModel("model.bad")
		require.ErrorIs(t, err, ErrFailedModelCast)
	})
}

// TestIsIncrementalTransformation_Branches covers GetNode error, non-transformation
// node, cast failure and nil handler branches.
func TestIsIncrementalTransformation_Branches(t *testing.T) {
	t.Run("get node error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		dag.EXPECT().GetNode("missing").Return(models.Node{}, errDAG)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		assert.False(t, v.isIncrementalTransformation("missing"))
	})

	t.Run("node not a transformation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		dag.EXPECT().GetNode("ext").Return(models.Node{NodeType: models.NodeTypeExternal}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		assert.False(t, v.isIncrementalTransformation("ext"))
	})

	t.Run("model cast fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		ext := modelsmock.NewMockExternal(ctrl)
		dag.EXPECT().GetNode("bad").Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    ext,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		assert.False(t, v.isIncrementalTransformation("bad"))
	})

	t.Run("handler is nil", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		mockTrans := modelsmock.NewMockTransformation(ctrl)
		mockTrans.EXPECT().GetHandler().Return(nil)
		dag.EXPECT().GetNode("nilh").Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    mockTrans,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		assert.False(t, v.isIncrementalTransformation("nilh"))
	})

	t.Run("handler tracks position - is incremental", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		mockTrans := modelsmock.NewMockTransformation(ctrl)
		mockTrans.EXPECT().GetHandler().Return(&bareHandler{track: true})
		dag.EXPECT().GetNode("inc").Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    mockTrans,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		assert.True(t, v.isIncrementalTransformation("inc"))
	})
}

// TestCheckIncrementalDependencyGaps_Branches covers the early-return branches.
func TestCheckIncrementalDependencyGaps_Branches(t *testing.T) {
	t.Run("get node error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		dag.EXPECT().GetNode("model.test").Return(models.Node{}, errDAG)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		next, hasGaps, err := v.checkIncrementalDependencyGaps(context.Background(), "model.test", 100, 10)
		require.ErrorIs(t, err, errDAG)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})

	t.Run("model is not a transformation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		ext := modelsmock.NewMockExternal(ctrl)
		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeExternal,
			Model:    ext,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		next, hasGaps, err := v.checkIncrementalDependencyGaps(context.Background(), "model.test", 100, 10)
		require.NoError(t, err)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})

	t.Run("handler is nil", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		mockTrans := modelsmock.NewMockTransformation(ctrl)
		mockTrans.EXPECT().GetHandler().Return(nil)
		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    mockTrans,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		next, hasGaps, err := v.checkIncrementalDependencyGaps(context.Background(), "model.test", 100, 10)
		require.NoError(t, err)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})

	t.Run("handler is not a DependencyHandler", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		mockTrans := modelsmock.NewMockTransformation(ctrl)
		mockTrans.EXPECT().GetHandler().Return(&bareHandler{track: true})
		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    mockTrans,
		}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		next, hasGaps, err := v.checkIncrementalDependencyGaps(context.Background(), "model.test", 100, 10)
		require.NoError(t, err)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})
}

// TestCheckSingleDependencyGaps_Errors covers the admin error branches (fail-open).
func TestCheckSingleDependencyGaps_Errors(t *testing.T) {
	// incrementalDepNode wires a DAG that reports depID as an incremental transformation.
	setupIncremental := func(ctrl *gomock.Controller, depID string) *modelsmock.MockDAGReader {
		dag := modelsmock.NewMockDAGReader(ctrl)
		depTrans := modelsmock.NewMockTransformation(ctrl)
		depTrans.EXPECT().GetHandler().Return(&bareHandler{track: true}).AnyTimes()
		dag.EXPECT().GetNode(depID).Return(models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    depTrans,
		}, nil).AnyTimes()
		return dag
	}

	t.Run("first position error returns no gaps", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := setupIncremental(ctrl, "dep.x")
		adminSvc := adminmock.NewMockService(ctrl)
		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.x").Return(uint64(0), errAdminOp)

		v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
		next, hasGaps := v.checkSingleDependencyGaps(context.Background(), "dep.x", 100, 200)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})

	t.Run("next position error returns no gaps", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := setupIncremental(ctrl, "dep.x")
		adminSvc := adminmock.NewMockService(ctrl)
		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.x").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.x").Return(uint64(0), errAdminOp)

		v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
		next, hasGaps := v.checkSingleDependencyGaps(context.Background(), "dep.x", 100, 200)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})

	t.Run("find gaps error returns no gaps", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := setupIncremental(ctrl, "dep.x")
		adminSvc := adminmock.NewMockService(ctrl)
		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.x").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.x").Return(uint64(500), nil)
		adminSvc.EXPECT().FindGaps(gomock.Any(), "dep.x", uint64(50), uint64(500), uint64(maxGapQueryLimit)).
			Return(nil, errAdminOp)

		v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
		next, hasGaps := v.checkSingleDependencyGaps(context.Background(), "dep.x", 100, 200)
		assert.Zero(t, next)
		assert.False(t, hasGaps)
	})

	t.Run("gaps found within range", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := setupIncremental(ctrl, "dep.x")
		adminSvc := adminmock.NewMockService(ctrl)
		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.x").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.x").Return(uint64(500), nil)
		adminSvc.EXPECT().FindGaps(gomock.Any(), "dep.x", uint64(50), uint64(500), uint64(maxGapQueryLimit)).
			Return([]admin.GapInfo{{StartPos: 120, EndPos: 180}}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
		next, hasGaps := v.checkSingleDependencyGaps(context.Background(), "dep.x", 100, 200)
		assert.Equal(t, uint64(180), next)
		assert.True(t, hasGaps)
	})
}

// TestCheckORGroupGaps covers the all-members-have-gaps path and the minNextValid update.
func TestCheckORGroupGaps(t *testing.T) {
	t.Run("all members have gaps - returns min next valid", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		adminSvc := adminmock.NewMockService(ctrl)

		for _, depID := range []string{"dep.a", "dep.b"} {
			depTrans := modelsmock.NewMockTransformation(ctrl)
			depTrans.EXPECT().GetHandler().Return(&bareHandler{track: true}).AnyTimes()
			dag.EXPECT().GetNode(depID).Return(models.Node{
				NodeType: models.NodeTypeTransformation,
				Model:    depTrans,
			}, nil).AnyTimes()
		}

		// dep.a gap ends at 180, dep.b gap ends at 150 -> min next valid = 150.
		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.a").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.a").Return(uint64(500), nil)
		adminSvc.EXPECT().FindGaps(gomock.Any(), "dep.a", uint64(50), uint64(500), uint64(maxGapQueryLimit)).
			Return([]admin.GapInfo{{StartPos: 120, EndPos: 180}}, nil)

		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.b").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.b").Return(uint64(500), nil)
		adminSvc.EXPECT().FindGaps(gomock.Any(), "dep.b", uint64(50), uint64(500), uint64(maxGapQueryLimit)).
			Return([]admin.GapInfo{{StartPos: 110, EndPos: 150}}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
		next, hasGaps := v.checkORGroupGaps(context.Background(), []string{"dep.a", "dep.b"}, 100, 200)
		assert.True(t, hasGaps)
		assert.Equal(t, uint64(150), next)
	})

	t.Run("one member has no gaps - group not blocked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		adminSvc := adminmock.NewMockService(ctrl)

		for _, depID := range []string{"dep.a", "dep.b"} {
			depTrans := modelsmock.NewMockTransformation(ctrl)
			depTrans.EXPECT().GetHandler().Return(&bareHandler{track: true}).AnyTimes()
			dag.EXPECT().GetNode(depID).Return(models.Node{
				NodeType: models.NodeTypeTransformation,
				Model:    depTrans,
			}, nil).AnyTimes()
		}

		// dep.a has a gap, but dep.b has none -> group not blocked.
		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.a").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.a").Return(uint64(500), nil)
		adminSvc.EXPECT().FindGaps(gomock.Any(), "dep.a", uint64(50), uint64(500), uint64(maxGapQueryLimit)).
			Return([]admin.GapInfo{{StartPos: 120, EndPos: 180}}, nil)

		adminSvc.EXPECT().GetFirstPosition(gomock.Any(), "dep.b").Return(uint64(50), nil)
		adminSvc.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.b").Return(uint64(500), nil)
		adminSvc.EXPECT().FindGaps(gomock.Any(), "dep.b", uint64(50), uint64(500), uint64(maxGapQueryLimit)).
			Return([]admin.GapInfo{}, nil)

		v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
		next, hasGaps := v.checkORGroupGaps(context.Background(), []string{"dep.a", "dep.b"}, 100, 200)
		assert.False(t, hasGaps)
		assert.Zero(t, next)
	})
}

// errRangeHandler is a handler that triggers a controlled error from GetValidRange by
// returning a dependency whose DAG lookup fails. Used to exercise validator.go error paths.

// TestValidateDependencies_GetValidRangeError covers the GetValidRange error branch.
func TestValidateDependencies_GetValidRangeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	dag := modelsmock.NewMockDAGReader(ctrl)
	adminSvc := adminmock.NewMockService(ctrl)

	mockTrans := modelsmock.NewMockTransformation(ctrl)
	handler := &depHandler{track: true, deps: []transformation.Dependency{
		{IsGroup: false, SingleDep: "dep.broken"},
	}}
	mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

	dag.EXPECT().GetNode("model.test").Return(models.Node{
		NodeType: models.NodeTypeTransformation,
		Model:    mockTrans,
	}, nil).AnyTimes()
	dag.EXPECT().GetNode("dep.broken").Return(models.Node{}, errRangeGen).AnyTimes()

	v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc}
	result, err := v.ValidateDependencies(context.Background(), "model.test", 100, 10)
	require.NoError(t, err)
	assert.False(t, result.CanProcess)
	require.Len(t, result.Errors, 1)
}

// TestValidateDependencies_GapCheckErrorFailsOpen covers the fail-open branch where the
// gap check errors but processing is allowed because bounds were satisfied.
func TestValidateDependencies_GapCheckErrorFailsOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	dag := modelsmock.NewMockDAGReader(ctrl)
	adminSvc := adminmock.NewMockService(ctrl)
	extVal := &stubExternalValidator{minPos: 0, maxPos: 100000}

	// Main model depends on an external (so bounds pass), and the gap check will fail
	// because GetNode for the main model is made to error on the second call path.
	mockTrans := modelsmock.NewMockTransformation(ctrl)
	handler := &depHandler{track: true, deps: []transformation.Dependency{
		{IsGroup: false, SingleDep: "ext.model1"},
	}}
	mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

	ext1 := modelsmock.NewMockExternal(ctrl)
	ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

	// First GetNode("model.test") (in GetValidRange) succeeds; the gap-check call uses a
	// gomock DoAndReturn that errors the second time.
	gomock.InOrder(
		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeTransformation, Model: mockTrans,
		}, nil),
		dag.EXPECT().GetNode("model.test").Return(models.Node{}, errDAG),
	)
	dag.EXPECT().GetNode("ext.model1").Return(models.Node{
		NodeType: models.NodeTypeExternal, Model: ext1,
	}, nil).AnyTimes()

	v := &dependencyValidator{log: quietLogger(), dag: dag, admin: adminSvc, externalManager: extVal}
	result, err := v.ValidateDependencies(context.Background(), "model.test", 100, 10)
	require.NoError(t, err)
	// Fail-open: gap check errored, so CanProcess is true.
	assert.True(t, result.CanProcess)
}

// stubExternalValidator is a minimal ExternalValidator returning fixed bounds.
type stubExternalValidator struct {
	minPos uint64
	maxPos uint64
	err    error
}

func (s *stubExternalValidator) GetMinMax(_ context.Context, _ models.External) (minPos, maxPos uint64, err error) {
	return s.minPos, s.maxPos, s.err
}

// TestGetStartPosition_Branches covers the GetValidRange error, empty-range and
// head-first-near-min branches.
func TestGetStartPosition_Branches(t *testing.T) {
	t.Run("get valid range error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)

		mockTrans := modelsmock.NewMockTransformation(ctrl)
		handler := &depHandler{track: true, deps: []transformation.Dependency{
			{IsGroup: false, SingleDep: "dep.broken"},
		}}
		mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeTransformation, Model: mockTrans,
		}, nil).AnyTimes()
		dag.EXPECT().GetNode("dep.broken").Return(models.Node{}, errRangeGen).AnyTimes()

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		pos, err := v.GetStartPosition(context.Background(), "model.test")
		require.Error(t, err)
		assert.Zero(t, pos)
	})

	t.Run("empty range returns zero", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)

		mockTrans := modelsmock.NewMockTransformation(ctrl)
		// No dependencies -> GetValidRange returns (0,0,nil) -> maxPos==0 -> return 0.
		handler := &depHandler{track: true, deps: []transformation.Dependency{}}
		mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeTransformation, Model: mockTrans,
		}, nil).AnyTimes()

		v := &dependencyValidator{log: quietLogger(), dag: dag}
		pos, err := v.GetStartPosition(context.Background(), "model.test")
		require.NoError(t, err)
		assert.Zero(t, pos)
	})

	t.Run("head-first where max minus interval is below min uses min", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		dag := modelsmock.NewMockDAGReader(ctrl)
		extVal := &stubExternalValidator{minPos: 90, maxPos: 100}

		mockTrans := modelsmock.NewMockTransformation(ctrl)
		// Interval 100 (default GetMaxInterval on bareHandler is not implemented, so
		// use depHandler which lacks IntervalHandler -> interval stays 0). To exercise
		// the "max-interval not greater than min" branch, use an interval handler.
		handler := &intervalDepHandler{
			depHandler:  depHandler{track: true, deps: []transformation.Dependency{{IsGroup: false, SingleDep: "ext.model1"}}},
			maxInterval: 50,
			direction:   "head",
		}
		mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

		ext1 := modelsmock.NewMockExternal(ctrl)
		ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

		dag.EXPECT().GetNode("model.test").Return(models.Node{
			NodeType: models.NodeTypeTransformation, Model: mockTrans,
		}, nil).AnyTimes()
		dag.EXPECT().GetNode("ext.model1").Return(models.Node{
			NodeType: models.NodeTypeExternal, Model: ext1,
		}, nil).AnyTimes()

		v := &dependencyValidator{log: quietLogger(), dag: dag, externalManager: extVal}
		pos, err := v.GetStartPosition(context.Background(), "model.test")
		require.NoError(t, err)
		// max=100, interval=50: max>interval (100>50) but max-interval=50 < min=90 -> use min.
		assert.Equal(t, uint64(90), pos)
	})
}

// intervalDepHandler adds IntervalHandler and FillHandler behavior to depHandler.
type intervalDepHandler struct {
	depHandler
	maxInterval uint64
	direction   string
}

func (h *intervalDepHandler) GetMinInterval() uint64       { return 1 }
func (h *intervalDepHandler) GetMaxInterval() uint64       { return h.maxInterval }
func (h *intervalDepHandler) AllowsPartialIntervals() bool { return false }
func (h *intervalDepHandler) AllowGapSkipping() bool       { return false }
func (h *intervalDepHandler) GetFillDirection() string     { return h.direction }
func (h *intervalDepHandler) GetFillBuffer() uint64        { return 0 }
