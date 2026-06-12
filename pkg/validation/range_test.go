package validation_test

import (
	"context"
	"testing"

	adminmock "github.com/ethpandaops/cbt/pkg/admin/mock"
	"github.com/ethpandaops/cbt/pkg/models"
	modelsmock "github.com/ethpandaops/cbt/pkg/models/mock"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	validationmock "github.com/ethpandaops/cbt/pkg/validation/mock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	errBounds   = assertError("bounds lookup failed")
	errFirstPos = assertError("first position failed")
	errNextPos  = assertError("next position failed")
)

// assertError is a static error helper for tests.
type assertError string

func (e assertError) Error() string { return string(e) }

// plainHandler implements only transformation.Handler (no DependencyHandler,
// LimitsHandler, FillHandler, or IntervalHandler). It is used to exercise the
// type-assertion-failure branches in the validator.
type plainHandler struct {
	trackPosition bool
}

func (h *plainHandler) Type() transformation.Type { return "plain" }
func (h *plainHandler) Config() any               { return nil }
func (h *plainHandler) Validate() error           { return nil }
func (h *plainHandler) ShouldTrackPosition() bool { return h.trackPosition }
func (h *plainHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *plainHandler) GetAdminTable() transformation.AdminTable { return transformation.AdminTable{} }
func (h *plainHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

func newTestLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// TestGetValidRange_Errors covers error and edge branches in GetValidRange,
// getBoundsForDependency, collectDependencyBoundsWithOR, processDependenciesFromHandler,
// processSingleDependency, processORGroup, calculateRangeUnion and hasDependencies.
func TestGetValidRange_Errors(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		semantics   validation.RangeSemantics
		setup       func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedMin uint64
		expectedMax uint64
		wantErr     bool
		errContains string
	}{
		{
			name:      "getTransformationModel error - model not found",
			modelID:   "missing.model",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)
				dag.EXPECT().GetNode("missing.model").Return(models.Node{}, assertError("nope")).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "model not found",
		},
		{
			name:      "no dependencies via nil handler - returns 0,0",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				mockTrans.EXPECT().GetHandler().Return(nil).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()
				return admin, extVal, dag
			},
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:      "handler is not a DependencyHandler - returns 0,0",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				mockTrans.EXPECT().GetHandler().Return(&plainHandler{trackPosition: true}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()
				return admin, extVal, dag
			},
			expectedMin: 0,
			expectedMax: 0,
		},
		{
			name:      "single external dependency bounds error",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "ext.model1"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: ext1,
				}, nil).AnyTimes()

				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(0), uint64(0), errBounds).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "bounds lookup failed",
		},
		{
			name:      "dependency node not found",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "ext.missing"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.missing").Return(models.Node{}, assertError("not in dag")).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "dependency model not found",
		},
		{
			name:      "external node model is not an External - invalid model type",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "ext.badtype"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				// Node typed External but Model does not implement models.External.
				wrongModel := modelsmock.NewMockTransformation(ctrl)

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.badtype").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: wrongModel,
				}, nil).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "invalid dependency model type",
		},
		{
			name:      "dependency node type is invalid",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "dep.unknown"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.unknown").Return(models.Node{
					NodeType: models.NodeType("weird"), Model: mockTrans,
				}, nil).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "invalid dependency type",
		},
		{
			name:      "transformation dependency first position error",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "dep.trans"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				depTrans := modelsmock.NewMockTransformation(ctrl)
				depTrans.EXPECT().GetHandler().Return(&testIncrementalHandler{}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.trans").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: depTrans,
				}, nil).AnyTimes()

				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.trans").Return(uint64(0), errFirstPos).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "first position failed",
		},
		{
			name:      "transformation dependency next position error",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "dep.trans"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				depTrans := modelsmock.NewMockTransformation(ctrl)
				depTrans.EXPECT().GetHandler().Return(&testIncrementalHandler{}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.trans").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: depTrans,
				}, nil).AnyTimes()

				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.trans").Return(uint64(100), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.trans").Return(uint64(0), errNextPos).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "next position failed",
		},
		{
			name:      "incremental transformation dependency contributes to union mins",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "ext.model1"},
					{IsGroup: false, SingleDep: "dep.trans"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				depTrans := modelsmock.NewMockTransformation(ctrl)
				depTrans.EXPECT().GetHandler().Return(&testIncrementalHandler{}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: ext1,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.trans").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: depTrans,
				}, nil).AnyTimes()

				// external: min 100, max 9000
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(9000), nil).AnyTimes()
				// transformation: min 500, max 8000
				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.trans").Return(uint64(500), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.trans").Return(uint64(8000), nil).AnyTimes()
				return admin, extVal, dag
			},
			// Union: finalMin = MAX(MIN(ext mins)=100, MAX(trans mins)=500) = 500
			// finalMax = MIN(9000, 8000) = 8000
			expectedMin: 500,
			expectedMax: 8000,
		},
		{
			name:      "uninitialized transformation dependency",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "dep.trans"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				depTrans := modelsmock.NewMockTransformation(ctrl)
				depTrans.EXPECT().GetHandler().Return(&testIncrementalHandler{}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.trans").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: depTrans,
				}, nil).AnyTimes()

				// Both first and next position are 0 -> uninitialized.
				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.trans").Return(uint64(0), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.trans").Return(uint64(0), nil).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "has not been initialized",
		},
		{
			name:      "non-incremental transformation dependency is skipped from bounds",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "ext.model1"},
					{IsGroup: false, SingleDep: "dep.scheduled"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				// Scheduled (non-incremental) transformation.
				schedTrans := modelsmock.NewMockTransformation(ctrl)
				schedTrans.EXPECT().GetHandler().Return(&plainHandler{trackPosition: false}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: ext1,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.scheduled").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: schedTrans,
				}, nil).AnyTimes()

				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(9000), nil).AnyTimes()
				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.scheduled").Return(uint64(1), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.scheduled").Return(uint64(2), nil).AnyTimes()
				return admin, extVal, dag
			},
			// Only the external contributes; scheduled trans skipped.
			expectedMin: 100,
			expectedMax: 9000,
		},
		{
			name:      "OR group with no available member errors",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: true, GroupDeps: []string{"ext.a", "ext.b"}},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				extA := modelsmock.NewMockExternal(ctrl)
				extA.EXPECT().GetID().Return("ext.a").AnyTimes()
				extB := modelsmock.NewMockExternal(ctrl)
				extB.EXPECT().GetID().Return("ext.b").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.a").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: extA,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.b").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: extB,
				}, nil).AnyTimes()

				// One errors, the other returns empty (0,0) -> no valid member.
				extVal.EXPECT().GetMinMax(gomock.Any(), extA).Return(uint64(0), uint64(0), errBounds)
				extVal.EXPECT().GetMinMax(gomock.Any(), extB).Return(uint64(0), uint64(0), nil)
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "no dependencies in OR group are available",
		},
		{
			name:      "OR group of transformations contributes to bounds",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: true, GroupDeps: []string{"dep.t1", "dep.t2"}},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				t1 := modelsmock.NewMockTransformation(ctrl)
				t1.EXPECT().GetHandler().Return(&testIncrementalHandler{}).AnyTimes()
				t2 := modelsmock.NewMockTransformation(ctrl)
				t2.EXPECT().GetHandler().Return(&testIncrementalHandler{}).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.t1").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: t1,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.t2").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: t2,
				}, nil).AnyTimes()

				// t1: 1000-9000, t2: 2000-8000 -> OR group min=1000, max=9000
				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.t1").Return(uint64(1000), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.t1").Return(uint64(9000), nil).AnyTimes()
				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.t2").Return(uint64(2000), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.t2").Return(uint64(8000), nil).AnyTimes()
				return admin, extVal, dag
			},
			// Union with only transformation mins: finalMin = MAX(trans mins) = 1000
			// finalMax = MIN(trans maxes) = 9000
			expectedMin: 1000,
			expectedMax: 9000,
		},
		{
			name:      "OR group single dependency error propagates",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				// AND single dep that errors, to exercise processSingleDependency error return.
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "dep.trans"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("dep.trans").Return(models.Node{}, assertError("dep missing")).AnyTimes()
				return admin, extVal, dag
			},
			wantErr:     true,
			errContains: "dependency model not found",
		},
		{
			name:      "finalMin greater than finalMax returns zero range",
			modelID:   "model.test",
			semantics: validation.Intersection,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{deps: []transformation.Dependency{
					{IsGroup: false, SingleDep: "ext.a"},
					{IsGroup: false, SingleDep: "ext.b"},
				}}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				extA := modelsmock.NewMockExternal(ctrl)
				extA.EXPECT().GetID().Return("ext.a").AnyTimes()
				extB := modelsmock.NewMockExternal(ctrl)
				extB.EXPECT().GetID().Return("ext.b").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation, Model: mockTrans,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.a").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: extA,
				}, nil).AnyTimes()
				dag.EXPECT().GetNode("ext.b").Return(models.Node{
					NodeType: models.NodeTypeExternal, Model: extB,
				}, nil).AnyTimes()

				// Intersection: min = MAX(mins) = 5000, max = MIN(maxes) = 3000 -> min > max
				extVal.EXPECT().GetMinMax(gomock.Any(), extA).Return(uint64(5000), uint64(6000), nil)
				extVal.EXPECT().GetMinMax(gomock.Any(), extB).Return(uint64(1000), uint64(3000), nil)
				return admin, extVal, dag
			},
			expectedMin: 0,
			expectedMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(newTestLogger(), admin, extVal, dag)

			minPos, maxPos, err := validator.GetValidRange(context.Background(), tt.modelID, tt.semantics)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMin, minPos)
				assert.Equal(t, tt.expectedMax, maxPos)
			}
		})
	}
}

// TestGetValidRange_NoLimitsNoBuffer covers applyLimitsFromHandler and applyFillBuffer
// branches where the handler does not provide limits or buffer.
func TestGetValidRange_NoLimitsNoBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)

	admin := adminmock.NewMockService(ctrl)
	extVal := validationmock.NewMockExternalValidator(ctrl)
	dag := modelsmock.NewMockDAGReader(ctrl)

	mockTrans := modelsmock.NewMockTransformation(ctrl)
	// Handler implements DependencyHandler (for hasDependencies/collect) but not
	// LimitsHandler/FillHandler/bufferProvider via an embedded dependency-only stub.
	handler := &depsOnlyHandler{deps: []transformation.Dependency{
		{IsGroup: false, SingleDep: "ext.model1"},
	}}
	mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

	ext1 := modelsmock.NewMockExternal(ctrl)
	ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

	dag.EXPECT().GetNode("model.test").Return(models.Node{
		NodeType: models.NodeTypeTransformation, Model: mockTrans,
	}, nil).AnyTimes()
	dag.EXPECT().GetNode("ext.model1").Return(models.Node{
		NodeType: models.NodeTypeExternal, Model: ext1,
	}, nil).AnyTimes()

	extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil).AnyTimes()

	validator := validation.NewDependencyValidator(newTestLogger(), admin, extVal, dag)

	minPos, maxPos, err := validator.GetValidRange(context.Background(), "model.test", validation.Union)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), minPos)
	assert.Equal(t, uint64(5000), maxPos)
}

// depsOnlyHandler implements transformation.Handler and DependencyHandler but
// intentionally NOT LimitsHandler, FillHandler or the bufferProvider interface.
type depsOnlyHandler struct {
	deps []transformation.Dependency
}

func (h *depsOnlyHandler) Type() transformation.Type { return "depsonly" }
func (h *depsOnlyHandler) Config() any               { return nil }
func (h *depsOnlyHandler) Validate() error           { return nil }
func (h *depsOnlyHandler) ShouldTrackPosition() bool { return true }
func (h *depsOnlyHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *depsOnlyHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (h *depsOnlyHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}
func (h *depsOnlyHandler) GetDependencies() []transformation.Dependency { return h.deps }
func (h *depsOnlyHandler) GetFlattenedDependencies() []string {
	result := make([]string, 0, len(h.deps))
	for i := range h.deps {
		result = append(result, h.deps[i].GetAllDependencies()...)
	}
	return result
}

// TestApplyLimitsFromHandler_NilLimits ensures a LimitsHandler returning nil limits
// leaves bounds unchanged.
func TestApplyLimitsFromHandler_NilLimits(t *testing.T) {
	ctrl := gomock.NewController(t)

	admin := adminmock.NewMockService(ctrl)
	extVal := validationmock.NewMockExternalValidator(ctrl)
	dag := modelsmock.NewMockDAGReader(ctrl)

	mockTrans := modelsmock.NewMockTransformation(ctrl)
	// testIncrementalHandler implements LimitsHandler; nil limits -> unchanged.
	handler := &testIncrementalHandler{
		deps:   []transformation.Dependency{{IsGroup: false, SingleDep: "ext.model1"}},
		limits: nil,
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

	extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil).AnyTimes()

	validator := validation.NewDependencyValidator(newTestLogger(), admin, extVal, dag)

	minPos, maxPos, err := validator.GetValidRange(context.Background(), "model.test", validation.Union)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), minPos)
	assert.Equal(t, uint64(5000), maxPos)
}
