package validation_test

import (
	"context"
	"fmt"
	"testing"

	adminpkg "github.com/ethpandaops/cbt/pkg/admin"
	adminmock "github.com/ethpandaops/cbt/pkg/admin/mock"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	modelsmock "github.com/ethpandaops/cbt/pkg/models/mock"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	transmock "github.com/ethpandaops/cbt/pkg/models/transformation/mock"
	"github.com/ethpandaops/cbt/pkg/validation"
	validationmock "github.com/ethpandaops/cbt/pkg/validation/mock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestValidateDependencies_GeneratedMocks(t *testing.T) {
	tests := []struct {
		name           string
		modelID        string
		position       uint64
		interval       uint64
		setup          func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedResult validation.Result
		wantErr        bool
	}{
		{
			name:     "no dependencies - cannot process",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				// Create mock transformation with no dependencies
				mockTrans := modelsmock.NewMockTransformation(ctrl)
				mockHandler := transmock.NewMockHandler(ctrl)
				mockDepHandler := transmock.NewMockDependencyHandler(ctrl)

				// DAG returns the model
				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				// Handler setup
				mockTrans.EXPECT().GetHandler().Return(mockHandler).AnyTimes()
				mockHandler.EXPECT().ShouldTrackPosition().Return(true).AnyTimes()

				// Cast handler to DependencyHandler interface - need a combined mock
				// The handler must implement DependencyHandler
				mockDepHandler.EXPECT().GetDependencies().Return([]transformation.Dependency{}).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess: false,
			},
			wantErr: false,
		},
		{
			name:     "single external dependency satisfied",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				// Create mock transformation
				mockTrans := modelsmock.NewMockTransformation(ctrl)

				// Create a handler that implements both Handler and DependencyHandler
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				// Create mock external
				mockExt := modelsmock.NewMockExternal(ctrl)
				mockExt.EXPECT().GetID().Return("ext.model1").AnyTimes()

				// DAG expectations
				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    mockExt,
				}, nil).AnyTimes()

				// External validator returns valid range covering our position
				extVal.EXPECT().GetMinMax(gomock.Any(), mockExt).Return(uint64(500), uint64(2000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess: true,
			},
			wantErr: false,
		},
		{
			name:     "position before external dependency range",
			modelID:  "model.test",
			position: 100,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				mockExt := modelsmock.NewMockExternal(ctrl)
				mockExt.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    mockExt,
				}, nil).AnyTimes()

				// External data starts at 500, but we're requesting position 100
				extVal.EXPECT().GetMinMax(gomock.Any(), mockExt).Return(uint64(500), uint64(2000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess:   false,
				NextValidPos: 500, // Should skip to where data is available
			},
			wantErr: false,
		},
		{
			name:     "transformation dependency with gaps",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				// Main model
				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "dep.model1"},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				// Dependency model (also incremental transformation)
				depTrans := modelsmock.NewMockTransformation(ctrl)
				depHandler := &testIncrementalHandler{deps: []transformation.Dependency{}}
				depTrans.EXPECT().GetHandler().Return(depHandler).AnyTimes()

				// DAG expectations
				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("dep.model1").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    depTrans,
				}, nil).AnyTimes()

				// Admin expectations for dependency bounds
				admin.EXPECT().GetFirstPosition(gomock.Any(), "dep.model1").Return(uint64(500), nil).AnyTimes()
				admin.EXPECT().GetNextUnprocessedPosition(gomock.Any(), "dep.model1").Return(uint64(2000), nil).AnyTimes()

				// Gap in the dependency at position 1000-1100
				admin.EXPECT().FindGaps(gomock.Any(), "dep.model1", gomock.Any(), gomock.Any(), gomock.Any()).
					Return([]adminpkg.GapInfo{{StartPos: 1000, EndPos: 1100}}, nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess:   false,
				NextValidPos: 1100,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(
				logrus.New().WithField("test", tt.name),
				admin,
				extVal,
				dag,
			)

			result, err := validator.ValidateDependencies(context.Background(), tt.modelID, tt.position, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult.CanProcess, result.CanProcess)
				if tt.expectedResult.NextValidPos > 0 {
					assert.Equal(t, tt.expectedResult.NextValidPos, result.NextValidPos)
				}
			}
		})
	}
}

func TestGetValidRange_GeneratedMocks(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		semantics   validation.RangeSemantics
		setup       func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedMin uint64
		expectedMax uint64
		wantErr     bool
	}{
		{
			name:      "union semantics - uses MIN of external mins",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
						{IsGroup: false, SingleDep: "ext.model2"},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()
				ext2 := modelsmock.NewMockExternal(ctrl)
				ext2.EXPECT().GetID().Return("ext.model2").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model2").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext2,
				}, nil).AnyTimes()

				// ext1: 100-5000, ext2: 200-4000
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil)
				extVal.EXPECT().GetMinMax(gomock.Any(), ext2).Return(uint64(200), uint64(4000), nil)

				return admin, extVal, dag
			},
			expectedMin: 100,  // MIN of 100, 200 (Union)
			expectedMax: 4000, // MIN of 5000, 4000
			wantErr:     false,
		},
		{
			name:      "intersection semantics - uses MAX of all mins",
			modelID:   "model.test",
			semantics: validation.Intersection,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
						{IsGroup: false, SingleDep: "ext.model2"},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()
				ext2 := modelsmock.NewMockExternal(ctrl)
				ext2.EXPECT().GetID().Return("ext.model2").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model2").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext2,
				}, nil).AnyTimes()

				// ext1: 100-5000, ext2: 200-4000
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil)
				extVal.EXPECT().GetMinMax(gomock.Any(), ext2).Return(uint64(200), uint64(4000), nil)

				return admin, extVal, dag
			},
			expectedMin: 200,  // MAX of 100, 200 (Intersection)
			expectedMax: 4000, // MIN of 5000, 4000
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(
				logrus.New().WithField("test", tt.name),
				admin,
				extVal,
				dag,
			)

			minPos, maxPos, err := validator.GetValidRange(context.Background(), tt.modelID, tt.semantics)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMin, minPos)
				assert.Equal(t, tt.expectedMax, maxPos)
			}
		})
	}
}

// testIncrementalHandler is a test helper that implements both Handler and DependencyHandler
type testIncrementalHandler struct {
	deps          []transformation.Dependency
	limits        *transformation.Limits
	fillDirection string
	fillBuffer    uint64
}

func (h *testIncrementalHandler) Type() transformation.Type { return "incremental" }
func (h *testIncrementalHandler) Config() any               { return nil }
func (h *testIncrementalHandler) Validate() error           { return nil }
func (h *testIncrementalHandler) ShouldTrackPosition() bool { return true }
func (h *testIncrementalHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *testIncrementalHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (h *testIncrementalHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}
func (h *testIncrementalHandler) GetMaxInterval() uint64       { return 100 }
func (h *testIncrementalHandler) GetMinInterval() uint64       { return 1 }
func (h *testIncrementalHandler) AllowsPartialIntervals() bool { return false }
func (h *testIncrementalHandler) AllowGapSkipping() bool       { return false }
func (h *testIncrementalHandler) GetDependencies() []transformation.Dependency {
	return h.deps
}
func (h *testIncrementalHandler) GetFlattenedDependencies() []string {
	var result []string
	for _, dep := range h.deps {
		result = append(result, dep.GetAllDependencies()...)
	}
	return result
}
func (h *testIncrementalHandler) GetLimits() *transformation.Limits { return h.limits }
func (h *testIncrementalHandler) GetFillDirection() string          { return h.fillDirection }
func (h *testIncrementalHandler) GetFillBuffer() uint64             { return h.fillBuffer }

func TestGetStartPosition_GeneratedMocks(t *testing.T) {
	tests := []struct {
		name             string
		modelID          string
		setup            func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedPosition uint64
		wantErr          bool
	}{
		{
			name:    "tail-first with multiple externals uses intersection semantics (MAX of mins)",
			modelID: "model.test",
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.traces"},
						{IsGroup: false, SingleDep: "ext.contracts"},
					},
					fillDirection: "tail",
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				extTraces := modelsmock.NewMockExternal(ctrl)
				extTraces.EXPECT().GetID().Return("ext.traces").AnyTimes()
				extContracts := modelsmock.NewMockExternal(ctrl)
				extContracts.EXPECT().GetID().Return("ext.contracts").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.traces").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    extTraces,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.contracts").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    extContracts,
				}, nil).AnyTimes()

				// This is the key scenario: traces starts at 0, contracts starts at 47205
				// With Union semantics (old behavior): min = MIN(0, 47205) = 0 (WRONG - can't process at 0)
				// With Intersection semantics (fix): min = MAX(0, 47205) = 47205 (CORRECT - all deps have data)
				extVal.EXPECT().GetMinMax(gomock.Any(), extTraces).Return(uint64(0), uint64(24338331), nil)
				extVal.EXPECT().GetMinMax(gomock.Any(), extContracts).Return(uint64(47205), uint64(24338331), nil)

				return admin, extVal, dag
			},
			// With the fix, start position should be 47205 (MAX of mins using Intersection)
			// This ensures we start where ALL dependencies have data available
			expectedPosition: 47205,
			wantErr:          false,
		},
		{
			name:    "head-first direction - starts from near max",
			modelID: "model.test",
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
					fillDirection: "head",
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				// External range: 100-5000
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil).AnyTimes()

				return admin, extVal, dag
			},
			// head-first: start at max - interval = 5000 - 100 = 4900
			expectedPosition: 4900,
			wantErr:          false,
		},
		{
			name:    "tail-first direction - starts from min",
			modelID: "model.test",
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
					fillDirection: "tail",
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedPosition: 100, // tail-first starts from min
			wantErr:          false,
		},
		{
			name:    "model not found - returns error",
			modelID: "nonexistent.model",
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				dag.EXPECT().GetNode("nonexistent.model").Return(models.Node{}, fmt.Errorf("%w: nonexistent.model", validation.ErrModelNotFound)).AnyTimes()

				return admin, extVal, dag
			},
			expectedPosition: 0,
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(
				logrus.New().WithField("test", tt.name),
				admin,
				extVal,
				dag,
			)

			pos, err := validator.GetStartPosition(context.Background(), tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPosition, pos)
			}
		})
	}
}

func TestValidateDependencies_ORGroups_GeneratedMocks(t *testing.T) {
	tests := []struct {
		name           string
		modelID        string
		position       uint64
		interval       uint64
		setup          func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedResult validation.Result
		wantErr        bool
	}{
		{
			name:     "OR group - at least one dependency satisfies",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				// OR group: [ext.model1, ext.model2]
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: true, GroupDeps: []string{"ext.model1", "ext.model2"}},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()
				ext2 := modelsmock.NewMockExternal(ctrl)
				ext2.EXPECT().GetID().Return("ext.model2").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model2").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext2,
				}, nil).AnyTimes()

				// For OR groups, the union is taken: min=MIN(mins), max=MAX(maxs)
				// ext1: 100-2000 (covers 1000)
				// ext2: 500-3000 (also covers 1000)
				// OR group valid range: 100-3000 (union)
				// With Intersection semantics: we'd need position 1000-1100 to be covered
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(2000), nil).AnyTimes()
				extVal.EXPECT().GetMinMax(gomock.Any(), ext2).Return(uint64(500), uint64(3000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess: true,
			},
			wantErr: false,
		},
		{
			name:     "OR group - no dependency satisfies",
			modelID:  "model.test",
			position: 3000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: true, GroupDeps: []string{"ext.model1", "ext.model2"}},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()
				ext2 := modelsmock.NewMockExternal(ctrl)
				ext2.EXPECT().GetID().Return("ext.model2").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model2").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext2,
				}, nil).AnyTimes()

				// Neither covers position 3000
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(500), nil).AnyTimes()
				extVal.EXPECT().GetMinMax(gomock.Any(), ext2).Return(uint64(500), uint64(2000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess: false,
			},
			wantErr: false,
		},
		{
			name:     "mixed AND and OR dependencies",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				// AND dependency on ext.required, OR group on [ext.opt1, ext.opt2]
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.required"},
						{IsGroup: true, GroupDeps: []string{"ext.opt1", "ext.opt2"}},
					},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				extRequired := modelsmock.NewMockExternal(ctrl)
				extRequired.EXPECT().GetID().Return("ext.required").AnyTimes()
				extOpt1 := modelsmock.NewMockExternal(ctrl)
				extOpt1.EXPECT().GetID().Return("ext.opt1").AnyTimes()
				extOpt2 := modelsmock.NewMockExternal(ctrl)
				extOpt2.EXPECT().GetID().Return("ext.opt2").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.required").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    extRequired,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.opt1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    extOpt1,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.opt2").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    extOpt2,
				}, nil).AnyTimes()

				// Required dependency covers 1000
				extVal.EXPECT().GetMinMax(gomock.Any(), extRequired).Return(uint64(0), uint64(5000), nil).AnyTimes()
				// opt1 doesn't cover, opt2 covers - OR group satisfied
				extVal.EXPECT().GetMinMax(gomock.Any(), extOpt1).Return(uint64(100), uint64(500), nil).AnyTimes()
				extVal.EXPECT().GetMinMax(gomock.Any(), extOpt2).Return(uint64(500), uint64(2000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(
				logrus.New().WithField("test", tt.name),
				admin,
				extVal,
				dag,
			)

			result, err := validator.ValidateDependencies(context.Background(), tt.modelID, tt.position, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult.CanProcess, result.CanProcess)
			}
		})
	}
}

func TestValidateDependencies_WithLimits_GeneratedMocks(t *testing.T) {
	tests := []struct {
		name           string
		modelID        string
		position       uint64
		interval       uint64
		setup          func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedResult validation.Result
		wantErr        bool
	}{
		{
			name:     "position below limits.min - cannot process with nextValidPos",
			modelID:  "model.test",
			position: 50,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
					limits: &transformation.Limits{Min: 500, Max: 10000},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				// External covers 0-20000, but limits.min is 500
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(0), uint64(20000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess:   false,
				NextValidPos: 500, // Should skip to limits.min
			},
			wantErr: false,
		},
		{
			name:     "position above limits.max - cannot process",
			modelID:  "model.test",
			position: 15000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
					limits: &transformation.Limits{Min: 500, Max: 10000},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(0), uint64(20000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess:   false,
				NextValidPos: 0, // Above max, no next valid position
			},
			wantErr: false,
		},
		{
			name:     "position within limits - can process",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
					limits: &transformation.Limits{Min: 500, Max: 10000},
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(0), uint64(20000), nil).AnyTimes()

				return admin, extVal, dag
			},
			expectedResult: validation.Result{
				CanProcess: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(
				logrus.New().WithField("test", tt.name),
				admin,
				extVal,
				dag,
			)

			result, err := validator.ValidateDependencies(context.Background(), tt.modelID, tt.position, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult.CanProcess, result.CanProcess)
				if tt.expectedResult.NextValidPos > 0 {
					assert.Equal(t, tt.expectedResult.NextValidPos, result.NextValidPos)
				}
			}
		})
	}
}

func TestExternalModelValidator_GetMinMax(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		lag         uint64
		setup       func(ctrl *gomock.Controller) *adminmock.MockService
		expectedMin uint64
		expectedMax uint64
		wantErr     bool
		errContains string
	}{
		{
			name:    "cache hit - returns cached bounds",
			modelID: "ext.test",
			lag:     0,
			setup: func(ctrl *gomock.Controller) *adminmock.MockService {
				admin := adminmock.NewMockService(ctrl)
				admin.EXPECT().GetExternalBounds(gomock.Any(), "ext.test").Return(&adminpkg.BoundsCache{
					ModelID: "ext.test",
					Min:     100,
					Max:     5000,
				}, nil)
				return admin
			},
			expectedMin: 100,
			expectedMax: 5000,
			wantErr:     false,
		},
		{
			name:    "cache hit with lag - applies lag to max",
			modelID: "ext.test",
			lag:     500,
			setup: func(ctrl *gomock.Controller) *adminmock.MockService {
				admin := adminmock.NewMockService(ctrl)
				admin.EXPECT().GetExternalBounds(gomock.Any(), "ext.test").Return(&adminpkg.BoundsCache{
					ModelID: "ext.test",
					Min:     100,
					Max:     5000,
				}, nil)
				return admin
			},
			expectedMin: 100,
			expectedMax: 4500, // 5000 - 500 lag
			wantErr:     false,
		},
		{
			name:    "cache miss - returns error",
			modelID: "ext.test",
			lag:     0,
			setup: func(ctrl *gomock.Controller) *adminmock.MockService {
				admin := adminmock.NewMockService(ctrl)
				admin.EXPECT().GetExternalBounds(gomock.Any(), "ext.test").Return(nil, nil)
				return admin
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
			errContains: "external model bounds not initialized",
		},
		{
			name:    "cache error - returns error",
			modelID: "ext.test",
			lag:     0,
			setup: func(ctrl *gomock.Controller) *adminmock.MockService {
				admin := adminmock.NewMockService(ctrl)
				admin.EXPECT().GetExternalBounds(gomock.Any(), "ext.test").Return(nil, fmt.Errorf("%w: redis connection failed", validation.ErrExternalNotInitialized))
				return admin
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
			errContains: "external model bounds not initialized",
		},
		{
			name:    "lag exceeds max - returns min for both",
			modelID: "ext.test",
			lag:     10000,
			setup: func(ctrl *gomock.Controller) *adminmock.MockService {
				admin := adminmock.NewMockService(ctrl)
				admin.EXPECT().GetExternalBounds(gomock.Any(), "ext.test").Return(&adminpkg.BoundsCache{
					ModelID: "ext.test",
					Min:     100,
					Max:     5000,
				}, nil)
				return admin
			},
			expectedMin: 100,
			expectedMax: 100, // Lag exceeds max, so max is set to min
			wantErr:     false,
		},
		{
			name:    "lag equals max - returns min for both",
			modelID: "ext.test",
			lag:     5000,
			setup: func(ctrl *gomock.Controller) *adminmock.MockService {
				admin := adminmock.NewMockService(ctrl)
				admin.EXPECT().GetExternalBounds(gomock.Any(), "ext.test").Return(&adminpkg.BoundsCache{
					ModelID: "ext.test",
					Min:     100,
					Max:     5000,
				}, nil)
				return admin
			},
			expectedMin: 100,
			expectedMax: 100, // Lag equals max, so max is set to min
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			adminSvc := tt.setup(ctrl)

			validator := validation.NewExternalModelExecutor(
				logrus.New().WithField("test", tt.name),
				adminSvc,
			)

			// Create a mock external model
			mockExt := modelsmock.NewMockExternal(ctrl)
			mockExt.EXPECT().GetID().Return(tt.modelID).AnyTimes()
			mockExt.EXPECT().GetConfig().Return(external.Config{
				Database: "test_db",
				Table:    "test_table",
				Lag:      tt.lag,
			}).AnyTimes()

			minPos, maxPos, err := validator.GetMinMax(context.Background(), mockExt)

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

func TestGetValidRange_WithBuffer_GeneratedMocks(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		semantics   validation.RangeSemantics
		setup       func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader)
		expectedMin uint64
		expectedMax uint64
		wantErr     bool
	}{
		{
			name:      "fill buffer reduces max position",
			modelID:   "model.test",
			semantics: validation.Union,
			setup: func(ctrl *gomock.Controller) (*adminmock.MockService, *validationmock.MockExternalValidator, *modelsmock.MockDAGReader) {
				admin := adminmock.NewMockService(ctrl)
				extVal := validationmock.NewMockExternalValidator(ctrl)
				dag := modelsmock.NewMockDAGReader(ctrl)

				mockTrans := modelsmock.NewMockTransformation(ctrl)
				handler := &testIncrementalHandler{
					deps: []transformation.Dependency{
						{IsGroup: false, SingleDep: "ext.model1"},
					},
					fillBuffer: 1000, // Stay 1000 positions behind
				}
				mockTrans.EXPECT().GetHandler().Return(handler).AnyTimes()

				ext1 := modelsmock.NewMockExternal(ctrl)
				ext1.EXPECT().GetID().Return("ext.model1").AnyTimes()

				dag.EXPECT().GetNode("model.test").Return(models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    mockTrans,
				}, nil).AnyTimes()

				dag.EXPECT().GetNode("ext.model1").Return(models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    ext1,
				}, nil).AnyTimes()

				// External range: 100-5000
				extVal.EXPECT().GetMinMax(gomock.Any(), ext1).Return(uint64(100), uint64(5000), nil)

				return admin, extVal, dag
			},
			expectedMin: 100,
			expectedMax: 4000, // 5000 - 1000 buffer
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			admin, extVal, dag := tt.setup(ctrl)

			validator := validation.NewDependencyValidator(
				logrus.New().WithField("test", tt.name),
				admin,
				extVal,
				dag,
			)

			minPos, maxPos, err := validator.GetValidRange(context.Background(), tt.modelID, tt.semantics)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMin, minPos)
				assert.Equal(t, tt.expectedMax, maxPos)
			}
		})
	}
}
