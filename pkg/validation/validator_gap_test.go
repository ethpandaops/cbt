package validation

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDependenciesWithGaps(t *testing.T) {
	tests := []struct {
		name               string
		modelID            string
		position           uint64
		interval           uint64
		setupMocks         func(dag *mockDAGReader, adminMock *mockAdmin)
		expectedCanProcess bool
		expectedNextValid  uint64
	}{
		{
			name:     "no gaps - should process normally",
			modelID:  "model.b",
			position: 100,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, adminMock *mockAdmin) {
				// Setup TableA as transformation dependency with no gaps
				dag.nodes["model.a"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.a", interval: 100},
				}
				dag.nodes["model.b"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.b", interval: 50, dependencies: []string{"model.a"}},
				}
				dag.dependencies = []string{"model.a"}

				// TableA has data from 0-200 with no gaps
				adminMock.firstPositions["model.a"] = 0
				adminMock.lastPositions["model.a"] = 200
				// No gaps configured
			},
			expectedCanProcess: true,
			expectedNextValid:  0,
		},
		{
			name:     "gap at start of range - should return gap end",
			modelID:  "model.b",
			position: 100,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, adminMock *mockAdmin) {
				// Setup TableA as transformation dependency with gap at 100-110
				dag.nodes["model.a"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.a", interval: 100},
				}
				dag.nodes["model.b"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.b", interval: 50, dependencies: []string{"model.a"}},
				}
				dag.dependencies = []string{"model.a"}

				// TableA has data with gap [100-110]
				adminMock.firstPositions["model.a"] = 0
				adminMock.lastPositions["model.a"] = 200
				adminMock.gaps["model.a"] = []admin.GapInfo{{StartPos: 100, EndPos: 110}}
			},
			expectedCanProcess: false,
			expectedNextValid:  110,
		},
		{
			name:     "gap in middle of range - should return gap end",
			modelID:  "model.b",
			position: 100,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, adminMock *mockAdmin) {
				// Setup TableA as transformation dependency with gap at 120-130
				dag.nodes["model.a"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.a", interval: 100},
				}
				dag.nodes["model.b"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.b", interval: 50, dependencies: []string{"model.a"}},
				}
				dag.dependencies = []string{"model.a"}

				// TableA has data with gap [120-130]
				adminMock.firstPositions["model.a"] = 0
				adminMock.lastPositions["model.a"] = 200
				gap2 := admin.GapInfo{StartPos: 120, EndPos: 130}
				adminMock.gaps["model.a"] = []admin.GapInfo{gap2}
			},
			expectedCanProcess: false,
			expectedNextValid:  130,
		},
		{
			name:     "multiple dependencies with different gaps - should return maximum",
			modelID:  "model.c",
			position: 100,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, adminMock *mockAdmin) {
				// Setup two transformation dependencies with different gaps
				dag.nodes["model.a"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.a", interval: 100},
				}
				dag.nodes["model.b"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.b", interval: 100},
				}
				dag.nodes["model.c"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.c", interval: 50, dependencies: []string{"model.a", "model.b"}},
				}
				dag.dependencies = []string{"model.a", "model.b"}

				// TableA has gap [100-110]
				adminMock.firstPositions["model.a"] = 0
				adminMock.lastPositions["model.a"] = 200
				adminMock.gaps["model.a"] = []admin.GapInfo{{StartPos: 100, EndPos: 110}}

				// TableB has gap [105-120]
				adminMock.firstPositions["model.b"] = 0
				adminMock.lastPositions["model.b"] = 200
				gapB := admin.GapInfo{StartPos: 105, EndPos: 120}
				adminMock.gaps["model.b"] = []admin.GapInfo{gapB}
			},
			expectedCanProcess: false,
			expectedNextValid:  120, // Should return MAX(110, 120) = 120
		},
		{
			name:     "external dependencies ignored - only transformation gaps matter",
			modelID:  "model.b",
			position: 100,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, adminMock *mockAdmin) {
				// Setup one external and one transformation dependency
				dag.nodes["external.a"] = models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    &mockExternal{id: "external.a"},
				}
				dag.nodes["model.a"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.a", interval: 100},
				}
				dag.nodes["model.b"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.b", interval: 50, dependencies: []string{"external.a", "model.a"}},
				}
				dag.dependencies = []string{"external.a", "model.a"}

				// External has no gaps (but we don't check them anyway)
				// Transformation has gap [120-130]
				adminMock.firstPositions["model.a"] = 0
				adminMock.lastPositions["model.a"] = 200
				gap2 := admin.GapInfo{StartPos: 120, EndPos: 130}
				adminMock.gaps["model.a"] = []admin.GapInfo{gap2}
			},
			expectedCanProcess: false,
			expectedNextValid:  130,
		},
		{
			name:     "position outside bounds - should find next valid position after gaps",
			modelID:  "model.b",
			position: 250,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, adminMock *mockAdmin) {
				// Setup TableA as transformation dependency with data only up to 200
				dag.nodes["model.a"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.a", interval: 100},
				}
				dag.nodes["model.b"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.b", interval: 50, dependencies: []string{"model.a"}},
				}
				dag.dependencies = []string{"model.a"}

				// TableA has data from 0-200
				adminMock.firstPositions["model.a"] = 0
				adminMock.lastPositions["model.a"] = 200
				// No gaps, but position 250 is beyond max bounds
			},
			expectedCanProcess: false,
			expectedNextValid:  0, // No valid position beyond bounds
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			mockDag := newMockDAGReader()
			mockAdmin := newMockAdmin()
			mockExternal := &mockExternalModelValidator{
				admin: mockAdmin,
			}

			// Apply test-specific setup
			if tt.setupMocks != nil {
				tt.setupMocks(mockDag, mockAdmin)
			}

			// Create validator
			v := &dependencyValidator{
				log:             logrus.NewEntry(logrus.New()),
				admin:           mockAdmin,
				externalManager: mockExternal,
				dag:             mockDag,
			}

			// Execute
			result, err := v.ValidateDependencies(context.Background(), tt.modelID, tt.position, tt.interval)

			// Verify
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCanProcess, result.CanProcess, "CanProcess mismatch")
			assert.Equal(t, tt.expectedNextValid, result.NextValidPos, "NextValidPos mismatch")
		})
	}
}

func TestFindNextValidPosition(t *testing.T) {
	tests := []struct {
		name         string
		fromPosition uint64
		setupGaps    map[string][]admin.GapInfo
		bounds       *dependencyBounds
		expectedNext uint64
	}{
		{
			name:         "single dependency with gap - jump to gap end",
			fromPosition: 100,
			setupGaps: map[string][]admin.GapInfo{
				"model.a": {{StartPos: 100, EndPos: 110}},
			},
			bounds: &dependencyBounds{
				transformationDeps: []dependencyBound{
					{ModelID: "model.a", MinPos: 0, MaxPos: 200},
				},
			},
			expectedNext: 110,
		},
		{
			name:         "multiple dependencies - take maximum",
			fromPosition: 100,
			setupGaps: map[string][]admin.GapInfo{
				"model.a": {{StartPos: 100, EndPos: 110}},
				"model.b": {{StartPos: 105, EndPos: 120}},
			},
			bounds: &dependencyBounds{
				transformationDeps: []dependencyBound{
					{ModelID: "model.a", MinPos: 0, MaxPos: 200},
					{ModelID: "model.b", MinPos: 0, MaxPos: 200},
				},
			},
			expectedNext: 120, // MAX(110, 120)
		},
		{
			name:         "no gaps - return current position",
			fromPosition: 100,
			setupGaps:    map[string][]admin.GapInfo{},
			bounds: &dependencyBounds{
				transformationDeps: []dependencyBound{
					{ModelID: "model.a", MinPos: 0, MaxPos: 200},
				},
			},
			expectedNext: 100,
		},
		{
			name:         "gap before position - return current position",
			fromPosition: 150,
			setupGaps: map[string][]admin.GapInfo{
				"model.a": {{StartPos: 100, EndPos: 110}},
			},
			bounds: &dependencyBounds{
				transformationDeps: []dependencyBound{
					{ModelID: "model.a", MinPos: 0, MaxPos: 200},
				},
			},
			expectedNext: 150,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock admin with gaps
			mockAdmin := newMockAdmin()
			mockAdmin.gaps = tt.setupGaps

			// Create validator
			v := &dependencyValidator{
				log:   logrus.NewEntry(logrus.New()),
				admin: mockAdmin,
			}

			// Execute
			result := v.findNextValidPosition(context.Background(), tt.bounds, tt.fromPosition)

			// Verify
			assert.Equal(t, tt.expectedNext, result)
		})
	}
}
