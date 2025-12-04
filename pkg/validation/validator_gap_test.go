package validation

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestProcessGapsForRange(t *testing.T) {
	validator := &dependencyValidator{}

	tests := []struct {
		name           string
		gaps           []admin.GapInfo
		position       uint64
		endPos         uint64
		expectedNext   uint64
		expectedHasGap bool
	}{
		{
			name:           "single gap in range",
			gaps:           []admin.GapInfo{{StartPos: 105, EndPos: 115}},
			position:       100,
			endPos:         150,
			expectedNext:   115,
			expectedHasGap: true,
		},
		{
			name: "overlapping gaps, returns furthest end",
			gaps: []admin.GapInfo{
				{StartPos: 105, EndPos: 115},
				{StartPos: 110, EndPos: 125},
			},
			position:       100,
			endPos:         150,
			expectedNext:   125, // Furthest gap end
			expectedHasGap: true,
		},
		{
			name:           "gaps outside range ignored",
			gaps:           []admin.GapInfo{{StartPos: 200, EndPos: 210}},
			position:       100,
			endPos:         150,
			expectedNext:   0,
			expectedHasGap: false,
		},
		{
			// Tests non-contiguous gaps (separated gaps)
			// Both gaps affect our range, so we take the furthest end
			// This simulates fragmented data with multiple missing ranges
			name: "multiple separate gaps, returns furthest end",
			gaps: []admin.GapInfo{
				{StartPos: 105, EndPos: 115}, // First gap
				{StartPos: 130, EndPos: 140}, // Second gap (separate)
			},
			position:       100,
			endPos:         150,
			expectedNext:   140, // Skip to end of furthest gap
			expectedHasGap: true,
		},
		{
			name:           "gap partially outside range",
			gaps:           []admin.GapInfo{{StartPos: 140, EndPos: 160}},
			position:       100,
			endPos:         150,
			expectedNext:   160, // Gap extends beyond our range
			expectedHasGap: true,
		},
		{
			name:           "no gaps",
			gaps:           []admin.GapInfo{},
			position:       100,
			endPos:         150,
			expectedNext:   0,
			expectedHasGap: false,
		},
		{
			name:           "gap starts at position boundary",
			gaps:           []admin.GapInfo{{StartPos: 100, EndPos: 110}},
			position:       100,
			endPos:         150,
			expectedNext:   110,
			expectedHasGap: true,
		},
		{
			name:           "gap ends at position boundary",
			gaps:           []admin.GapInfo{{StartPos: 90, EndPos: 100}},
			position:       100,
			endPos:         150,
			expectedNext:   0,
			expectedHasGap: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextPos, hasGap := validator.processGapsForRange(tt.gaps, tt.position, tt.endPos)
			assert.Equal(t, tt.expectedNext, nextPos)
			assert.Equal(t, tt.expectedHasGap, hasGap)
		})
	}
}

// TestCheckORGroupGaps tests the OR group gap checking logic.
// An OR group only blocks if ALL members have gaps at the requested position.
func TestCheckORGroupGaps(t *testing.T) {
	tests := []struct {
		name            string
		groupDeps       []string
		position        uint64
		endPos          uint64
		setupMocks      func(*mockDAGReader, *mockAdmin)
		expectedNextPos uint64
		expectedHasGaps bool
	}{
		{
			name:      "single member with no gaps - OR group has no gaps",
			groupDeps: []string{"dep.model1"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, adm *mockAdmin) {
				// dep.model1 is an incremental transformation with no gaps
				dag.nodes["dep.model1"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model1", interval: 100},
				}
				adm.firstPositions["dep.model1"] = 0
				adm.lastPositions["dep.model1"] = 500
				// No gaps returned by FindGaps
			},
			expectedNextPos: 0,
			expectedHasGaps: false,
		},
		{
			name:      "single member with gaps - OR group has gaps",
			groupDeps: []string{"dep.model1"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, adm *mockAdmin) {
				dag.nodes["dep.model1"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model1", interval: 100},
				}
				adm.firstPositions["dep.model1"] = 0
				adm.lastPositions["dep.model1"] = 500
				adm.gaps = map[string][]admin.GapInfo{
					"dep.model1": {{StartPos: 150, EndPos: 180}},
				}
			},
			expectedNextPos: 180,
			expectedHasGaps: true,
		},
		{
			name:      "two members - one has gaps, one doesn't - OR group has no gaps",
			groupDeps: []string{"dep.model1", "dep.model2"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, adm *mockAdmin) {
				// dep.model1 has a gap
				dag.nodes["dep.model1"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model1", interval: 100},
				}
				adm.firstPositions["dep.model1"] = 0
				adm.lastPositions["dep.model1"] = 500
				adm.gaps = map[string][]admin.GapInfo{
					"dep.model1": {{StartPos: 150, EndPos: 180}},
				}

				// dep.model2 has no gaps
				dag.nodes["dep.model2"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model2", interval: 100},
				}
				adm.firstPositions["dep.model2"] = 0
				adm.lastPositions["dep.model2"] = 500
				// No gaps for dep.model2
			},
			expectedNextPos: 0,
			expectedHasGaps: false, // One member has no gaps, so OR group is satisfied
		},
		{
			name:      "two members - both have gaps - OR group has gaps with min next pos",
			groupDeps: []string{"dep.model1", "dep.model2"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, adm *mockAdmin) {
				// dep.model1 has a gap ending at 180
				dag.nodes["dep.model1"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model1", interval: 100},
				}
				adm.firstPositions["dep.model1"] = 0
				adm.lastPositions["dep.model1"] = 500
				adm.gaps = map[string][]admin.GapInfo{
					"dep.model1": {{StartPos: 150, EndPos: 180}},
				}

				// dep.model2 has a gap ending at 160
				dag.nodes["dep.model2"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model2", interval: 100},
				}
				adm.firstPositions["dep.model2"] = 0
				adm.lastPositions["dep.model2"] = 500
				adm.gaps["dep.model2"] = []admin.GapInfo{{StartPos: 120, EndPos: 160}}
			},
			expectedNextPos: 160, // Minimum of 180 and 160
			expectedHasGaps: true,
		},
		{
			name:      "three members - all have gaps - returns minimum next valid pos",
			groupDeps: []string{"dep.model1", "dep.model2", "dep.model3"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, adm *mockAdmin) {
				// dep.model1 gap ends at 180
				dag.nodes["dep.model1"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model1", interval: 100},
				}
				adm.firstPositions["dep.model1"] = 0
				adm.lastPositions["dep.model1"] = 500
				adm.gaps = map[string][]admin.GapInfo{
					"dep.model1": {{StartPos: 150, EndPos: 180}},
				}

				// dep.model2 gap ends at 160
				dag.nodes["dep.model2"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model2", interval: 100},
				}
				adm.firstPositions["dep.model2"] = 0
				adm.lastPositions["dep.model2"] = 500
				adm.gaps["dep.model2"] = []admin.GapInfo{{StartPos: 120, EndPos: 160}}

				// dep.model3 gap ends at 190
				dag.nodes["dep.model3"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model3", interval: 100},
				}
				adm.firstPositions["dep.model3"] = 0
				adm.lastPositions["dep.model3"] = 500
				adm.gaps["dep.model3"] = []admin.GapInfo{{StartPos: 110, EndPos: 190}}
			},
			expectedNextPos: 160, // Minimum of 180, 160, 190
			expectedHasGaps: true,
		},
		{
			name:      "external dependency in OR group - no gaps (externals don't have gaps)",
			groupDeps: []string{"ext.model1"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				// External models are not incremental transformations, so no gap checking
				dag.nodes["ext.model1"] = models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    &mockExternal{id: "ext.model1"},
				}
			},
			expectedNextPos: 0,
			expectedHasGaps: false, // External nodes don't have gaps
		},
		{
			name:      "mixed OR group - external and transformation with gaps - no gaps (external satisfies)",
			groupDeps: []string{"ext.model1", "dep.model1"},
			position:  100,
			endPos:    200,
			setupMocks: func(dag *mockDAGReader, adm *mockAdmin) {
				// External model (no gaps by definition)
				dag.nodes["ext.model1"] = models.Node{
					NodeType: models.NodeTypeExternal,
					Model:    &mockExternal{id: "ext.model1"},
				}

				// Transformation with gaps
				dag.nodes["dep.model1"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "dep.model1", interval: 100},
				}
				adm.firstPositions["dep.model1"] = 0
				adm.lastPositions["dep.model1"] = 500
				adm.gaps = map[string][]admin.GapInfo{
					"dep.model1": {{StartPos: 150, EndPos: 180}},
				}
			},
			expectedNextPos: 0,
			expectedHasGaps: false, // External satisfies the OR group
		},
		{
			name:      "empty OR group - no gaps",
			groupDeps: []string{},
			position:  100,
			endPos:    200,
			setupMocks: func(_ *mockDAGReader, _ *mockAdmin) {
				// No setup needed
			},
			expectedNextPos: 0,
			expectedHasGaps: true, // Empty group returns minNextValid=0, hasGaps=true from the loop
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockDAG := newMockDAGReader()
			mockAdmin := newMockAdminWithGaps()

			tt.setupMocks(mockDAG, mockAdmin)

			validator := &dependencyValidator{
				log:   logrus.New().WithField("test", tt.name),
				dag:   mockDAG,
				admin: mockAdmin,
			}

			nextPos, hasGaps := validator.checkORGroupGaps(ctx, tt.groupDeps, tt.position, tt.endPos)

			assert.Equal(t, tt.expectedNextPos, nextPos, "nextValidPos mismatch")
			assert.Equal(t, tt.expectedHasGaps, hasGaps, "hasGaps mismatch")
		})
	}
}

// newMockAdminWithGaps creates a mock admin that supports gap queries
func newMockAdminWithGaps() *mockAdmin {
	return &mockAdmin{
		lastPositions:  make(map[string]uint64),
		firstPositions: make(map[string]uint64),
		gaps:           make(map[string][]admin.GapInfo),
	}
}
