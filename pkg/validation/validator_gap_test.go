package validation

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
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
