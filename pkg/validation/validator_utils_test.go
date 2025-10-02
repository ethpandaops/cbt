package validation

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortGapsByStart(t *testing.T) {
	tests := []struct {
		name     string
		input    []admin.GapInfo
		expected []admin.GapInfo
	}{
		{
			name:     "empty slice",
			input:    []admin.GapInfo{},
			expected: []admin.GapInfo{},
		},
		{
			name: "single gap",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
			},
		},
		{
			name: "already sorted",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 300, EndPos: 400},
				{StartPos: 500, EndPos: 600},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 300, EndPos: 400},
				{StartPos: 500, EndPos: 600},
			},
		},
		{
			name: "reverse order",
			input: []admin.GapInfo{
				{StartPos: 500, EndPos: 600},
				{StartPos: 300, EndPos: 400},
				{StartPos: 100, EndPos: 200},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 300, EndPos: 400},
				{StartPos: 500, EndPos: 600},
			},
		},
		{
			name: "random order",
			input: []admin.GapInfo{
				{StartPos: 300, EndPos: 400},
				{StartPos: 100, EndPos: 200},
				{StartPos: 700, EndPos: 800},
				{StartPos: 500, EndPos: 600},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 300, EndPos: 400},
				{StartPos: 500, EndPos: 600},
				{StartPos: 700, EndPos: 800},
			},
		},
		{
			name: "gaps with same start",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 100, EndPos: 150},
				{StartPos: 100, EndPos: 300},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 100, EndPos: 150},
				{StartPos: 100, EndPos: 300},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying the original test data
			gaps := make([]admin.GapInfo, len(tt.input))
			copy(gaps, tt.input)

			sortGapsByStart(gaps)

			require.Equal(t, len(tt.expected), len(gaps))
			for i := range tt.expected {
				assert.Equal(t, tt.expected[i].StartPos, gaps[i].StartPos, "Gap %d start position mismatch", i)
				assert.Equal(t, tt.expected[i].EndPos, gaps[i].EndPos, "Gap %d end position mismatch", i)
			}
		})
	}
}

func TestMergeOverlappingGaps(t *testing.T) {
	tests := []struct {
		name     string
		input    []admin.GapInfo
		expected []admin.GapInfo
		desc     string
	}{
		{
			name:     "empty slice",
			input:    []admin.GapInfo{},
			expected: []admin.GapInfo{},
			desc:     "Empty input should return empty result",
		},
		{
			name: "single gap",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
			},
			desc: "Single gap should remain unchanged",
		},
		{
			name: "non-overlapping gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 300, EndPos: 400},
				{StartPos: 500, EndPos: 600},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 300, EndPos: 400},
				{StartPos: 500, EndPos: 600},
			},
			desc: "Non-overlapping gaps should remain separate",
		},
		{
			name: "fully overlapping gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 300},
				{StartPos: 150, EndPos: 250},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 300},
			},
			desc: "Fully overlapping gaps should merge into one",
		},
		{
			name: "partially overlapping gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 150, EndPos: 250},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 250},
			},
			desc: "Partially overlapping gaps should extend to cover both",
		},
		{
			name: "adjacent gaps (touching)",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 200, EndPos: 300},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 300},
			},
			desc: "Adjacent gaps should merge",
		},
		{
			name: "multiple overlapping gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 150, EndPos: 250},
				{StartPos: 180, EndPos: 350},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 350},
			},
			desc: "Multiple overlapping gaps should merge into one",
		},
		{
			name: "mixed overlapping and non-overlapping",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 150, EndPos: 250},
				{StartPos: 400, EndPos: 500},
				{StartPos: 450, EndPos: 550},
				{StartPos: 700, EndPos: 800},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 250},
				{StartPos: 400, EndPos: 550},
				{StartPos: 700, EndPos: 800},
			},
			desc: "Should merge overlapping groups but keep separate non-overlapping ones",
		},
		{
			name: "gaps with one position between",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 201, EndPos: 300},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 201, EndPos: 300},
			},
			desc: "Gaps with one position between should NOT merge",
		},
		{
			name: "nested gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 500},
				{StartPos: 200, EndPos: 300},
				{StartPos: 350, EndPos: 400},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 500},
			},
			desc: "Nested gaps should be absorbed by the larger gap",
		},
		{
			name: "chain of overlapping gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 200},
				{StartPos: 180, EndPos: 280},
				{StartPos: 260, EndPos: 360},
				{StartPos: 340, EndPos: 440},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 440},
			},
			desc: "Chain of overlapping gaps should merge into one continuous gap",
		},
		{
			name: "unsorted input",
			input: []admin.GapInfo{
				{StartPos: 300, EndPos: 400},
				{StartPos: 100, EndPos: 200},
				{StartPos: 150, EndPos: 250},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 250},
				{StartPos: 300, EndPos: 400},
			},
			desc: "Should handle unsorted input correctly",
		},
		{
			name: "zero-width gaps",
			input: []admin.GapInfo{
				{StartPos: 100, EndPos: 100},
				{StartPos: 200, EndPos: 200},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 100},
				{StartPos: 200, EndPos: 200},
			},
			desc: "Zero-width gaps should remain separate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying the original test data
			gaps := make([]admin.GapInfo, len(tt.input))
			copy(gaps, tt.input)

			// Sort first (as the function expects sorted input)
			sortGapsByStart(gaps)
			result := mergeOverlappingGaps(gaps)

			require.Equal(t, len(tt.expected), len(result), tt.desc)
			for i := range tt.expected {
				assert.Equal(t, tt.expected[i].StartPos, result[i].StartPos,
					"%s - Gap %d start position mismatch", tt.desc, i)
				assert.Equal(t, tt.expected[i].EndPos, result[i].EndPos,
					"%s - Gap %d end position mismatch", tt.desc, i)
			}
		})
	}
}

func TestSortAndMergeIntegration(t *testing.T) {
	// Test that sort + merge work correctly together
	tests := []struct {
		name     string
		input    []admin.GapInfo
		expected []admin.GapInfo
		desc     string
	}{
		{
			name: "complex unsorted overlapping gaps",
			input: []admin.GapInfo{
				{StartPos: 500, EndPos: 600},
				{StartPos: 150, EndPos: 250},
				{StartPos: 100, EndPos: 200},
				{StartPos: 400, EndPos: 450},
				{StartPos: 440, EndPos: 520},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 250},
				{StartPos: 400, EndPos: 600},
			},
			desc: "Should sort then merge correctly",
		},
		{
			name: "all gaps merge into one",
			input: []admin.GapInfo{
				{StartPos: 200, EndPos: 300},
				{StartPos: 100, EndPos: 210},
				{StartPos: 290, EndPos: 400},
				{StartPos: 390, EndPos: 500},
			},
			expected: []admin.GapInfo{
				{StartPos: 100, EndPos: 500},
			},
			desc: "All gaps should merge into single continuous gap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying the original test data
			gaps := make([]admin.GapInfo, len(tt.input))
			copy(gaps, tt.input)

			// This is how the functions are used in the actual code
			sortGapsByStart(gaps)
			result := mergeOverlappingGaps(gaps)

			require.Equal(t, len(tt.expected), len(result), tt.desc)
			for i := range tt.expected {
				assert.Equal(t, tt.expected[i].StartPos, result[i].StartPos,
					"%s - Gap %d start position mismatch", tt.desc, i)
				assert.Equal(t, tt.expected[i].EndPos, result[i].EndPos,
					"%s - Gap %d end position mismatch", tt.desc, i)
			}
		})
	}
}
