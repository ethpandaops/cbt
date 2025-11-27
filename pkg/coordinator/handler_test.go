package coordinator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetermineIntervalForGap(t *testing.T) {
	tests := []struct {
		name        string
		gapSize     uint64
		minInterval uint64
		maxInterval uint64
		expected    uint64
	}{
		{
			name:        "min interval is 0, gap smaller than max",
			gapSize:     50,
			minInterval: 0,
			maxInterval: 100,
			expected:    50,
		},
		{
			name:        "min interval is 0, gap larger than max",
			gapSize:     150,
			minInterval: 0,
			maxInterval: 100,
			expected:    100,
		},
		{
			name:        "min interval is 0, gap equals max",
			gapSize:     100,
			minInterval: 0,
			maxInterval: 100,
			expected:    100,
		},
		{
			name:        "gap smaller than min interval",
			gapSize:     5,
			minInterval: 10,
			maxInterval: 100,
			expected:    10,
		},
		{
			name:        "gap between min and max",
			gapSize:     50,
			minInterval: 10,
			maxInterval: 100,
			expected:    50,
		},
		{
			name:        "gap equals min interval",
			gapSize:     10,
			minInterval: 10,
			maxInterval: 100,
			expected:    10,
		},
		{
			name:        "gap equals max interval",
			gapSize:     100,
			minInterval: 10,
			maxInterval: 100,
			expected:    100,
		},
		{
			name:        "gap larger than max interval",
			gapSize:     200,
			minInterval: 10,
			maxInterval: 100,
			expected:    100,
		},
		{
			name:        "all zeros returns zero",
			gapSize:     0,
			minInterval: 0,
			maxInterval: 0,
			expected:    0,
		},
		{
			name:        "gap zero with non-zero min",
			gapSize:     0,
			minInterval: 10,
			maxInterval: 100,
			expected:    10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineIntervalForGap(tt.gapSize, tt.minInterval, tt.maxInterval)
			assert.Equal(t, tt.expected, result)
		})
	}
}
