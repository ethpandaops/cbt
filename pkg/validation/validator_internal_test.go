package validation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Tests for internal/private methods that cannot be tested from external package

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
			expectedNext:   125,
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
			name: "multiple separate gaps, returns furthest end",
			gaps: []admin.GapInfo{
				{StartPos: 105, EndPos: 115},
				{StartPos: 130, EndPos: 140},
			},
			position:       100,
			endPos:         150,
			expectedNext:   140,
			expectedHasGap: true,
		},
		{
			name:           "gap partially outside range",
			gaps:           []admin.GapInfo{{StartPos: 140, EndPos: 160}},
			position:       100,
			endPos:         150,
			expectedNext:   160,
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

func TestApplyFillBuffer(t *testing.T) {
	tests := []struct {
		name        string
		buffer      uint64
		finalMin    uint64
		finalMax    uint64
		expectedMax uint64
	}{
		{
			name:        "no buffer configured",
			buffer:      0,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 2000,
		},
		{
			name:        "buffer smaller than max",
			buffer:      100,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 1900,
		},
		{
			name:        "buffer equals max",
			buffer:      2000,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 1000,
		},
		{
			name:        "buffer larger than max",
			buffer:      3000,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 1000,
		},
		{
			name:        "buffer applied to large range",
			buffer:      500,
			finalMin:    0,
			finalMax:    10000,
			expectedMax: 9500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := &dependencyValidator{
				log: logrus.New(),
			}

			handler := &bufferHandler{buffer: tt.buffer}

			result := validator.applyFillBuffer(handler, "test_model", tt.finalMin, tt.finalMax)
			assert.Equal(t, tt.expectedMax, result)
		})
	}
}

// bufferHandler is a minimal handler for testing applyFillBuffer
// It only needs to implement GetFillBuffer() since applyFillBuffer
// does a type assertion for the bufferProvider interface
type bufferHandler struct {
	buffer uint64
}

func (h *bufferHandler) GetFillBuffer() uint64 {
	return h.buffer
}

// Implement transformation.Handler interface (minimal stubs for testing)
func (h *bufferHandler) Type() transformation.Type { return "test" }
func (h *bufferHandler) Config() any               { return nil }
func (h *bufferHandler) Validate() error           { return nil }
func (h *bufferHandler) ShouldTrackPosition() bool {
	return true
}
func (h *bufferHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *bufferHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (h *bufferHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

func TestFlexUint64(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    uint64
		expectError bool
	}{
		{
			name:        "numeric value",
			input:       "12345",
			expected:    12345,
			expectError: false,
		},
		{
			name:        "string value",
			input:       `"67890"`,
			expected:    67890,
			expectError: false,
		},
		{
			name:        "large numeric value",
			input:       "18446744073709551615", // max uint64
			expected:    18446744073709551615,
			expectError: false,
		},
		{
			name:        "invalid string",
			input:       `"not_a_number"`,
			expected:    0,
			expectError: true,
		},
		{
			name:        "negative number",
			input:       "-100",
			expected:    0,
			expectError: true,
		},
		{
			name:        "float value",
			input:       "123.45",
			expected:    0,
			expectError: true,
		},
		{
			name:        "null value",
			input:       "null",
			expected:    0,
			expectError: true,
		},
		{
			name:        "empty string",
			input:       `""`,
			expected:    0,
			expectError: true,
		},
		{
			name:        "boolean value",
			input:       "true",
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f FlexUint64
			err := json.Unmarshal([]byte(tt.input), &f)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, uint64(f))
			}
		})
	}
}

func TestFlexUint64Scan(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expected    uint64
		expectError bool
	}{
		{
			name:        "int64 value",
			input:       int64(12345),
			expected:    12345,
			expectError: false,
		},
		{
			name:        "uint64 value",
			input:       uint64(67890),
			expected:    67890,
			expectError: false,
		},
		{
			name:        "int value",
			input:       int(111),
			expected:    111,
			expectError: false,
		},
		{
			name:        "int8 value",
			input:       int8(42),
			expected:    42,
			expectError: false,
		},
		{
			name:        "uint8 value",
			input:       uint8(255),
			expected:    255,
			expectError: false,
		},
		{
			name:        "int16 value",
			input:       int16(1000),
			expected:    1000,
			expectError: false,
		},
		{
			name:        "uint16 value",
			input:       uint16(65535),
			expected:    65535,
			expectError: false,
		},
		{
			name:        "int32 value",
			input:       int32(222),
			expected:    222,
			expectError: false,
		},
		{
			name:        "uint32 value",
			input:       uint32(333),
			expected:    333,
			expectError: false,
		},
		{
			name:        "float64 value",
			input:       float64(444),
			expected:    444,
			expectError: false,
		},
		{
			name:        "string value",
			input:       "12345",
			expected:    12345,
			expectError: false,
		},
		{
			name:        "[]byte value",
			input:       []byte("67890"),
			expected:    67890,
			expectError: false,
		},
		{
			name:        "nil value",
			input:       nil,
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid string",
			input:       "not_a_number",
			expected:    0,
			expectError: true,
		},
		{
			name:        "unsupported type",
			input:       true,
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f FlexUint64
			err := f.Scan(tt.input)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, uint64(f))
			}
		})
	}
}
