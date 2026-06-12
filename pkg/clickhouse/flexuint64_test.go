package clickhouse

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				require.NoError(t, err)
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
			name:        "negative int value",
			input:       int(-5),
			expected:    0,
			expectError: true,
		},
		{
			name:        "negative int64 value",
			input:       int64(-7),
			expected:    0,
			expectError: true,
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
				require.NoError(t, err)
				assert.Equal(t, tt.expected, uint64(f))
			}
		})
	}
}
