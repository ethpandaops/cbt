package modelid

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	tests := []struct {
		name     string
		database string
		table    string
		expected string
	}{
		{
			name:     "standard names",
			database: "my_database",
			table:    "my_table",
			expected: "my_database.my_table",
		},
		{
			name:     "names with underscores",
			database: "analytics_db",
			table:    "block_propagation",
			expected: "analytics_db.block_propagation",
		},
		{
			name:     "simple names",
			database: "db",
			table:    "table",
			expected: "db.table",
		},
		{
			name:     "empty strings",
			database: "",
			table:    "",
			expected: ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Format(tt.database, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name             string
		modelID          string
		expectedDatabase string
		expectedTable    string
		expectError      bool
	}{
		{
			name:             "valid model ID",
			modelID:          "my_database.my_table",
			expectedDatabase: "my_database",
			expectedTable:    "my_table",
			expectError:      false,
		},
		{
			name:             "valid model ID with underscores",
			modelID:          "analytics_db.block_propagation",
			expectedDatabase: "analytics_db",
			expectedTable:    "block_propagation",
			expectError:      false,
		},
		{
			name:        "invalid - no dot",
			modelID:     "no_dot_here",
			expectError: true,
		},
		{
			name:        "invalid - multiple dots",
			modelID:     "db.schema.table",
			expectError: true,
		},
		{
			name:        "invalid - empty string",
			modelID:     "",
			expectError: true,
		},
		{
			name:             "edge case - just a dot",
			modelID:          ".",
			expectedDatabase: "",
			expectedTable:    "",
			expectError:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database, table, err := Parse(tt.modelID)
			if tt.expectError {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidModelID)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedDatabase, database)
				assert.Equal(t, tt.expectedTable, table)
			}
		})
	}
}

func TestFormatAndParseRoundtrip(t *testing.T) {
	tests := []struct {
		database string
		table    string
	}{
		{"my_database", "my_table"},
		{"analytics_db", "block_propagation"},
		{"db", "table"},
	}

	for _, tt := range tests {
		t.Run(tt.database+"."+tt.table, func(t *testing.T) {
			modelID := Format(tt.database, tt.table)
			database, table, err := Parse(modelID)
			require.NoError(t, err)
			assert.Equal(t, tt.database, database)
			assert.Equal(t, tt.table, table)
		})
	}
}
