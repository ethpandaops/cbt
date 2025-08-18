package models

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelParser_Parse(t *testing.T) {
	parser := NewModelParser()

	tests := []struct {
		name          string
		content       string
		isExternal    bool
		expectedError error
		validate      func(t *testing.T, config ModelConfig)
	}{
		{
			name: "valid external model with SQL",
			content: `---
database: mydb
table: external_table
partition: timestamp
ttl: 1h
---
SELECT * FROM source_table`,
			isExternal:    true,
			expectedError: nil,
			validate: func(t *testing.T, config ModelConfig) {
				assert.Equal(t, "mydb", config.Database)
				assert.Equal(t, "external_table", config.Table)
				assert.Equal(t, "timestamp", config.Partition)
				assert.Equal(t, time.Hour, config.TTL)
				assert.Equal(t, "SELECT * FROM source_table", config.Content)
				assert.True(t, config.External)
			},
		},
		{
			name: "valid external model with exec",
			content: `---
database: mydb
table: external_exec
partition: slot
exec: /usr/bin/custom_script.sh
ttl: 2h
---
`,
			isExternal:    true,
			expectedError: nil,
			validate: func(t *testing.T, config ModelConfig) {
				assert.Equal(t, "mydb", config.Database)
				assert.Equal(t, "external_exec", config.Table)
				assert.Equal(t, "/usr/bin/custom_script.sh", config.Exec)
				assert.Equal(t, "/usr/bin/custom_script.sh", config.Content)
				assert.Equal(t, 2*time.Hour, config.TTL)
			},
		},
		{
			name: "valid transformation model",
			content: `---
database: mydb
table: transform_table
partition: timestamp
interval: 3600
schedule: "@every 1h"
dependencies:
  - mydb.source_table
---
INSERT INTO mydb.transform_table
SELECT * FROM mydb.source_table`,
			isExternal:    false,
			expectedError: nil,
			validate: func(t *testing.T, config ModelConfig) {
				assert.Equal(t, "mydb", config.Database)
				assert.Equal(t, "transform_table", config.Table)
				assert.Equal(t, uint64(3600), config.Interval)
				assert.Equal(t, "@every 1h", config.Schedule)
				assert.Contains(t, config.Dependencies, "mydb.source_table")
				assert.Contains(t, config.Content, "INSERT INTO")
				assert.False(t, config.External)
			},
		},
		{
			name: "external model with interval should fail",
			content: `---
database: mydb
table: bad_external
partition: timestamp
interval: 3600
---
SELECT * FROM source`,
			isExternal:    true,
			expectedError: ErrExternalNoInterval,
		},
		{
			name: "external model with schedule should fail",
			content: `---
database: mydb
table: bad_external
partition: timestamp
schedule: "@every 1h"
---
SELECT * FROM source`,
			isExternal:    true,
			expectedError: ErrExternalNoSchedule,
		},
		{
			name: "external model with dependencies should fail",
			content: `---
database: mydb
table: bad_external
partition: timestamp
dependencies:
  - other.table
---
SELECT * FROM source`,
			isExternal:    true,
			expectedError: ErrExternalNoDependencies,
		},
		{
			name: "transformation model without interval should fail",
			content: `---
database: mydb
table: bad_transform
partition: timestamp
schedule: "@every 1h"
---
INSERT INTO table`,
			isExternal:    false,
			expectedError: ErrTransformationIntervalRequired,
		},
		{
			name: "transformation model without schedule should fail",
			content: `---
database: mydb
table: bad_transform
partition: timestamp
interval: 3600
---
INSERT INTO table`,
			isExternal:    false,
			expectedError: ErrTransformationScheduleRequired,
		},
		{
			name: "transformation model with TTL should fail",
			content: `---
database: mydb
table: bad_transform
partition: timestamp
interval: 3600
schedule: "@every 1h"
ttl: 2h
---
INSERT INTO table`,
			isExternal:    false,
			expectedError: ErrTransformationNoTTL,
		},
		{
			name: "missing database should fail",
			content: `---
table: no_database
partition: timestamp
---
`,
			isExternal:    true,
			expectedError: ErrDatabaseRequired,
		},
		{
			name: "missing table should fail",
			content: `---
database: mydb
partition: timestamp
---
`,
			isExternal:    true,
			expectedError: ErrTableRequired,
		},
		{
			name: "missing partition should fail",
			content: `---
database: mydb
table: no_partition
---
`,
			isExternal:    true,
			expectedError: ErrPartitionRequired,
		},
		{
			name: "pure YAML without frontmatter",
			content: `database: mydb
table: yaml_table
partition: timestamp
exec: /bin/script.sh`,
			isExternal:    true,
			expectedError: nil,
			validate: func(t *testing.T, config ModelConfig) {
				assert.Equal(t, "mydb", config.Database)
				assert.Equal(t, "yaml_table", config.Table)
				assert.Equal(t, "/bin/script.sh", config.Content)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			filePath := filepath.Join(tmpDir, "model.yaml")
			err := os.WriteFile(filePath, []byte(tt.content), 0o600)
			require.NoError(t, err)

			// Parse the file
			config, err := parser.Parse(ModelFile{
				FilePath:   filePath,
				IsExternal: tt.isExternal,
			})

			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
			} else {
				require.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, config)
				}
			}
		})
	}
}

func TestModelParser_GetModelID(t *testing.T) {
	parser := NewModelParser()

	tests := []struct {
		name     string
		config   ModelConfig
		expected string
	}{
		{
			name: "simple model ID",
			config: ModelConfig{
				Database: "mydb",
				Table:    "mytable",
			},
			expected: "mydb.mytable",
		},
		{
			name: "model ID with underscores",
			config: ModelConfig{
				Database: "my_database",
				Table:    "my_table_name",
			},
			expected: "my_database.my_table_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modelID := parser.GetModelID(&tt.config)
			assert.Equal(t, tt.expected, modelID)
		})
	}
}

func TestModelParser_parseFrontmatter(t *testing.T) {
	parser := NewModelParser()

	tests := []struct {
		name          string
		content       string
		expectedError bool
		expectedSQL   string
	}{
		{
			name: "valid frontmatter",
			content: `---
database: test
table: test_table
partition: timestamp
---
SELECT * FROM table`,
			expectedError: false,
			expectedSQL:   "SELECT * FROM table",
		},
		{
			name: "missing separator",
			content: `---
database: test
table: test_table
SELECT * FROM table`,
			expectedError: true,
		},
		{
			name: "invalid YAML in frontmatter",
			content: `---
database: [invalid
---
SELECT * FROM table`,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, sql, err := parser.parseFrontmatter([]byte(tt.content), "test.yaml")

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedSQL, sql)
				assert.NotEmpty(t, config.Database)
			}
		})
	}
}
