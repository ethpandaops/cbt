package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfigSetDefaults(t *testing.T) {
	tests := []struct {
		name            string
		config          *Config
		defaultDatabase string
		expectedDB      string
	}{
		{
			name: "apply default when database is empty",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			defaultDatabase: "default_db",
			expectedDB:      "default_db",
		},
		{
			name: "keep existing database when already set",
			config: &Config{
				Database: "existing_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			defaultDatabase: "default_db",
			expectedDB:      "existing_db",
		},
		{
			name: "no change when default is empty",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			defaultDatabase: "",
			expectedDB:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.config.SetDefaults(tt.defaultDatabase)
			assert.Equal(t, tt.expectedDB, tt.config.Database)
		})
	}
}

func TestConfigSubstituteDependencyPlaceholders(t *testing.T) {
	tests := []struct {
		name                    string
		config                  *Config
		externalDefaultDB       string
		transformationDefaultDB string
		expectedDependencies    []string
	}{
		{
			name: "substitute external placeholders",
			config: &Config{
				Dependencies: []Dependency{
					{IsGroup: false, SingleDep: "{{external}}.beacon_blocks"},
					{IsGroup: false, SingleDep: "{{external}}.validators"},
					{IsGroup: false, SingleDep: "analytics.hourly_stats"},
				},
			},
			externalDefaultDB:       "ethereum",
			transformationDefaultDB: "analytics",
			expectedDependencies: []string{
				"ethereum.beacon_blocks",
				"ethereum.validators",
				"analytics.hourly_stats",
			},
		},
		{
			name: "substitute transformation placeholders",
			config: &Config{
				Dependencies: []Dependency{
					{IsGroup: false, SingleDep: "{{transformation}}.daily_summary"},
					{IsGroup: false, SingleDep: "{{transformation}}.weekly_rollup"},
					{IsGroup: false, SingleDep: "ethereum.blocks"},
				},
			},
			externalDefaultDB:       "ethereum",
			transformationDefaultDB: "analytics",
			expectedDependencies: []string{
				"analytics.daily_summary",
				"analytics.weekly_rollup",
				"ethereum.blocks",
			},
		},
		{
			name: "substitute mixed placeholders",
			config: &Config{
				Dependencies: []Dependency{
					{IsGroup: false, SingleDep: "{{external}}.blocks"},
					{IsGroup: false, SingleDep: "{{transformation}}.hourly"},
					{IsGroup: false, SingleDep: "custom.specific_table"},
				},
			},
			externalDefaultDB:       "raw_data",
			transformationDefaultDB: "processed",
			expectedDependencies: []string{
				"raw_data.blocks",
				"processed.hourly",
				"custom.specific_table",
			},
		},
		{
			name: "no substitution when defaults are empty",
			config: &Config{
				Dependencies: []Dependency{
					{IsGroup: false, SingleDep: "{{external}}.blocks"},
					{IsGroup: false, SingleDep: "{{transformation}}.hourly"},
				},
			},
			externalDefaultDB:       "",
			transformationDefaultDB: "",
			expectedDependencies: []string{
				"{{external}}.blocks",
				"{{transformation}}.hourly",
			},
		},
		{
			name: "no placeholders to substitute",
			config: &Config{
				Dependencies: []Dependency{
					{IsGroup: false, SingleDep: "ethereum.blocks"},
					{IsGroup: false, SingleDep: "analytics.hourly"},
				},
			},
			externalDefaultDB:       "default_external",
			transformationDefaultDB: "default_transform",
			expectedDependencies: []string{
				"ethereum.blocks",
				"analytics.hourly",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.config.SubstituteDependencyPlaceholders(tt.externalDefaultDB, tt.transformationDefaultDB)
			// Verify that the actual dependencies are transformed correctly
			for i, dep := range tt.config.Dependencies {
				if !dep.IsGroup {
					assert.Equal(t, tt.expectedDependencies[i], dep.SingleDep)
				}
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  error
	}{
		{
			name: "valid config with all required fields",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			wantErr: false,
		},
		{
			name: "invalid config without database",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			wantErr: true,
			errMsg:  ErrDatabaseRequired,
		},
		{
			name: "invalid config without table",
			config: &Config{
				Database: "test_db",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			wantErr: true,
			errMsg:  ErrTableRequired,
		},
		{
			name: "valid standalone config without dependencies",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Max: 100,
					Min: 10,
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{},
			},
			wantErr: false,
		},
		{
			name: "valid standalone config without interval",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{}, // No dependencies = standalone
			},
			wantErr: false,
		},
		{
			name: "invalid config with dependencies but no interval",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedules: &SchedulesConfig{
					ForwardFill: "0 * * * *",
				},
				Dependencies: []Dependency{{IsGroup: false, SingleDep: "dep1"}},
			},
			wantErr: true,
			errMsg:  ErrIntervalRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != nil {
					assert.Equal(t, tt.errMsg, err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDependencyUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected Dependency
		wantErr  bool
	}{
		{
			name: "single string dependency",
			yaml: `"ethereum.blocks"`,
			expected: Dependency{
				IsGroup:   false,
				SingleDep: "ethereum.blocks",
			},
			wantErr: false,
		},
		{
			name: "OR group dependency",
			yaml: `["ethereum.blocks", "ethereum.blocks_v2"]`,
			expected: Dependency{
				IsGroup:   true,
				GroupDeps: []string{"ethereum.blocks", "ethereum.blocks_v2"},
			},
			wantErr: false,
		},
		{
			name: "OR group with multiple items",
			yaml: `["source1.data", "source2.data", "source3.data"]`,
			expected: Dependency{
				IsGroup:   true,
				GroupDeps: []string{"source1.data", "source2.data", "source3.data"},
			},
			wantErr: false,
		},
		{
			name:    "empty OR group",
			yaml:    `[]`,
			wantErr: true,
		},
		{
			name:    "invalid type - number",
			yaml:    `123`,
			wantErr: true,
		},
		{
			name:    "invalid type - object",
			yaml:    `{key: value}`,
			wantErr: true,
		},
		{
			name:    "invalid array element",
			yaml:    `["valid", 123]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dep Dependency
			err := yaml.Unmarshal([]byte(tt.yaml), &dep)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, dep)
			}
		})
	}
}

func TestDependencyMarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		dep      Dependency
		expected string
	}{
		{
			name: "single dependency",
			dep: Dependency{
				IsGroup:   false,
				SingleDep: "ethereum.blocks",
			},
			expected: "ethereum.blocks\n",
		},
		{
			name: "OR group",
			dep: Dependency{
				IsGroup:   true,
				GroupDeps: []string{"source1.data", "source2.data"},
			},
			expected: "- source1.data\n- source2.data\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := yaml.Marshal(&tt.dep)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestDependencyGetAllDependencies(t *testing.T) {
	tests := []struct {
		name     string
		dep      Dependency
		expected []string
	}{
		{
			name: "single dependency",
			dep: Dependency{
				IsGroup:   false,
				SingleDep: "ethereum.blocks",
			},
			expected: []string{"ethereum.blocks"},
		},
		{
			name: "OR group",
			dep: Dependency{
				IsGroup:   true,
				GroupDeps: []string{"source1.data", "source2.data", "source3.data"},
			},
			expected: []string{"source1.data", "source2.data", "source3.data"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.dep.GetAllDependencies()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConfigWithMixedDependencies(t *testing.T) {
	yamlContent := `
database: analytics
table: combined_data
interval:
  max: 3600
  min: 0
schedules:
  forwardfill: "@every 1m"
dependencies:
  - ethereum.blocks                              # Single dependency (AND)
  - ["source1.data", "source2.data"]            # OR group
  - analytics.preprocessed                       # Single dependency (AND)
  - ["backup1.blocks", "backup2.blocks", "backup3.blocks"]  # OR group
`

	var config Config
	err := yaml.Unmarshal([]byte(yamlContent), &config)
	require.NoError(t, err)

	// Validate the config
	err = config.Validate()
	require.NoError(t, err)

	// Check that dependencies were parsed correctly
	assert.Len(t, config.Dependencies, 4)

	// First dependency - single
	assert.False(t, config.Dependencies[0].IsGroup)
	assert.Equal(t, "ethereum.blocks", config.Dependencies[0].SingleDep)

	// Second dependency - OR group
	assert.True(t, config.Dependencies[1].IsGroup)
	assert.Equal(t, []string{"source1.data", "source2.data"}, config.Dependencies[1].GroupDeps)

	// Third dependency - single
	assert.False(t, config.Dependencies[2].IsGroup)
	assert.Equal(t, "analytics.preprocessed", config.Dependencies[2].SingleDep)

	// Fourth dependency - OR group
	assert.True(t, config.Dependencies[3].IsGroup)
	assert.Equal(t, []string{"backup1.blocks", "backup2.blocks", "backup3.blocks"}, config.Dependencies[3].GroupDeps)

	// Test GetFlattenedDependencies
	flattened := config.GetFlattenedDependencies()
	expected := []string{
		"ethereum.blocks",
		"source1.data", "source2.data",
		"analytics.preprocessed",
		"backup1.blocks", "backup2.blocks", "backup3.blocks",
	}
	assert.Equal(t, expected, flattened)
}

func TestConfigSubstitutionWithORGroups(t *testing.T) {
	config := &Config{
		Database: "analytics",
		Table:    "test",
		Dependencies: []Dependency{
			{IsGroup: false, SingleDep: "{{external}}.blocks"},
			{IsGroup: true, GroupDeps: []string{"{{external}}.source1", "{{transformation}}.processed"}},
			{IsGroup: false, SingleDep: "{{transformation}}.aggregated"},
		},
	}

	config.SubstituteDependencyPlaceholders("raw_data", "processed_data")

	// Check single dependencies
	assert.Equal(t, "raw_data.blocks", config.Dependencies[0].SingleDep)
	assert.Equal(t, "processed_data.aggregated", config.Dependencies[2].SingleDep)

	// Check OR group
	assert.Equal(t, []string{"raw_data.source1", "processed_data.processed"}, config.Dependencies[1].GroupDeps)

	// Verify originals are saved
	assert.Equal(t, "{{external}}.blocks", config.OriginalDependencies[0].SingleDep)
	assert.Equal(t, []string{"{{external}}.source1", "{{transformation}}.processed"}, config.OriginalDependencies[1].GroupDeps)
}

// TestGetIDWithHyphenatedDatabases tests that GetID correctly handles database names with hyphens
func TestGetIDWithHyphenatedDatabases(t *testing.T) {
	tests := []struct {
		name       string
		database   string
		table      string
		expectedID string
	}{
		{
			name:       "simple hyphenated database",
			database:   "some-database",
			table:      "my_table",
			expectedID: "some-database.my_table",
		},
		{
			name:       "multiple hyphens in database",
			database:   "my-super-long-database",
			table:      "users",
			expectedID: "my-super-long-database.users",
		},
		{
			name:       "hyphen at start and end",
			database:   "-database-",
			table:      "data",
			expectedID: "-database-.data",
		},
		{
			name:       "hyphenated database and table with underscore",
			database:   "analytics-db",
			table:      "user_events",
			expectedID: "analytics-db.user_events",
		},
		{
			name:       "numeric with hyphens",
			database:   "db-2024-01",
			table:      "metrics",
			expectedID: "db-2024-01.metrics",
		},
		{
			name:       "hyphenated database with dependencies",
			database:   "analytics-db",
			table:      "aggregates",
			expectedID: "analytics-db.aggregates",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				Database: tt.database,
				Table:    tt.table,
			}

			// Test GetID
			assert.Equal(t, tt.expectedID, config.GetID())
		})
	}
}
