package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
				Dependencies: []string{"dep1"},
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
				Dependencies: []string{"dep1"},
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
				Dependencies: []string{"dep1"},
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
				Dependencies: []string{
					"{{external}}.beacon_blocks",
					"{{external}}.validators",
					"analytics.hourly_stats",
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
				Dependencies: []string{
					"{{transformation}}.daily_summary",
					"{{transformation}}.weekly_rollup",
					"ethereum.blocks",
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
				Dependencies: []string{
					"{{external}}.blocks",
					"{{transformation}}.hourly",
					"custom.specific_table",
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
				Dependencies: []string{
					"{{external}}.blocks",
					"{{transformation}}.hourly",
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
				Dependencies: []string{
					"ethereum.blocks",
					"analytics.hourly",
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
			assert.Equal(t, tt.expectedDependencies, tt.config.Dependencies)
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
				Dependencies: []string{"dep1"},
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
				Dependencies: []string{"dep1"},
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
				Dependencies: []string{"dep1"},
			},
			wantErr: true,
			errMsg:  ErrTableRequired,
		},
		{
			name: "invalid config without dependencies",
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
				Dependencies: []string{},
			},
			wantErr: true,
			errMsg:  ErrDependenciesRequired,
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
