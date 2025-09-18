package models

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelOverride_IsDisabled(t *testing.T) {
	tests := []struct {
		name     string
		override *ModelOverride
		expected bool
	}{
		{
			name:     "nil override",
			override: nil,
			expected: false,
		},
		{
			name:     "enabled not set",
			override: &ModelOverride{},
			expected: false,
		},
		{
			name: "explicitly enabled",
			override: &ModelOverride{
				Enabled: boolPtr(true),
			},
			expected: false,
		},
		{
			name: "explicitly disabled",
			override: &ModelOverride{
				Enabled: boolPtr(false),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.override.IsDisabled())
		})
	}
}

func TestModelOverride_ApplyToTransformation(t *testing.T) {
	tests := []struct {
		name           string
		override       *ModelOverride
		config         *transformation.Config
		expectedConfig *transformation.Config
	}{
		{
			name:     "nil override does nothing",
			override: nil,
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 100,
					Min: 10,
				},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 100,
					Min: 10,
				},
			},
		},
		{
			name: "override interval max only",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Interval: &IntervalOverride{
						Max: uint64Ptr(200),
					},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 100,
					Min: 10,
				},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 200,
					Min: 10,
				},
			},
		},
		{
			name: "override both interval values",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Interval: &IntervalOverride{
						Max: uint64Ptr(500),
						Min: uint64Ptr(50),
					},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 100,
					Min: 10,
				},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 500,
					Min: 50,
				},
			},
		},
		{
			name: "override schedules",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Schedules: &SchedulesOverride{
						ForwardFill: stringPtr("@every 10m"),
						Backfill:    stringPtr(""),
					},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Schedules: &transformation.SchedulesConfig{
					ForwardFill: "@every 1m",
					Backfill:    "@every 5m",
				},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Schedules: &transformation.SchedulesConfig{
					ForwardFill: "@every 10m",
					Backfill:    "",
				},
			},
		},
		{
			name: "add limits when not present",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Limits: &LimitsOverride{
						Min: uint64Ptr(1000),
						Max: uint64Ptr(2000),
					},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Limits: &transformation.LimitsConfig{
					Min: 1000,
					Max: 2000,
				},
			},
		},
		{
			name: "override existing limits",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Limits: &LimitsOverride{
						Max: uint64Ptr(5000),
					},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Limits: &transformation.LimitsConfig{
					Min: 100,
					Max: 1000,
				},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Limits: &transformation.LimitsConfig{
					Min: 100,
					Max: 5000,
				},
			},
		},
		{
			name: "append tags",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Tags: []string{"staging", "low-priority"},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Tags:     []string{"batch", "daily"},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Tags:     []string{"batch", "daily", "staging", "low-priority"},
			},
		},
		{
			name: "complex override with multiple fields",
			override: &ModelOverride{
				Config: &TransformationOverride{
					Interval: &IntervalOverride{
						Max: uint64Ptr(7200),
					},
					Schedules: &SchedulesOverride{
						ForwardFill: stringPtr("@every 30m"),
					},
					Limits: &LimitsOverride{
						Min: uint64Ptr(5000),
					},
					Tags: []string{"override-tag"},
				},
			},
			config: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 3600,
					Min: 0,
				},
				Schedules: &transformation.SchedulesConfig{
					ForwardFill: "@every 5m",
					Backfill:    "@every 10m",
				},
				Limits: &transformation.LimitsConfig{
					Min: 1000,
					Max: 10000,
				},
				Tags: []string{"original"},
			},
			expectedConfig: &transformation.Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &transformation.IntervalConfig{
					Max: 7200,
					Min: 0,
				},
				Schedules: &transformation.SchedulesConfig{
					ForwardFill: "@every 30m",
					Backfill:    "@every 10m",
				},
				Limits: &transformation.LimitsConfig{
					Min: 5000,
					Max: 10000,
				},
				Tags: []string{"original", "override-tag"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying the original
			configCopy := *tt.config
			if tt.config.Interval != nil {
				intervalCopy := *tt.config.Interval
				configCopy.Interval = &intervalCopy
			}
			if tt.config.Schedules != nil {
				schedulesCopy := *tt.config.Schedules
				configCopy.Schedules = &schedulesCopy
			}
			if tt.config.Limits != nil {
				limitsCopy := *tt.config.Limits
				configCopy.Limits = &limitsCopy
			}
			if tt.config.Tags != nil {
				configCopy.Tags = make([]string, len(tt.config.Tags))
				copy(configCopy.Tags, tt.config.Tags)
			}

			tt.override.ApplyToTransformation(&configCopy)
			assert.Equal(t, tt.expectedConfig, &configCopy)
		})
	}
}

func TestOverrideIntegration(t *testing.T) {
	// Test that overrides work correctly when applied through the service
	config := &Config{
		External: ExternalConfig{
			Paths:           []string{"testdata/external"},
			DefaultDatabase: "ethereum",
		},
		Transformation: TransformationConfig{
			Paths:           []string{"testdata/transformations"},
			DefaultDatabase: "analytics",
		},
		Overrides: map[string]*ModelOverride{
			"analytics.test_table": {
				Config: &TransformationOverride{
					Interval: &IntervalOverride{
						Max: uint64Ptr(7200),
					},
				},
			},
			"analytics.disabled_model": {
				Enabled: boolPtr(false),
			},
		},
	}

	// Validate the config structure
	require.NotNil(t, config.Overrides)
	assert.Len(t, config.Overrides, 2)

	// Check specific override configurations
	testTableOverride := config.Overrides["analytics.test_table"]
	require.NotNil(t, testTableOverride)
	require.NotNil(t, testTableOverride.Config)
	require.NotNil(t, testTableOverride.Config.Interval)
	assert.Equal(t, uint64(7200), *testTableOverride.Config.Interval.Max)

	disabledOverride := config.Overrides["analytics.disabled_model"]
	require.NotNil(t, disabledOverride)
	assert.True(t, disabledOverride.IsDisabled())
}

func TestTableOnlyOverrides(t *testing.T) {
	// Test that table-only overrides work when models use default database
	tests := []struct {
		name                string
		defaultDatabase     string
		overrides           map[string]*ModelOverride
		modelFullID         string
		modelTable          string
		expectOverrideFound bool
		expectDisabled      bool
	}{
		{
			name:            "table-only override matches model with default database",
			defaultDatabase: "analytics",
			overrides: map[string]*ModelOverride{
				"hourly_stats": {
					Enabled: boolPtr(false),
				},
			},
			modelFullID:         "analytics.hourly_stats",
			modelTable:          "hourly_stats",
			expectOverrideFound: true,
			expectDisabled:      true,
		},
		{
			name:            "full ID override still works",
			defaultDatabase: "analytics",
			overrides: map[string]*ModelOverride{
				"analytics.hourly_stats": {
					Enabled: boolPtr(false),
				},
			},
			modelFullID:         "analytics.hourly_stats",
			modelTable:          "hourly_stats",
			expectOverrideFound: true,
			expectDisabled:      true,
		},
		{
			name:            "table-only override doesn't match model with different database",
			defaultDatabase: "analytics",
			overrides: map[string]*ModelOverride{
				"hourly_stats": {
					Enabled: boolPtr(false),
				},
			},
			modelFullID:         "custom.hourly_stats",
			modelTable:          "hourly_stats",
			expectOverrideFound: false,
			expectDisabled:      false,
		},
		{
			name:            "prefer full ID override over table-only",
			defaultDatabase: "analytics",
			overrides: map[string]*ModelOverride{
				"analytics.hourly_stats": {
					Enabled: boolPtr(false),
				},
				"hourly_stats": {
					Enabled: boolPtr(true),
				},
			},
			modelFullID:         "analytics.hourly_stats",
			modelTable:          "hourly_stats",
			expectOverrideFound: true,
			expectDisabled:      true, // Should use the full ID override (disabled)
		},
		{
			name:            "no override found",
			defaultDatabase: "analytics",
			overrides: map[string]*ModelOverride{
				"other_table": {
					Enabled: boolPtr(false),
				},
			},
			modelFullID:         "analytics.hourly_stats",
			modelTable:          "hourly_stats",
			expectOverrideFound: false,
			expectDisabled:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal service with the config
			s := &service{
				config: &Config{
					Transformation: TransformationConfig{
						DefaultDatabase: tt.defaultDatabase,
					},
					Overrides: tt.overrides,
				},
			}

			// Test the findOverride method
			override, overrideKey := s.findOverride(tt.modelFullID, tt.modelTable)

			if tt.expectOverrideFound {
				assert.NotNil(t, override, "Expected to find override")
				assert.NotEmpty(t, overrideKey, "Expected override key")

				if tt.expectDisabled {
					assert.True(t, override.IsDisabled(), "Expected model to be disabled")
				} else {
					assert.False(t, override.IsDisabled(), "Expected model to be enabled")
				}
			} else {
				assert.Nil(t, override, "Expected no override")
				assert.Empty(t, overrideKey, "Expected no override key")
			}
		})
	}
}

// Helper functions for creating pointers
func boolPtr(b bool) *bool {
	return &b
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

func stringPtr(s string) *string {
	return &s
}
