package models

import (
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestModelOverride_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantErr   bool
		checkFunc func(t *testing.T, m *ModelOverride)
	}{
		{
			name: "override with enabled flag only",
			yaml: `
enabled: false
`,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				require.NotNil(t, m.Enabled)
				assert.False(t, *m.Enabled)
				assert.Nil(t, m.rawConfig)
			},
		},
		{
			name: "override with config for transformation",
			yaml: `
enabled: true
config:
  interval:
    min: 100
    max: 1000
  schedules:
    forwardfill: "@every 10s"
    backfill: "@every 30s"
`,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				require.NotNil(t, m.Enabled)
				assert.True(t, *m.Enabled)
				assert.NotNil(t, m.rawConfig)
			},
		},
		{
			name: "override with config for external",
			yaml: `
enabled: true
config:
  interval: 500
  updateInterval: 10m
  maxSecondsOverdue: 300
`,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				require.NotNil(t, m.Enabled)
				assert.True(t, *m.Enabled)
				assert.NotNil(t, m.rawConfig)
			},
		},
		{
			name: "override without enabled flag",
			yaml: `
config:
  interval:
    min: 50
`,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				assert.Nil(t, m.Enabled)
				assert.NotNil(t, m.rawConfig)
			},
		},
		{
			name: "empty override",
			yaml: `{}`,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				assert.Nil(t, m.Enabled)
				assert.Nil(t, m.rawConfig)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m ModelOverride
			err := yaml.Unmarshal([]byte(tt.yaml), &m)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			tt.checkFunc(t, &m)
		})
	}
}

func TestModelOverride_ResolveConfig(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		modelType ModelType
		wantErr   bool
		checkFunc func(t *testing.T, m *ModelOverride)
	}{
		{
			name: "resolve transformation config",
			yaml: `
enabled: true
config:
  interval:
    min: 100
    max: 1000
  schedules:
    forwardfill: "@every 10s"
    backfill: ""
  limits:
    min: 1000
    max: 2000
  tags:
    - override-tag1
    - override-tag2
`,
			modelType: ModelTypeTransformation,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				require.NotNil(t, m.Config)

				config, ok := m.Config.(*TransformationOverride)
				require.True(t, ok, "expected TransformationOverride type")

				// Check interval
				require.NotNil(t, config.Interval)
				require.NotNil(t, config.Interval.Min)
				assert.Equal(t, uint64(100), *config.Interval.Min)
				require.NotNil(t, config.Interval.Max)
				assert.Equal(t, uint64(1000), *config.Interval.Max)

				// Check schedules
				require.NotNil(t, config.Schedules)
				require.NotNil(t, config.Schedules.ForwardFill)
				assert.Equal(t, "@every 10s", *config.Schedules.ForwardFill)
				require.NotNil(t, config.Schedules.Backfill)
				assert.Equal(t, "", *config.Schedules.Backfill) // Empty string means disable

				// Check limits
				require.NotNil(t, config.Limits)
				require.NotNil(t, config.Limits.Min)
				assert.Equal(t, uint64(1000), *config.Limits.Min)
				require.NotNil(t, config.Limits.Max)
				assert.Equal(t, uint64(2000), *config.Limits.Max)

				// Check tags
				assert.Equal(t, []string{"override-tag1", "override-tag2"}, config.Tags)
			},
		},
		{
			name: "resolve external config",
			yaml: `
config:
  lag: 250
  cache:
    incremental_scan_interval: 5m
    full_scan_interval: 30m
`,
			modelType: ModelTypeExternal,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				require.NotNil(t, m.Config)

				config, ok := m.Config.(*ExternalOverride)
				require.True(t, ok, "expected ExternalOverride type")

				require.NotNil(t, config.Lag)
				assert.Equal(t, uint64(250), *config.Lag)

				require.NotNil(t, config.Cache)
				require.NotNil(t, config.Cache.IncrementalScanInterval)
				assert.Equal(t, 5*time.Minute, *config.Cache.IncrementalScanInterval)
				require.NotNil(t, config.Cache.FullScanInterval)
				assert.Equal(t, 30*time.Minute, *config.Cache.FullScanInterval)
			},
		},
		{
			name: "resolve with no config",
			yaml: `
enabled: false
`,
			modelType: ModelTypeTransformation,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				assert.Nil(t, m.Config)
			},
		},
		{
			name: "resolve with unknown model type",
			yaml: `
config:
  interval: 100
`,
			modelType: ModelType("unknown"),
			wantErr:   true,
		},
		{
			name: "transformation config with partial override",
			yaml: `
config:
  schedules:
    forwardfill: "@every 1m"
`,
			modelType: ModelTypeTransformation,
			checkFunc: func(t *testing.T, m *ModelOverride) {
				require.NotNil(t, m.Config)

				config, ok := m.Config.(*TransformationOverride)
				require.True(t, ok)

				// Only schedules should be set
				assert.Nil(t, config.Interval)
				assert.Nil(t, config.Limits)
				require.NotNil(t, config.Schedules)
				require.NotNil(t, config.Schedules.ForwardFill)
				assert.Equal(t, "@every 1m", *config.Schedules.ForwardFill)
				assert.Nil(t, config.Schedules.Backfill) // Not set, so nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m ModelOverride
			err := yaml.Unmarshal([]byte(tt.yaml), &m)
			require.NoError(t, err)

			err = m.ResolveConfig(tt.modelType)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			tt.checkFunc(t, &m)
		})
	}
}

func TestModelOverride_IsDisabled(t *testing.T) {
	tests := []struct {
		name     string
		override *ModelOverride
		want     bool
	}{
		{
			name:     "nil override",
			override: nil,
			want:     false,
		},
		{
			name:     "enabled not set",
			override: &ModelOverride{},
			want:     false,
		},
		{
			name: "explicitly enabled",
			override: &ModelOverride{
				Enabled: ptrBool(true),
			},
			want: false,
		},
		{
			name: "explicitly disabled",
			override: &ModelOverride{
				Enabled: ptrBool(false),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.override.IsDisabled())
		})
	}
}

func TestExternalOverride_ApplyToExternal(t *testing.T) {
	tests := []struct {
		name      string
		override  *ExternalOverride
		initial   *external.Config
		checkFunc func(t *testing.T, config *external.Config)
	}{
		{
			name: "apply all overrides",
			override: &ExternalOverride{
				Lag: ptrUint64(500),
				Cache: &CacheOverride{
					IncrementalScanInterval: ptrDuration(10 * time.Minute),
					FullScanInterval:        ptrDuration(1 * time.Hour),
				},
			},
			initial: &external.Config{
				Database: "test_db",
				Table:    "test_table",
				Lag:      100,
				Cache: &external.CacheConfig{
					IncrementalScanInterval: 5 * time.Minute,
					FullScanInterval:        30 * time.Minute,
				},
			},
			checkFunc: func(t *testing.T, config *external.Config) {
				assert.Equal(t, uint64(500), config.Lag)
				assert.Equal(t, 10*time.Minute, config.Cache.IncrementalScanInterval)
				assert.Equal(t, 1*time.Hour, config.Cache.FullScanInterval)
				// Unchanged fields
				assert.Equal(t, "test_db", config.Database)
				assert.Equal(t, "test_table", config.Table)
			},
		},
		{
			name: "apply partial overrides - lag only",
			override: &ExternalOverride{
				Lag: ptrUint64(250),
			},
			initial: &external.Config{
				Database: "test_db",
				Table:    "test_table",
				Lag:      100,
				Cache: &external.CacheConfig{
					IncrementalScanInterval: 5 * time.Minute,
					FullScanInterval:        30 * time.Minute,
				},
			},
			checkFunc: func(t *testing.T, config *external.Config) {
				assert.Equal(t, uint64(250), config.Lag)
				// Unchanged fields
				assert.Equal(t, 5*time.Minute, config.Cache.IncrementalScanInterval)
				assert.Equal(t, 30*time.Minute, config.Cache.FullScanInterval)
			},
		},
		{
			name: "apply cache overrides only",
			override: &ExternalOverride{
				Cache: &CacheOverride{
					IncrementalScanInterval: ptrDuration(2 * time.Minute),
				},
			},
			initial: &external.Config{
				Database: "test_db",
				Table:    "test_table",
				Lag:      100,
				Cache: &external.CacheConfig{
					IncrementalScanInterval: 5 * time.Minute,
					FullScanInterval:        30 * time.Minute,
				},
			},
			checkFunc: func(t *testing.T, config *external.Config) {
				assert.Equal(t, uint64(100), config.Lag) // Unchanged
				assert.Equal(t, 2*time.Minute, config.Cache.IncrementalScanInterval)
				assert.Equal(t, 30*time.Minute, config.Cache.FullScanInterval) // Unchanged
			},
		},
		{
			name:     "nil override does nothing",
			override: nil,
			initial: &external.Config{
				Database: "test_db",
				Table:    "test_table",
				Lag:      100,
			},
			checkFunc: func(t *testing.T, config *external.Config) {
				assert.Equal(t, uint64(100), config.Lag)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.initial
			if tt.override != nil {
				tt.override.applyToExternal(config)
			}
			tt.checkFunc(t, config)
		})
	}
}

func TestSchedulesOverride_Behavior(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		checkFunc func(t *testing.T, s *SchedulesOverride)
	}{
		{
			name: "nil means keep existing",
			yaml: `{}`,
			checkFunc: func(t *testing.T, s *SchedulesOverride) {
				assert.Nil(t, s.ForwardFill)
				assert.Nil(t, s.Backfill)
			},
		},
		{
			name: "empty string means disable",
			yaml: `
forwardfill: ""
backfill: ""
`,
			checkFunc: func(t *testing.T, s *SchedulesOverride) {
				require.NotNil(t, s.ForwardFill)
				assert.Equal(t, "", *s.ForwardFill)
				require.NotNil(t, s.Backfill)
				assert.Equal(t, "", *s.Backfill)
			},
		},
		{
			name: "non-empty means new schedule",
			yaml: `
forwardfill: "@every 30s"
backfill: "@every 1m"
`,
			checkFunc: func(t *testing.T, s *SchedulesOverride) {
				require.NotNil(t, s.ForwardFill)
				assert.Equal(t, "@every 30s", *s.ForwardFill)
				require.NotNil(t, s.Backfill)
				assert.Equal(t, "@every 1m", *s.Backfill)
			},
		},
		{
			name: "mixed: one disabled, one new",
			yaml: `
forwardfill: ""
backfill: "@every 2m"
`,
			checkFunc: func(t *testing.T, s *SchedulesOverride) {
				require.NotNil(t, s.ForwardFill)
				assert.Equal(t, "", *s.ForwardFill) // Disabled
				require.NotNil(t, s.Backfill)
				assert.Equal(t, "@every 2m", *s.Backfill) // New schedule
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s SchedulesOverride
			err := yaml.Unmarshal([]byte(tt.yaml), &s)
			require.NoError(t, err)
			tt.checkFunc(t, &s)
		})
	}
}

// Helper functions
func ptrBool(b bool) *bool {
	return &b
}

func ptrUint64(u uint64) *uint64 {
	return &u
}

func ptrDuration(d time.Duration) *time.Duration {
	return &d
}
