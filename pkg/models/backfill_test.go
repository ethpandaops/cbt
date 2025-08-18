package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBackfillConfig_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected *BackfillConfig
		wantErr  bool
	}{
		{
			name: "backfill with enabled and schedule",
			yaml: `
database: test
table: test_table
partition: timestamp
interval: 3600
schedule: "@every 1m"
backfill:
  enabled: true
  schedule: "@every 5m"
`,
			expected: &BackfillConfig{
				Enabled:  true,
				Schedule: "@every 5m",
			},
		},
		{
			name: "backfill with minimum position",
			yaml: `
database: test
table: test_table
partition: timestamp
interval: 3600
schedule: "@every 1m"
backfill:
  enabled: true
  schedule: "@every 5m"
  minimum: 1704067200
`,
			expected: &BackfillConfig{
				Enabled:  true,
				Schedule: "@every 5m",
				Minimum:  1704067200,
			},
		},
		{
			name: "backfill disabled",
			yaml: `
database: test
table: test_table
partition: timestamp
interval: 3600
schedule: "@every 1m"
backfill:
  enabled: false
`,
			expected: &BackfillConfig{
				Enabled: false,
			},
		},
		{
			name: "no backfill field",
			yaml: `
database: test
table: test_table
partition: timestamp
interval: 3600
schedule: "@every 1m"
`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config ModelConfig
			err := yaml.Unmarshal([]byte(tt.yaml), &config)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.expected == nil {
				assert.Nil(t, config.Backfill)
			} else {
				require.NotNil(t, config.Backfill)
				assert.Equal(t, tt.expected.Enabled, config.Backfill.Enabled)
				assert.Equal(t, tt.expected.Schedule, config.Backfill.Schedule)
				assert.Equal(t, tt.expected.Minimum, config.Backfill.Minimum)
			}
		})
	}
}

func TestBackfillConfig_Validation(t *testing.T) {
	parser := &ModelParser{}

	tests := []struct {
		name    string
		config  *BackfillConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil config is valid",
			config:  nil,
			wantErr: false,
		},
		{
			name: "disabled config is valid",
			config: &BackfillConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "enabled with valid schedule",
			config: &BackfillConfig{
				Enabled:  true,
				Schedule: "@every 5m",
			},
			wantErr: false,
		},
		{
			name: "enabled without schedule",
			config: &BackfillConfig{
				Enabled: true,
			},
			wantErr: true,
			errMsg:  "backfill.schedule is required",
		},
		{
			name: "enabled with invalid @every format",
			config: &BackfillConfig{
				Enabled:  true,
				Schedule: "@every invalid",
			},
			wantErr: true,
			errMsg:  "invalid @every duration",
		},
		{
			name: "enabled with valid cron format",
			config: &BackfillConfig{
				Enabled:  true,
				Schedule: "0 */5 * * *",
			},
			wantErr: false,
		},
		{
			name: "enabled with invalid cron format",
			config: &BackfillConfig{
				Enabled:  true,
				Schedule: "invalid cron",
			},
			wantErr: true,
			errMsg:  "cron expression should have 5 or 6 fields",
		},
		{
			name: "enabled with @hourly",
			config: &BackfillConfig{
				Enabled:  true,
				Schedule: "@hourly",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parser.validateBackfillConfig(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
