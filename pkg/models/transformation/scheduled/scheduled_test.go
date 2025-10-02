package scheduled

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestNewHandler(t *testing.T) {
	tests := []struct {
		name       string
		yamlData   string
		adminTable transformation.AdminTable
		wantErr    bool
		errMsg     string
	}{
		{
			name: "valid scheduled config",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "*/5 * * * *"  # Every 5 minutes
tags:
  - scheduled-task
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_scheduled",
			},
			wantErr: false,
		},
		{
			name: "scheduled config with dependencies should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
dependencies:  # Not allowed for scheduled type
  - source.table1
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_scheduled",
			},
			wantErr: true,
			errMsg:  "field dependencies not found",
		},
		{
			name: "scheduled config with interval should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
interval:  # Not allowed for scheduled type
  min: 100
  max: 1000
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_scheduled",
			},
			wantErr: true,
			errMsg:  "field interval not found",
		},
		{
			name: "scheduled config with schedules should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
schedules:  # Not allowed - use schedule instead
  forwardfill: "@every 10s"
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_scheduled",
			},
			wantErr: true,
			errMsg:  "field schedules not found",
		},
		{
			name: "valid scheduled config with exec field",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "0 0 * * *"  # Daily at midnight
exec: INSERT INTO test_table SELECT NOW()
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_scheduled",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewHandler([]byte(tt.yamlData), tt.adminTable)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, handler)
			assert.Equal(t, tt.adminTable, handler.adminTable)
			assert.NotNil(t, handler.config)
		})
	}
}

func TestHandler_Type(t *testing.T) {
	handler := &Handler{}
	assert.Equal(t, transformation.TypeScheduled, handler.Type())
}

func TestHandler_ShouldTrackPosition(t *testing.T) {
	handler := &Handler{}
	assert.False(t, handler.ShouldTrackPosition())
}

func TestHandler_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errType error
	}{
		{
			name: "valid config with standard cron",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedule: "*/5 * * * *", // Every 5 minutes
			},
			wantErr: false,
		},
		{
			name: "valid config with @every syntax",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedule: "@every 30s",
			},
			wantErr: false,
		},
		{
			name: "valid config with @hourly syntax",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedule: "@hourly",
			},
			wantErr: false,
		},
		{
			name: "missing database",
			config: &Config{
				Table:    "test_table",
				Schedule: "@every 1m",
			},
			wantErr: true,
			errType: transformation.ErrDatabaseRequired,
		},
		{
			name: "missing table",
			config: &Config{
				Database: "test_db",
				Schedule: "@every 1m",
			},
			wantErr: true,
			errType: transformation.ErrTableRequired,
		},
		{
			name: "missing schedule",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
			},
			wantErr: true,
			errType: ErrScheduleRequired,
		},
		{
			name: "invalid cron expression",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedule: "invalid cron",
			},
			wantErr: true,
		},
		{
			name: "invalid cron with too many fields",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Schedule: "* * * * * * *", // Too many fields
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{config: tt.config}
			err := handler.Validate()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHandler_GetTemplateVariables(t *testing.T) {
	handler := &Handler{}
	taskInfo := transformation.TaskInfo{
		Position:  1000, // Should be ignored for scheduled
		Interval:  500,  // Should be ignored for scheduled
		Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		Direction: "forward",
	}

	vars := handler.GetTemplateVariables(context.Background(), taskInfo)

	// Check execution
	execution, ok := vars["execution"].(map[string]any)
	require.True(t, ok, "execution should be present")
	assert.Equal(t, int64(1704110400), execution["timestamp"]) // Unix timestamp
	assert.Equal(t, "2024-01-01T12:00:00Z", execution["datetime"])

	// Check task
	task, ok := vars["task"].(map[string]any)
	require.True(t, ok, "task should be present")
	assert.Equal(t, "forward", task["direction"])

	// Should NOT have bounds (that's for incremental)
	_, hasBounds := vars["bounds"]
	assert.False(t, hasBounds, "scheduled should not have bounds")
}

func TestHandler_GetSchedule(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		expected string
	}{
		{
			name:     "standard cron expression",
			schedule: "*/5 * * * *",
			expected: "*/5 * * * *",
		},
		{
			name:     "@every syntax",
			schedule: "@every 30s",
			expected: "@every 30s",
		},
		{
			name:     "@hourly syntax",
			schedule: "@hourly",
			expected: "@hourly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{
				config: &Config{
					Schedule: tt.schedule,
				},
			}
			assert.Equal(t, tt.expected, handler.GetSchedule())
		})
	}
}

func TestHandler_RecordCompletion(t *testing.T) {
	tests := []struct {
		name         string
		adminService interface{}
		modelID      string
		taskInfo     transformation.TaskInfo
		wantErr      bool
		errType      error
	}{
		{
			name: "successful record completion",
			adminService: &mockScheduledAdminService{
				recordFunc: func(_ context.Context, modelID string, startDateTime time.Time) error {
					assert.Equal(t, "test.model", modelID)
					expectedTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
					assert.Equal(t, expectedTime, startDateTime)
					return nil
				},
			},
			modelID: "test.model",
			taskInfo: transformation.TaskInfo{
				Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name:         "invalid admin service",
			adminService: struct{}{}, // Does not implement the interface
			modelID:      "test.model",
			taskInfo: transformation.TaskInfo{
				Timestamp: time.Now(),
			},
			wantErr: true,
			errType: ErrAdminServiceInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{}
			err := handler.RecordCompletion(context.Background(), tt.adminService, tt.modelID, tt.taskInfo)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateScheduleFormat(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		wantErr  bool
	}{
		{
			name:     "valid standard cron - every minute",
			schedule: "* * * * *",
			wantErr:  false,
		},
		{
			name:     "valid standard cron - every 5 minutes",
			schedule: "*/5 * * * *",
			wantErr:  false,
		},
		{
			name:     "valid standard cron - daily at midnight",
			schedule: "0 0 * * *",
			wantErr:  false,
		},
		{
			name:     "valid @every syntax",
			schedule: "@every 1h30m",
			wantErr:  false,
		},
		{
			name:     "valid @hourly",
			schedule: "@hourly",
			wantErr:  false,
		},
		{
			name:     "valid @daily",
			schedule: "@daily",
			wantErr:  false,
		},
		{
			name:     "valid @weekly",
			schedule: "@weekly",
			wantErr:  false,
		},
		{
			name:     "valid @monthly",
			schedule: "@monthly",
			wantErr:  false,
		},
		{
			name:     "valid @yearly",
			schedule: "@yearly",
			wantErr:  false,
		},
		{
			name:     "invalid - empty string",
			schedule: "",
			wantErr:  true,
		},
		{
			name:     "invalid - random text",
			schedule: "not a cron",
			wantErr:  true,
		},
		{
			name:     "invalid - too many fields",
			schedule: "* * * * * *",
			wantErr:  true,
		},
		{
			name:     "invalid - too few fields",
			schedule: "* * *",
			wantErr:  true,
		},
		{
			name:     "invalid @every syntax",
			schedule: "@every invalid",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateScheduleFormat(tt.schedule)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStrictYAMLUnmarshaling(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		wantErr  bool
		errMsg   string
	}{
		{
			name: "valid scheduled config",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
tags:
  - tag1
  - tag2
`,
			wantErr: false,
		},
		{
			name: "scheduled config with dependencies should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
dependencies:  # This belongs to incremental type
  - source.table
`,
			wantErr: true,
			errMsg:  "field dependencies not found",
		},
		{
			name: "scheduled config with interval should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
interval:  # This belongs to incremental type
  min: 100
  max: 1000
`,
			wantErr: true,
			errMsg:  "field interval not found",
		},
		{
			name: "scheduled config with schedules should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
schedules:  # This belongs to incremental type
  forwardfill: "@every 10s"
  backfill: "@every 30s"
`,
			wantErr: true,
			errMsg:  "field schedules not found",
		},
		{
			name: "scheduled config with unknown field should fail",
			yamlData: `
type: scheduled
database: test_db
table: test_table
schedule: "@every 1m"
unknown_field: value
`,
			wantErr: true,
			errMsg:  "field unknown_field not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config Config
			decoder := yaml.NewDecoder(bytes.NewReader([]byte(tt.yamlData)))
			decoder.KnownFields(true)
			err := decoder.Decode(&config)

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

// Mock scheduled admin service for testing
type mockScheduledAdminService struct {
	recordFunc func(ctx context.Context, modelID string, startDateTime time.Time) error
}

func (m *mockScheduledAdminService) RecordScheduledCompletion(ctx context.Context, modelID string, startDateTime time.Time) error {
	if m.recordFunc != nil {
		return m.recordFunc(ctx, modelID, startDateTime)
	}
	return nil
}
