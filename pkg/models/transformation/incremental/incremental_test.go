package incremental

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
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
			name: "valid incremental config",
			yamlData: `
type: incremental
database: test_db
table: test_table
interval:
  min: 100
  max: 1000
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
dependencies:
  - source.table1
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_incremental",
			},
			wantErr: false,
		},
		{
			name: "config with unknown fields should fail due to strict unmarshaling",
			yamlData: `
type: incremental
database: test_db
table: test_table
interval:
  min: 100
  max: 1000
schedule: "@every 1m"  # This field is for scheduled type, not incremental
schedules:
  forwardfill: "@every 10s"
dependencies:
  - source.table1
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_incremental",
			},
			wantErr: true,
			errMsg:  "field schedule not found",
		},
		{
			name: "config with dependencies field (valid for incremental)",
			yamlData: `
type: incremental
database: test_db
table: test_table
interval:
  min: 100
  max: 1000
schedules:
  forwardfill: "@every 10s"
dependencies:
  - source.table1
  - source.table2
`,
			adminTable: transformation.AdminTable{
				Database: "admin",
				Table:    "cbt_incremental",
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
	assert.Equal(t, transformation.TypeIncremental, handler.Type())
}

func TestHandler_ShouldTrackPosition(t *testing.T) {
	handler := &Handler{}
	assert.True(t, handler.ShouldTrackPosition())
}

func TestHandler_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errType error
	}{
		{
			name: "valid config",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Min:  100,
					Max:  1000,
					Type: "second",
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "@every 10s",
					Backfill:    "@every 30s",
				},
				Dependencies: []transformation.Dependency{
					{SingleDep: "source.table1"},
				},
			},
			wantErr: false,
		},
		{
			name: "missing database",
			config: &Config{
				Table: "test_table",
				Interval: &IntervalConfig{
					Min:  100,
					Max:  1000,
					Type: "second",
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "@every 10s",
				},
				Dependencies: []transformation.Dependency{
					{SingleDep: "source.table1"},
				},
			},
			wantErr: true,
			errType: transformation.ErrDatabaseRequired,
		},
		{
			name: "missing table",
			config: &Config{
				Database: "test_db",
				Interval: &IntervalConfig{
					Min:  100,
					Max:  1000,
					Type: "second",
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "@every 10s",
				},
				Dependencies: []transformation.Dependency{
					{SingleDep: "source.table1"},
				},
			},
			wantErr: true,
			errType: transformation.ErrTableRequired,
		},
		{
			name: "missing schedules",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Min:  100,
					Max:  1000,
					Type: "second",
				},
				Dependencies: []transformation.Dependency{
					{SingleDep: "source.table1"},
				},
			},
			wantErr: true,
			errType: ErrNoSchedulesConfig,
		},
		{
			name: "missing dependencies",
			config: &Config{
				Database: "test_db",
				Table:    "test_table",
				Interval: &IntervalConfig{
					Min:  100,
					Max:  1000,
					Type: "second",
				},
				Schedules: &SchedulesConfig{
					ForwardFill: "@every 10s",
				},
			},
			wantErr: true,
			errType: ErrDependenciesRequired,
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
		Position:  1000,
		Interval:  500,
		Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		Direction: "forward",
	}

	vars := handler.GetTemplateVariables(context.Background(), taskInfo)

	// Check bounds
	bounds, ok := vars["bounds"].(map[string]any)
	require.True(t, ok, "bounds should be present")
	assert.Equal(t, uint64(1000), bounds["start"])
	assert.Equal(t, uint64(1500), bounds["end"]) // start + interval

	// Check task
	task, ok := vars["task"].(map[string]any)
	require.True(t, ok, "task should be present")
	assert.Equal(t, int64(1704110400), task["start"]) // Unix timestamp
	assert.Equal(t, "forward", task["direction"])
}

func TestHandler_SubstituteDependencyPlaceholders(t *testing.T) {
	tests := []struct {
		name           string
		dependencies   []transformation.Dependency
		externalDB     string
		transformDB    string
		expectedResult []transformation.Dependency
	}{
		{
			name: "substitute external placeholder",
			dependencies: []transformation.Dependency{
				{SingleDep: "{{external}}.source_table"},
				{SingleDep: "other_db.table"},
			},
			externalDB:  "external_db",
			transformDB: "transform_db",
			expectedResult: []transformation.Dependency{
				{SingleDep: "external_db.source_table"},
				{SingleDep: "other_db.table"},
			},
		},
		{
			name: "substitute transformation placeholder",
			dependencies: []transformation.Dependency{
				{SingleDep: "{{transformation}}.derived_table"},
			},
			externalDB:  "external_db",
			transformDB: "transform_db",
			expectedResult: []transformation.Dependency{
				{SingleDep: "transform_db.derived_table"},
			},
		},
		{
			name: "substitute both placeholders",
			dependencies: []transformation.Dependency{
				{SingleDep: "{{external}}.source"},
				{SingleDep: "{{transformation}}.derived"},
			},
			externalDB:  "ext_db",
			transformDB: "trans_db",
			expectedResult: []transformation.Dependency{
				{SingleDep: "ext_db.source"},
				{SingleDep: "trans_db.derived"},
			},
		},
		{
			name: "group dependencies with placeholders",
			dependencies: []transformation.Dependency{
				{
					IsGroup: true,
					GroupDeps: []string{
						"{{external}}.table1",
						"{{external}}.table2",
						"static.table3",
					},
				},
			},
			externalDB:  "external_db",
			transformDB: "",
			expectedResult: []transformation.Dependency{
				{
					IsGroup: true,
					GroupDeps: []string{
						"external_db.table1",
						"external_db.table2",
						"static.table3",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{
				config: &Config{
					Dependencies: tt.dependencies,
				},
			}

			handler.SubstituteDependencyPlaceholders(tt.externalDB, tt.transformDB)

			// Check that dependencies were substituted correctly
			assert.Equal(t, tt.expectedResult, handler.config.Dependencies)

			// Check that original dependencies were preserved
			assert.NotNil(t, handler.config.OriginalDependencies)
			assert.NotEqual(t, handler.config.Dependencies, handler.config.OriginalDependencies)
		})
	}
}

func TestHandler_GetFlattenedDependencies(t *testing.T) {
	tests := []struct {
		name         string
		dependencies []transformation.Dependency
		expected     []string
	}{
		{
			name: "single dependencies",
			dependencies: []transformation.Dependency{
				{SingleDep: "db1.table1"},
				{SingleDep: "db2.table2"},
			},
			expected: []string{"db1.table1", "db2.table2"},
		},
		{
			name: "group dependencies",
			dependencies: []transformation.Dependency{
				{
					IsGroup:   true,
					GroupDeps: []string{"db1.table1", "db1.table2"},
				},
			},
			expected: []string{"db1.table1", "db1.table2"},
		},
		{
			name: "mixed single and group dependencies",
			dependencies: []transformation.Dependency{
				{SingleDep: "db1.table1"},
				{
					IsGroup:   true,
					GroupDeps: []string{"db2.table2", "db2.table3"},
				},
				{SingleDep: "db3.table4"},
			},
			expected: []string{"db1.table1", "db2.table2", "db2.table3", "db3.table4"},
		},
		{
			name:         "empty dependencies",
			dependencies: []transformation.Dependency{},
			expected:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{
				config: &Config{
					Dependencies: tt.dependencies,
				},
			}

			result := handler.GetFlattenedDependencies()
			assert.Equal(t, tt.expected, result)
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
			adminService: &mockAdminService{
				recordCompletionFunc: func(_ context.Context, modelID string, position, interval uint64) error {
					assert.Equal(t, "test.model", modelID)
					assert.Equal(t, uint64(1000), position)
					assert.Equal(t, uint64(500), interval)
					return nil
				},
			},
			modelID: "test.model",
			taskInfo: transformation.TaskInfo{
				Position: 1000,
				Interval: 500,
			},
			wantErr: false,
		},
		{
			name:         "invalid admin service",
			adminService: struct{}{}, // Does not implement the interface
			modelID:      "test.model",
			taskInfo: transformation.TaskInfo{
				Position: 1000,
				Interval: 500,
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

func TestStrictYAMLUnmarshaling(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		wantErr  bool
		errMsg   string
	}{
		{
			name: "valid incremental config",
			yamlData: `
type: incremental
database: test_db
table: test_table
interval:
  min: 100
  max: 1000
schedules:
  forwardfill: "@every 10s"
dependencies:
  - source.table
`,
			wantErr: false,
		},
		{
			name: "incremental config with scheduled field should fail",
			yamlData: `
type: incremental
database: test_db
table: test_table
schedule: "@every 1m"  # This belongs to scheduled type
interval:
  min: 100
  max: 1000
schedules:
  forwardfill: "@every 10s"
dependencies:
  - source.table
`,
			wantErr: true,
			errMsg:  "field schedule not found",
		},
		{
			name: "incremental config with unknown field should fail",
			yamlData: `
type: incremental
database: test_db
table: test_table
unknown_field: value
interval:
  min: 100
  max: 1000
schedules:
  forwardfill: "@every 10s"
dependencies:
  - source.table
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

// Mock admin service for testing
type mockAdminService struct {
	recordCompletionFunc func(ctx context.Context, modelID string, position, interval uint64) error
}

func (m *mockAdminService) RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error {
	if m.recordCompletionFunc != nil {
		return m.recordCompletionFunc(ctx, modelID, position, interval)
	}
	return nil
}

func (m *mockAdminService) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}
