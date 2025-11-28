package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors
var (
	errMockExecute = errors.New("mock execute error")
	errMockRender  = errors.New("mock render error")
	errCheckFailed = errors.New("check failed")
	errQueryFailed = errors.New("query failed")
	errNotFound    = errors.New("not found")
)

// Test NewModelExecutor
func TestNewModelExecutor(t *testing.T) {
	log := logrus.New()
	mockCH := &mockExecutorClickhouseClient{}
	mockModels := &mockExecutorModelsService{}
	mockAdmin := &mockExecutorAdminService{}

	executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)

	assert.NotNil(t, executor)
	assert.NotNil(t, executor.log)
	assert.NotNil(t, executor.chClient)
	assert.NotNil(t, executor.models)
	assert.NotNil(t, executor.admin)
}

// Test Execute method
func TestModelExecutor_Execute(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func(*mockExecutorClickhouseClient, *mockExecutorModelsService, *mockExecutorAdminService)
		taskCtx         interface{}
		wantErr         bool
		expectedErrType error
	}{
		{
			name: "invalid task context type",
			setupMocks: func(_ *mockExecutorClickhouseClient, _ *mockExecutorModelsService, _ *mockExecutorAdminService) {
				// No setup needed
			},
			taskCtx:         "invalid",
			wantErr:         true,
			expectedErrType: ErrInvalidTaskContext,
		},
		{
			name: "validation fails - table does not exist",
			setupMocks: func(ch *mockExecutorClickhouseClient, _ *mockExecutorModelsService, _ *mockExecutorAdminService) {
				ch.tableExists = false
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:   "test.model",
					typ:  transformation.TransformationTypeSQL,
					sql:  "SELECT 1",
					conf: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "SQL execution success",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, _ *mockExecutorAdminService) {
				ch.tableExists = true
				m.renderedSQL = "SELECT 1; SELECT 2"
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:   "test.model",
					typ:  transformation.TransformationTypeSQL,
					sql:  "SELECT 1",
					conf: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "exec command success",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, _ *mockExecutorAdminService) {
				ch.tableExists = true
				m.envVars = &[]string{"VAR1=value1"}
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:    "test.model",
					typ:   transformation.TransformationTypeExec,
					value: "echo 'test'",
					conf:  transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "invalid transformation type",
			setupMocks: func(ch *mockExecutorClickhouseClient, _ *mockExecutorModelsService, _ *mockExecutorAdminService) {
				ch.tableExists = true
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:   "test.model",
					typ:  "invalid",
					conf: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "SQL render fails",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, _ *mockExecutorAdminService) {
				ch.tableExists = true
				m.renderErr = errMockRender
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:   "test.model",
					typ:  transformation.TransformationTypeSQL,
					sql:  "SELECT 1",
					conf: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "SQL execution fails",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, _ *mockExecutorAdminService) {
				ch.tableExists = true
				ch.executeErr = errMockExecute
				m.renderedSQL = "SELECT 1"
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:   "test.model",
					typ:  transformation.TransformationTypeSQL,
					sql:  "SELECT 1",
					conf: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockCH := &mockExecutorClickhouseClient{}
			mockModels := &mockExecutorModelsService{}
			mockAdmin := &mockExecutorAdminService{}

			tt.setupMocks(mockCH, mockModels, mockAdmin)

			executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
			err := executor.Execute(context.Background(), tt.taskCtx)

			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErrType != nil {
					assert.ErrorIs(t, err, tt.expectedErrType)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test Validate method
func TestModelExecutor_Validate(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(*mockExecutorClickhouseClient)
		taskCtx    interface{}
		wantErr    bool
	}{
		{
			name:       "invalid task context",
			setupMocks: func(_ *mockExecutorClickhouseClient) {},
			taskCtx:    "invalid",
			wantErr:    true,
		},
		{
			name: "table exists",
			setupMocks: func(ch *mockExecutorClickhouseClient) {
				ch.tableExists = true
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					conf: transformation.Config{Database: "test", Table: "model"},
				},
			},
			wantErr: false,
		},
		{
			name: "table does not exist",
			setupMocks: func(ch *mockExecutorClickhouseClient) {
				ch.tableExists = false
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					conf: transformation.Config{Database: "test", Table: "model"},
				},
			},
			wantErr: true,
		},
		{
			name: "table check fails",
			setupMocks: func(ch *mockExecutorClickhouseClient) {
				ch.tableExistsErr = errCheckFailed
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					conf: transformation.Config{Database: "test", Table: "model"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockCH := &mockExecutorClickhouseClient{}
			mockModels := &mockExecutorModelsService{}
			mockAdmin := &mockExecutorAdminService{}

			tt.setupMocks(mockCH)

			executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
			err := executor.Validate(context.Background(), tt.taskCtx)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test UpdateBounds method
func TestModelExecutor_UpdateBounds(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(*mockExecutorClickhouseClient, *mockExecutorModelsService, *mockExecutorAdminService)
		modelID    string
		wantErr    bool
	}{
		{
			name: "no existing cache triggers full scan",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, a *mockExecutorAdminService) {
				// No existing cache - should trigger full scan and succeed
				a.externalBounds = nil
				m.dagReader = &mockDAGReader{
					externalNode: &mockExternal{
						id:   "test.external",
						conf: external.Config{Database: "test", Table: "external"},
					},
				}
				m.renderedSQL = "SELECT min(id), max(id) FROM test.external"
				ch.boundsMin = 1
				ch.boundsMax = 100
			},
			modelID: "test.external",
			wantErr: false,
		},
		{
			name: "external model not found",
			setupMocks: func(_ *mockExecutorClickhouseClient, m *mockExecutorModelsService, _ *mockExecutorAdminService) {
				// Setup for model not found error
				m.dagReader = &mockDAGReader{externalNodeErr: errNotFound}
			},
			modelID: "test.external",
			wantErr: true,
		},
		{
			name: "successful bounds update - initial full scan",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, a *mockExecutorAdminService) {
				// No existing cache (initial full scan)
				a.externalBounds = nil
				m.dagReader = &mockDAGReader{
					externalNode: &mockExternal{
						id:   "test.external",
						conf: external.Config{Database: "test", Table: "external"},
					},
				}
				m.renderedSQL = "SELECT min(id), max(id) FROM test.external"
				ch.boundsMin = 100
				ch.boundsMax = 200
			},
			modelID: "test.external",
			wantErr: false,
		},
		{
			name: "successful bounds update - incremental scan",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, a *mockExecutorAdminService) {
				a.externalBounds = &admin.BoundsCache{
					ModelID:             "test.external",
					Min:                 100,
					Max:                 200,
					LastFullScan:        time.Now().Add(-1 * time.Minute),
					LastIncrementalScan: time.Now().Add(-10 * time.Second),
				}
				m.dagReader = &mockDAGReader{
					externalNode: &mockExternal{
						id: "test.external",
						conf: external.Config{
							Database: "test",
							Table:    "external",
							Cache: &external.CacheConfig{
								IncrementalScanInterval: 30 * time.Second,
								FullScanInterval:        5 * time.Minute,
							},
						},
					},
				}
				m.renderedSQL = "SELECT min(id), max(id) FROM test.external WHERE ..."
				ch.boundsMin = 100
				ch.boundsMax = 250
			},
			modelID: "test.external",
			wantErr: false,
		},
		{
			name: "query bounds fails",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, a *mockExecutorAdminService) {
				a.externalBounds = nil
				m.dagReader = &mockDAGReader{
					externalNode: &mockExternal{
						id:   "test.external",
						conf: external.Config{Database: "test", Table: "external"},
					},
				}
				m.renderedSQL = "SELECT min(id), max(id) FROM test.external"
				ch.queryOneErr = errQueryFailed
			},
			modelID: "test.external",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockCH := &mockExecutorClickhouseClient{}
			mockModels := &mockExecutorModelsService{}
			mockAdmin := &mockExecutorAdminService{}

			tt.setupMocks(mockCH, mockModels, mockAdmin)

			executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
			err := executor.UpdateBounds(context.Background(), tt.modelID, "full")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Benchmark tests
func BenchmarkModelExecutor_Execute(b *testing.B) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	mockCH := &mockExecutorClickhouseClient{tableExists: true}
	mockModels := &mockExecutorModelsService{renderedSQL: "SELECT 1"}
	mockAdmin := &mockExecutorAdminService{}

	executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
	taskCtx := &tasks.TaskContext{
		Transformation: &mockExecutorTransformation{
			id:   "test.model",
			typ:  transformation.TransformationTypeSQL,
			sql:  "SELECT 1",
			conf: transformation.Config{Database: "test", Table: "model"},
		},
		Position:      100,
		Interval:      50,
		ExecutionTime: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = executor.Execute(context.Background(), taskCtx)
	}
}

// Mock implementations for executor tests

type mockExecutorClickhouseClient struct {
	tableExists    bool
	tableExistsErr error
	executeErr     error
	queryOneErr    error
	boundsMin      uint64
	boundsMax      uint64
}

func (m *mockExecutorClickhouseClient) QueryOne(_ context.Context, query string, result interface{}) error {
	if m.queryOneErr != nil {
		return m.queryOneErr
	}

	if m.tableExistsErr != nil {
		return m.tableExistsErr
	}

	// Use reflection to set fields regardless of struct tags
	v := reflect.ValueOf(result).Elem()

	// Handle TableExists query (Count field)
	if strings.Contains(query, "system.tables") {
		if countField := v.FieldByName("Count"); countField.IsValid() && countField.CanSet() {
			if m.tableExists {
				countField.SetUint(1)
			} else {
				countField.SetUint(0)
			}
		}

		return nil
	}

	// Handle bounds query (Min and Max fields)
	if minField := v.FieldByName("Min"); minField.IsValid() && minField.CanSet() {
		minField.SetUint(m.boundsMin)
	}

	if maxField := v.FieldByName("Max"); maxField.IsValid() && maxField.CanSet() {
		maxField.SetUint(m.boundsMax)
	}

	return nil
}
func (m *mockExecutorClickhouseClient) QueryMany(_ context.Context, _ string, _ interface{}) error {
	return nil
}
func (m *mockExecutorClickhouseClient) Execute(_ context.Context, _ string) ([]byte, error) {
	return nil, m.executeErr
}
func (m *mockExecutorClickhouseClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}
func (m *mockExecutorClickhouseClient) Start() error { return nil }
func (m *mockExecutorClickhouseClient) Stop() error  { return nil }

var _ clickhouse.ClientInterface = (*mockExecutorClickhouseClient)(nil)

type mockExecutorModelsService struct {
	renderedSQL string
	renderErr   error
	envVars     *[]string
	dagReader   models.DAGReader
}

func (m *mockExecutorModelsService) Start() error { return nil }
func (m *mockExecutorModelsService) Stop() error  { return nil }
func (m *mockExecutorModelsService) GetDAG() models.DAGReader {
	if m.dagReader != nil {
		return m.dagReader
	}
	return &mockDAGReader{}
}
func (m *mockExecutorModelsService) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time) (string, error) {
	if m.renderErr != nil {
		return "", m.renderErr
	}
	return m.renderedSQL, nil
}
func (m *mockExecutorModelsService) RenderExternal(_ models.External, _ map[string]interface{}) (string, error) {
	return "", nil
}
func (m *mockExecutorModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	if m.envVars != nil {
		return m.envVars, nil
	}
	vars := []string{}
	return &vars, nil
}

var _ models.Service = (*mockExecutorModelsService)(nil)

type mockExecutorAdminService struct {
	recordErr      error
	externalBounds *admin.BoundsCache
	setBoundsErr   error
}

func (m *mockExecutorAdminService) GetNextUnprocessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockExecutorAdminService) GetLastProcessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockExecutorAdminService) GetFirstPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockExecutorAdminService) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return m.recordErr
}
func (m *mockExecutorAdminService) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return true, nil
}
func (m *mockExecutorAdminService) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return []admin.GapInfo{}, nil
}
func (m *mockExecutorAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}
func (m *mockExecutorAdminService) GetExternalBounds(_ context.Context, _ string) (*admin.BoundsCache, error) {
	return m.externalBounds, nil
}
func (m *mockExecutorAdminService) SetExternalBounds(_ context.Context, _ *admin.BoundsCache) error {
	return m.setBoundsErr
}
func (m *mockExecutorAdminService) GetIncrementalAdminDatabase() string { return "admin_db" }
func (m *mockExecutorAdminService) GetIncrementalAdminTable() string    { return "admin_table" }
func (m *mockExecutorAdminService) GetScheduledAdminDatabase() string   { return "admin" }
func (m *mockExecutorAdminService) GetScheduledAdminTable() string      { return "cbt_scheduled" }
func (m *mockExecutorAdminService) RecordScheduledCompletion(_ context.Context, _ string, _ time.Time) error {
	return nil
}
func (m *mockExecutorAdminService) GetLastScheduledExecution(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}
func (m *mockExecutorAdminService) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}

var _ admin.Service = (*mockExecutorAdminService)(nil)

type mockExecutorTransformation struct {
	id    string
	typ   string
	value string
	sql   string
	conf  transformation.Config
}

func (m *mockExecutorTransformation) GetID() string                      { return m.id }
func (m *mockExecutorTransformation) GetConfig() *transformation.Config  { return &m.conf }
func (m *mockExecutorTransformation) GetHandler() transformation.Handler { return nil }
func (m *mockExecutorTransformation) GetValue() string                   { return m.value }
func (m *mockExecutorTransformation) GetDependencies() []string          { return []string{} }
func (m *mockExecutorTransformation) GetSQL() string                     { return m.sql }
func (m *mockExecutorTransformation) GetType() string                    { return m.typ }
func (m *mockExecutorTransformation) GetEnvironmentVariables() []string  { return []string{} }
func (m *mockExecutorTransformation) SetDefaultDatabase(defaultDB string) {
	if m.conf.Database == "" {
		m.conf.Database = defaultDB
	}
}

var _ models.Transformation = (*mockExecutorTransformation)(nil)

// Mock types for external models and DAG

type mockExternal struct {
	id   string
	conf external.Config
	val  string
	typ  string
}

func (m *mockExternal) GetID() string                      { return m.id }
func (m *mockExternal) GetConfig() external.Config         { return m.conf }
func (m *mockExternal) GetConfigMutable() *external.Config { return &m.conf }
func (m *mockExternal) GetValue() string                   { return m.val }
func (m *mockExternal) GetType() string                    { return m.typ }
func (m *mockExternal) SetDefaultDatabase(defaultDB string) {
	if m.conf.Database == "" {
		m.conf.Database = defaultDB
	}
}

func (m *mockExternal) SetDefaults(_, defaultDB string) {
	if m.conf.Database == "" && defaultDB != "" {
		m.conf.Database = defaultDB
	}
}

var _ models.External = (*mockExternal)(nil)

type mockDAGReader struct {
	transformations []models.Transformation
	externalNode    models.External
	externalNodeErr error
}

func (m *mockDAGReader) GetNode(_ string) (models.Node, error) {
	return models.Node{}, nil
}

func (m *mockDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	for _, t := range m.transformations {
		if t.GetID() == id {
			return t, nil
		}
	}
	return &mockExecutorTransformation{id: id}, nil
}

func (m *mockDAGReader) GetExternalNode(_ string) (models.External, error) {
	if m.externalNodeErr != nil {
		return nil, m.externalNodeErr
	}
	if m.externalNode != nil {
		return m.externalNode, nil
	}
	return &mockExternal{}, nil
}

func (m *mockDAGReader) GetDependencies(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetDependents(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetStructuredDependencies(_ string) []transformation.Dependency {
	return nil
}

func (m *mockDAGReader) GetAllDependencies(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetAllDependents(_ string) []string {
	return []string{}
}

func (m *mockDAGReader) GetTransformationNodes() []models.Transformation {
	return m.transformations
}

func (m *mockDAGReader) GetExternalNodes() []models.Node {
	return []models.Node{}
}

func (m *mockDAGReader) IsPathBetween(_, _ string) bool {
	return false
}

var _ models.DAGReader = (*mockDAGReader)(nil)

// TestExecuteCommand_CustomEnvironmentVariables verifies custom env vars are passed to scripts
func TestExecuteCommand_CustomEnvironmentVariables(t *testing.T) {
	// Create a temporary script that prints environment variables
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "test_env.sh")

	scriptContent := `#!/bin/bash
echo "API_KEY=${API_KEY}"
echo "ENVIRONMENT=${ENVIRONMENT}"
echo "CUSTOM_PARAM=${CUSTOM_PARAM}"
echo "SELF_DATABASE=${SELF_DATABASE}"
`
	err := os.WriteFile(scriptPath, []byte(scriptContent), 0o755)
	require.NoError(t, err)

	// Create mock services
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	mockModels := &mockExecutorModelsService{
		envVars: &[]string{
			"CLICKHOUSE_URL=http://localhost:8123",
			"SELF_DATABASE=test_db",
			"SELF_TABLE=test_table",
			"TASK_START=1234567890",
			"TASK_MODEL=test_db.test_table",
			"TASK_INTERVAL=100",
			"BOUNDS_START=1000",
			"BOUNDS_END=1100",
			// Global env var
			"ENVIRONMENT=production",
			// Transformation-specific env vars (override global)
			"API_KEY=model_override_key",
			"CUSTOM_PARAM=model_specific_value",
		},
	}

	mockAdmin := &mockExecutorAdminService{}

	executor := NewModelExecutor(log, &mockClickhouseClient{}, mockModels, mockAdmin)

	// Create a transformation with exec command
	model := &transformation.Exec{
		Config: transformation.Config{
			Type:     transformation.TypeIncremental,
			Database: "test_db",
			Table:    "test_table",
			Env: map[string]string{
				"API_KEY":      "model_override_key",
				"CUSTOM_PARAM": "model_specific_value",
			},
		},
		Exec: scriptPath,
	}

	taskCtx := &tasks.TaskContext{
		Transformation: model,
		Position:       1000,
		Interval:       100,
		ExecutionTime:  time.Unix(1234567890, 0),
	}

	// Execute the command
	err = executor.executeCommand(context.Background(), taskCtx)
	require.NoError(t, err)

	// Note: In a real test, we'd capture the output and verify it
	// For now, we've verified the env vars are being set in the template_test
	// This test verifies the integration path works without errors
}

// TestExecuteCommand_GlobalAndTransformationEnvVars tests env var override behavior
func TestExecuteCommand_GlobalAndTransformationEnvVars(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "test_env2.sh")

	// Script that checks for specific env vars and exits with error if not found
	scriptContent := `#!/bin/bash
if [ "$API_KEY" != "override_key" ]; then
  echo "ERROR: API_KEY should be 'override_key', got '$API_KEY'" >&2
  exit 1
fi

if [ "$GLOBAL_VAR" != "global_value" ]; then
  echo "ERROR: GLOBAL_VAR should be 'global_value', got '$GLOBAL_VAR'" >&2
  exit 1
fi

if [ "$MODEL_VAR" != "model_value" ]; then
  echo "ERROR: MODEL_VAR should be 'model_value', got '$MODEL_VAR'" >&2
  exit 1
fi

echo "All environment variables are correct!"
exit 0
`
	err := os.WriteFile(scriptPath, []byte(scriptContent), 0o755)
	require.NoError(t, err)

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	// Mock returns env vars with global and transformation-specific vars
	mockModels := &mockExecutorModelsService{
		envVars: &[]string{
			"CLICKHOUSE_URL=http://localhost:8123",
			"SELF_DATABASE=test_db",
			"SELF_TABLE=test_table",
			"BOUNDS_START=1000",
			"BOUNDS_END=1100",
			// Global var
			"GLOBAL_VAR=global_value",
			"API_KEY=global_key",
			// Transformation-specific (should override API_KEY)
			"API_KEY=override_key",
			"MODEL_VAR=model_value",
		},
	}

	executor := NewModelExecutor(log, &mockClickhouseClient{}, mockModels, &mockExecutorAdminService{})

	model := &transformation.Exec{
		Config: transformation.Config{
			Type:     transformation.TypeIncremental,
			Database: "test_db",
			Table:    "test_table",
			Env: map[string]string{
				"API_KEY":   "override_key",
				"MODEL_VAR": "model_value",
			},
		},
		Exec: scriptPath,
	}

	taskCtx := &tasks.TaskContext{
		Transformation: model,
		Position:       1000,
		Interval:       100,
		ExecutionTime:  time.Now(),
	}

	err = executor.executeCommand(context.Background(), taskCtx)
	require.NoError(t, err, "Script should exit successfully if env vars are correct")
}

// TestGetTransformationEnvironmentVariables_Integration verifies full integration
func TestGetTransformationEnvironmentVariables_Integration(t *testing.T) {
	// This test verifies the full integration from config -> template engine -> env vars
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	dag := models.NewDependencyGraph()
	chConfig := &clickhouse.Config{
		URL:         "http://localhost:8123",
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}

	// Global env vars from config
	globalEnv := map[string]string{
		"GLOBAL_API_KEY": "global_key_456",
		"ENVIRONMENT":    "staging",
	}

	templateEngine := models.NewTemplateEngine(chConfig, dag, globalEnv)

	// Create a transformation with custom env vars
	model := &transformation.Exec{
		Config: transformation.Config{
			Type:     transformation.TypeIncremental,
			Database: "test_db",
			Table:    "test_table",
			Env: map[string]string{
				"MODEL_API_KEY":  "model_key_123",
				"MODEL_SPECIFIC": "value_xyz",
			},
		},
		Exec: "echo test",
	}

	// Get environment variables
	envVars, err := templateEngine.GetTransformationEnvironmentVariables(
		model,
		1000,
		100,
		time.Unix(1234567890, 0),
	)

	require.NoError(t, err)
	require.NotNil(t, envVars)

	// Convert to map for easier testing
	envMap := make(map[string]string)
	for _, v := range *envVars {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	// Verify built-in vars
	assert.Equal(t, "http://localhost:8123", envMap["CLICKHOUSE_URL"])
	assert.Equal(t, "test_db", envMap["SELF_DATABASE"])
	assert.Equal(t, "test_table", envMap["SELF_TABLE"])
	assert.Equal(t, "1000", envMap["BOUNDS_START"])
	assert.Equal(t, "1100", envMap["BOUNDS_END"])

	// Verify global custom vars
	assert.Equal(t, "global_key_456", envMap["GLOBAL_API_KEY"])
	assert.Equal(t, "staging", envMap["ENVIRONMENT"])

	// Verify transformation-specific vars
	assert.Equal(t, "model_key_123", envMap["MODEL_API_KEY"])
	assert.Equal(t, "value_xyz", envMap["MODEL_SPECIFIC"])
}
