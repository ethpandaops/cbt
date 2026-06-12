package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
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
	mockCH := &testutil.FakeClickHouseClient{}
	mockModels := &testutil.FakeModelsService{}
	mockAdmin := &adminfake.FakeAdminService{}

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
		setupMocks      func(*testutil.FakeClickHouseClient, *testutil.FakeModelsService, *adminfake.FakeAdminService)
		taskCtx         *tasks.ExecutionContext
		wantErr         bool
		expectedErrType error
	}{
		{
			name: "validation fails - table does not exist",
			setupMocks: func(ch *testutil.FakeClickHouseClient, _ *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				ch.TableExists = false
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					ID:     "test.model",
					Type:   transformation.TypeSQL,
					Config: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "SQL execution success",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				ch.TableExists = true
				m.RenderedSQL = "SELECT 1; SELECT 2"
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					ID:     "test.model",
					Type:   transformation.TypeSQL,
					Config: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "exec command success",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				ch.TableExists = true
				m.EnvVars = &[]string{"VAR1=value1"}
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					ID:     "test.model",
					Type:   transformation.TypeExec,
					Value:  "echo 'test'",
					Config: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "invalid transformation type",
			setupMocks: func(ch *testutil.FakeClickHouseClient, _ *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				ch.TableExists = true
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					ID:     "test.model",
					Type:   "invalid",
					Config: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "SQL render fails",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				ch.TableExists = true
				m.RenderErr = errMockRender
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					ID:     "test.model",
					Type:   transformation.TypeSQL,
					Config: transformation.Config{Database: "test", Table: "model"},
				},
				Position:      100,
				Interval:      50,
				ExecutionTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "SQL execution fails",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				ch.TableExists = true
				ch.ExecuteErr = errMockExecute
				m.RenderedSQL = "SELECT 1"
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					ID:     "test.model",
					Type:   transformation.TypeSQL,
					Config: transformation.Config{Database: "test", Table: "model"},
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
			mockCH := &testutil.FakeClickHouseClient{}
			mockModels := &testutil.FakeModelsService{}
			mockAdmin := &adminfake.FakeAdminService{}

			tt.setupMocks(mockCH, mockModels, mockAdmin)

			executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
			err := executor.Execute(context.Background(), tt.taskCtx)

			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErrType != nil {
					require.ErrorIs(t, err, tt.expectedErrType)
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
		setupMocks func(*testutil.FakeClickHouseClient)
		taskCtx    *tasks.ExecutionContext
		wantErr    bool
	}{
		{
			name: "table exists",
			setupMocks: func(ch *testutil.FakeClickHouseClient) {
				ch.TableExists = true
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					Config: transformation.Config{Database: "test", Table: "model"},
				},
			},
			wantErr: false,
		},
		{
			name: "table does not exist",
			setupMocks: func(ch *testutil.FakeClickHouseClient) {
				ch.TableExists = false
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					Config: transformation.Config{Database: "test", Table: "model"},
				},
			},
			wantErr: true,
		},
		{
			name: "table check fails",
			setupMocks: func(ch *testutil.FakeClickHouseClient) {
				ch.QueryOneErr = errCheckFailed
			},
			taskCtx: &tasks.ExecutionContext{
				Transformation: &testutil.FakeTransformation{
					Config: transformation.Config{Database: "test", Table: "model"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockCH := &testutil.FakeClickHouseClient{}
			mockModels := &testutil.FakeModelsService{}
			mockAdmin := &adminfake.FakeAdminService{}

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
		setupMocks func(*testutil.FakeClickHouseClient, *testutil.FakeModelsService, *adminfake.FakeAdminService)
		modelID    string
		wantErr    bool
	}{
		{
			name: "no existing cache triggers full scan",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, a *adminfake.FakeAdminService) {
				// No existing cache - should trigger full scan and succeed
				a.ExternalBoundsDefault = nil
				m.DAG = &testutil.FakeDAGReader{
					ExternalNode: &testutil.FakeExternal{
						ID:     "test.external",
						Config: external.Config{Database: "test", Table: "external"},
					},
				}
				m.RenderedSQL = "SELECT min(id), max(id) FROM test.external"
				ch.BoundsMin = 1
				ch.BoundsMax = 100
			},
			modelID: "test.external",
			wantErr: false,
		},
		{
			name: "external model not found",
			setupMocks: func(_ *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, _ *adminfake.FakeAdminService) {
				// Setup for model not found error
				m.DAG = &testutil.FakeDAGReader{ExternalNodeErr: errNotFound}
			},
			modelID: "test.external",
			wantErr: true,
		},
		{
			name: "successful bounds update - initial full scan",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, a *adminfake.FakeAdminService) {
				// No existing cache (initial full scan)
				a.ExternalBoundsDefault = nil
				m.DAG = &testutil.FakeDAGReader{
					ExternalNode: &testutil.FakeExternal{
						ID:     "test.external",
						Config: external.Config{Database: "test", Table: "external"},
					},
				}
				m.RenderedSQL = "SELECT min(id), max(id) FROM test.external"
				ch.BoundsMin = 100
				ch.BoundsMax = 200
			},
			modelID: "test.external",
			wantErr: false,
		},
		{
			name: "successful bounds update - incremental scan",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, a *adminfake.FakeAdminService) {
				a.ExternalBoundsDefault = &admin.BoundsCache{
					ModelID:             "test.external",
					Min:                 100,
					Max:                 200,
					LastFullScan:        time.Now().Add(-1 * time.Minute),
					LastIncrementalScan: time.Now().Add(-10 * time.Second),
				}
				m.DAG = &testutil.FakeDAGReader{
					ExternalNode: &testutil.FakeExternal{
						ID: "test.external",
						Config: external.Config{
							Database: "test",
							Table:    "external",
							Cache: &external.CacheConfig{
								IncrementalScanInterval: 30 * time.Second,
								FullScanInterval:        5 * time.Minute,
							},
						},
					},
				}
				m.RenderedSQL = "SELECT min(id), max(id) FROM test.external WHERE ..."
				ch.BoundsMin = 100
				ch.BoundsMax = 250
			},
			modelID: "test.external",
			wantErr: false,
		},
		{
			name: "query bounds fails",
			setupMocks: func(ch *testutil.FakeClickHouseClient, m *testutil.FakeModelsService, a *adminfake.FakeAdminService) {
				a.ExternalBoundsDefault = nil
				m.DAG = &testutil.FakeDAGReader{
					ExternalNode: &testutil.FakeExternal{
						ID:     "test.external",
						Config: external.Config{Database: "test", Table: "external"},
					},
				}
				m.RenderedSQL = "SELECT min(id), max(id) FROM test.external"
				ch.QueryOneErr = errQueryFailed
			},
			modelID: "test.external",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			mockCH := &testutil.FakeClickHouseClient{}
			mockModels := &testutil.FakeModelsService{}
			mockAdmin := &adminfake.FakeAdminService{}

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
	mockCH := &testutil.FakeClickHouseClient{TableExists: true}
	mockModels := &testutil.FakeModelsService{RenderedSQL: "SELECT 1"}
	mockAdmin := &adminfake.FakeAdminService{}

	executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
	taskCtx := &tasks.ExecutionContext{
		Transformation: &testutil.FakeTransformation{
			ID:     "test.model",
			Type:   transformation.TypeSQL,
			Config: transformation.Config{Database: "test", Table: "model"},
		},
		Position:      100,
		Interval:      50,
		ExecutionTime: time.Now(),
	}

	b.ResetTimer()
	for range b.N {
		_ = executor.Execute(context.Background(), taskCtx)
	}
}

// Test computeFinalBounds - zero protection logic
func TestModelExecutor_ComputeFinalBounds(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	executor := &ModelExecutor{log: log}

	tests := []struct {
		name          string
		scanType      string
		queryMin      uint64
		queryMax      uint64
		existingCache *admin.BoundsCache
		expectedMin   uint64
		expectedMax   uint64
	}{
		{
			name:          "incremental zero with existing cache preserves bounds",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      0,
			queryMax:      0,
			existingCache: &admin.BoundsCache{Min: 100, Max: 200},
			expectedMin:   100,
			expectedMax:   200,
		},
		{
			name:          "incremental zero with no cache uses zeros",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      0,
			queryMax:      0,
			existingCache: nil,
			expectedMin:   0,
			expectedMax:   0,
		},
		{
			name:          "incremental with data uses query values",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      150,
			queryMax:      250,
			existingCache: &admin.BoundsCache{Min: 100, Max: 200},
			expectedMin:   150,
			expectedMax:   250,
		},
		{
			name:          "full scan zero with existing cache preserves bounds",
			scanType:      tasks.ScanTypeFull,
			queryMin:      0,
			queryMax:      0,
			existingCache: &admin.BoundsCache{Min: 100, Max: 200},
			expectedMin:   100,
			expectedMax:   200,
		},
		{
			name:          "full scan zero with no cache uses zeros",
			scanType:      tasks.ScanTypeFull,
			queryMin:      0,
			queryMax:      0,
			existingCache: nil,
			expectedMin:   0,
			expectedMax:   0,
		},
		{
			name:          "full scan zero with cache max=0 uses zeros",
			scanType:      tasks.ScanTypeFull,
			queryMin:      0,
			queryMax:      0,
			existingCache: &admin.BoundsCache{Min: 0, Max: 0},
			expectedMin:   0,
			expectedMax:   0,
		},
		{
			name:          "full scan with data overwrites cache",
			scanType:      tasks.ScanTypeFull,
			queryMin:      50,
			queryMax:      300,
			existingCache: &admin.BoundsCache{Min: 100, Max: 200},
			expectedMin:   50,
			expectedMax:   300,
		},
		{
			name:          "full scan with data and no cache uses query values",
			scanType:      tasks.ScanTypeFull,
			queryMin:      100,
			queryMax:      200,
			existingCache: nil,
			expectedMin:   100,
			expectedMax:   200,
		},
		// Asymmetric zero protection tests (bug fix)
		{
			name:          "incremental min valid max zero with cache - preserve bounds",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      150,
			queryMax:      0,
			existingCache: &admin.BoundsCache{Min: 100, Max: 500},
			expectedMin:   100,
			expectedMax:   500, // Preserved! Not corrupted to 0
		},
		{
			name:          "full scan min valid max zero with cache - preserve bounds",
			scanType:      tasks.ScanTypeFull,
			queryMin:      150,
			queryMax:      0,
			existingCache: &admin.BoundsCache{Min: 100, Max: 500},
			expectedMin:   100,
			expectedMax:   500, // Preserved!
		},
		{
			name:          "incremental min greater than max with cache - preserve bounds",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      600,
			queryMax:      200,
			existingCache: &admin.BoundsCache{Min: 100, Max: 500},
			expectedMin:   100,
			expectedMax:   500, // Preserved! Invalid bounds rejected
		},
		{
			name:          "full scan min greater than max with cache - preserve bounds",
			scanType:      tasks.ScanTypeFull,
			queryMin:      600,
			queryMax:      200,
			existingCache: &admin.BoundsCache{Min: 100, Max: 500},
			expectedMin:   100,
			expectedMax:   500, // Preserved!
		},
		{
			name:          "min valid max zero no cache - use query values",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      150,
			queryMax:      0,
			existingCache: nil, // No existing cache
			expectedMin:   150,
			expectedMax:   0, // Can't preserve, must use query values
		},
		{
			name:          "min zero max valid - legitimate zero start",
			scanType:      tasks.ScanTypeFull,
			queryMin:      0,
			queryMax:      500,
			existingCache: &admin.BoundsCache{Min: 100, Max: 200},
			expectedMin:   0,
			expectedMax:   500, // Valid: data could legitimately start at 0
		},
		{
			name:          "cache with zero max - no protection needed",
			scanType:      tasks.ScanTypeIncremental,
			queryMin:      150,
			queryMax:      0,
			existingCache: &admin.BoundsCache{Min: 0, Max: 0}, // Cache has no data
			expectedMin:   150,
			expectedMax:   0, // No protection because cache.Max == 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMin, gotMax := executor.computeFinalBounds(tt.queryMin, tt.queryMax, tt.existingCache, tt.scanType)
			assert.Equal(t, tt.expectedMin, gotMin, "min bound mismatch")
			assert.Equal(t, tt.expectedMax, gotMax, "max bound mismatch")
		})
	}
}

// Test that InitialScanComplete is set correctly after a full scan
func TestModelExecutor_UpdateBounds_InitialScanComplete(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	mockCH := &testutil.FakeClickHouseClient{
		BoundsMin: 100,
		BoundsMax: 200,
	}

	var savedCache *admin.BoundsCache
	mockAdmin := &adminfake.FakeAdminService{
		ExternalBoundsDefault: nil, // No existing cache - first scan
		OnSetExternalBounds: func(cache *admin.BoundsCache) {
			savedCache = cache
		},
	}

	mockModels := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{
			ExternalNode: &testutil.FakeExternal{
				ID:     "test.external",
				Config: external.Config{Database: "test", Table: "external"},
			},
		},
		RenderedSQL: "SELECT min(id), max(id) FROM test.external",
	}

	executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.NoError(t, err)

	// Verify InitialScanComplete is true after first full scan
	require.NotNil(t, savedCache, "Cache should have been saved")
	assert.True(t, savedCache.InitialScanComplete, "InitialScanComplete should be true after full scan")
	assert.NotNil(t, savedCache.InitialScanStarted, "InitialScanStarted should be set")
}

// Test that incremental scans are skipped when InitialScanComplete is false
func TestModelExecutor_UpdateBounds_SkipsIncrementalBeforeInitialComplete(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	mockCH := &testutil.FakeClickHouseClient{}

	now := time.Now().UTC()
	mockAdmin := &adminfake.FakeAdminService{
		ExternalBoundsDefault: &admin.BoundsCache{
			ModelID:             "test.external",
			Min:                 100,
			Max:                 200,
			InitialScanComplete: false, // Initial scan not complete
			InitialScanStarted:  &now,
		},
	}

	mockModels := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{
			ExternalNode: &testutil.FakeExternal{
				ID:     "test.external",
				Config: external.Config{Database: "test", Table: "external"},
			},
		},
	}

	executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeIncremental)
	require.NoError(t, err)

	// Should skip without error and not query ClickHouse
	// (mockCH would error if QueryOne was called without setup)
}

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

	mockModels := &testutil.FakeModelsService{
		EnvVars: &[]string{
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

	mockAdmin := &adminfake.FakeAdminService{}

	executor := NewModelExecutor(log, &testutil.FakeClickHouseClient{}, mockModels, mockAdmin)

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

	taskCtx := &tasks.ExecutionContext{
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
	mockModels := &testutil.FakeModelsService{
		EnvVars: &[]string{
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

	executor := NewModelExecutor(log, &testutil.FakeClickHouseClient{}, mockModels, &adminfake.FakeAdminService{})

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

	taskCtx := &tasks.ExecutionContext{
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
