package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors for branch coverage.
var (
	errBoundsLock = errors.New("acquire lock failed")
	errUnlock     = errors.New("unlock failed")
	errSetBounds  = errors.New("set bounds failed")
	errEnvVars    = errors.New("env vars failed")
)

// erroringLock is an admin.BoundsLock whose Unlock returns a configured error.
type erroringLock struct {
	err error
}

func (l *erroringLock) Unlock(_ context.Context) error { return l.err }

func quietLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

// externalModelsService returns a FakeModelsService whose DAG exposes a single external
// node and renders a fixed bounds query.
func externalModelsService() *testutil.FakeModelsService {
	return &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{
			ExternalNode: &testutil.FakeExternal{
				ID:     "test.external",
				Config: external.Config{Database: "test", Table: "external"},
			},
		},
		RenderedSQL: "SELECT min, max FROM test.external",
	}
}

// TestUpdateBounds_InitialCacheError covers the warn branch when the initial
// GetExternalBounds call returns an error (UpdateBounds still proceeds with a full scan).
func TestUpdateBounds_InitialCacheError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{BoundsMin: 1, BoundsMax: 100}
	mockAdmin := &adminfake.FakeAdminService{
		GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
			return nil, errSetBounds // any error; warned and ignored
		},
	}
	mockModels := externalModelsService()

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.NoError(t, err)
}

// TestApplyBoundsWithLock_AcquireError covers the AcquireBoundsLock error branch.
func TestApplyBoundsWithLock_AcquireError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{BoundsMin: 1, BoundsMax: 100}
	mockAdmin := &adminfake.FakeAdminService{
		AcquireBoundsLockFn: func(_ context.Context, _ string) (admin.BoundsLock, error) {
			return nil, errBoundsLock
		},
	}
	mockModels := externalModelsService()

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.ErrorIs(t, err, errBoundsLock)
	assert.Contains(t, err.Error(), "failed to acquire bounds lock")
}

// TestApplyBoundsWithLock_UnlockError covers the deferred Unlock error branch.
func TestApplyBoundsWithLock_UnlockError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{BoundsMin: 1, BoundsMax: 100}
	mockAdmin := &adminfake.FakeAdminService{
		AcquireBoundsLockFn: func(_ context.Context, _ string) (admin.BoundsLock, error) {
			return &erroringLock{err: errUnlock}, nil
		},
	}
	mockModels := externalModelsService()

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	// Unlock failure is logged, not returned, so the overall call still succeeds.
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.NoError(t, err)
}

// TestApplyBoundsWithLock_FreshCacheError covers the warn branch when the fresh cache
// read (while holding the lock) returns an error.
func TestApplyBoundsWithLock_FreshCacheError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{BoundsMin: 1, BoundsMax: 100}

	var call int
	mockAdmin := &adminfake.FakeAdminService{
		GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
			call++
			if call == 1 {
				return nil, nil // initial read: no cache
			}
			return nil, errSetBounds // fresh read under lock: error
		},
	}
	mockModels := externalModelsService()

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.NoError(t, err)
}

// TestApplyBoundsWithLock_UpdateCacheError covers the updateBoundsCache error path.
func TestApplyBoundsWithLock_UpdateCacheError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{BoundsMin: 1, BoundsMax: 100}
	mockAdmin := &adminfake.FakeAdminService{
		SetExternalBoundsFn: func(_ context.Context, _ *admin.BoundsCache) error {
			return errSetBounds
		},
	}
	mockModels := externalModelsService()

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.ErrorIs(t, err, errSetBounds)
	assert.Contains(t, err.Error(), "failed to update bounds cache")
}

// TestUpdateBounds_QueryRenderError covers queryExternalBounds RenderExternal failure.
func TestUpdateBounds_QueryRenderError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{}
	mockAdmin := &adminfake.FakeAdminService{}
	mockModels := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{
			ExternalNode: &testutil.FakeExternal{
				ID:     "test.external",
				Config: external.Config{Database: "test", Table: "external"},
			},
		},
		RenderExternalFn: func(_ models.External, _ map[string]any) (string, error) {
			return "", errSetBounds
		},
	}

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	err := executor.UpdateBounds(context.Background(), "test.external", tasks.ScanTypeFull)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query external bounds")
}

// TestShouldSkipScan covers all branches of shouldSkipScan directly.
func TestShouldSkipScan(t *testing.T) {
	now := time.Now().UTC()
	staleStart := now.Add(-31 * time.Minute)
	recentStart := now.Add(-1 * time.Minute)

	tests := []struct {
		name     string
		scanType string
		cache    *admin.BoundsCache
		want     bool
	}{
		{
			name:     "incremental with nil cache skips",
			scanType: tasks.ScanTypeIncremental,
			cache:    nil,
			want:     true,
		},
		{
			name:     "incremental before initial complete skips (recent start)",
			scanType: tasks.ScanTypeIncremental,
			cache:    &admin.BoundsCache{InitialScanComplete: false, InitialScanStarted: &recentStart},
			want:     true,
		},
		{
			name:     "incremental before initial complete skips (stuck start warns)",
			scanType: tasks.ScanTypeIncremental,
			cache:    &admin.BoundsCache{InitialScanComplete: false, InitialScanStarted: &staleStart},
			want:     true,
		},
		{
			name:     "incremental before initial complete with nil start",
			scanType: tasks.ScanTypeIncremental,
			cache:    &admin.BoundsCache{InitialScanComplete: false, InitialScanStarted: nil},
			want:     true,
		},
		{
			name:     "incremental after initial complete does not skip",
			scanType: tasks.ScanTypeIncremental,
			cache:    &admin.BoundsCache{InitialScanComplete: true},
			want:     false,
		},
		{
			name:     "full scan never skips",
			scanType: tasks.ScanTypeFull,
			cache:    nil,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldSkipScan(quietLogger(), "test.external", tt.scanType, tt.cache, now)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestUpdateCacheTimestamps covers the incremental and full-scan timestamp branches.
func TestUpdateCacheTimestamps(t *testing.T) {
	now := time.Now().UTC()
	earlier := now.Add(-time.Hour)

	t.Run("incremental with existing cache copies prior timestamps", func(t *testing.T) {
		existing := &admin.BoundsCache{
			LastFullScan:       earlier,
			InitialScanStarted: &earlier,
		}
		newCache := &admin.BoundsCache{}
		updateCacheTimestamps(newCache, existing, true, false, now)

		assert.Equal(t, now, newCache.LastIncrementalScan)
		assert.Equal(t, earlier, newCache.LastFullScan)
		require.NotNil(t, newCache.InitialScanStarted)
		assert.Equal(t, earlier, *newCache.InitialScanStarted)
	})

	t.Run("incremental with no existing cache", func(t *testing.T) {
		newCache := &admin.BoundsCache{}
		updateCacheTimestamps(newCache, nil, true, false, now)

		assert.Equal(t, now, newCache.LastIncrementalScan)
		assert.True(t, newCache.LastFullScan.IsZero())
		assert.Nil(t, newCache.InitialScanStarted)
	})

	t.Run("full scan first time sets initial start", func(t *testing.T) {
		newCache := &admin.BoundsCache{}
		updateCacheTimestamps(newCache, nil, false, true, now)

		assert.Equal(t, now, newCache.LastFullScan)
		assert.Equal(t, now, newCache.LastIncrementalScan)
		require.NotNil(t, newCache.InitialScanStarted)
		assert.Equal(t, now, *newCache.InitialScanStarted)
	})

	t.Run("full scan with existing initial start preserves it", func(t *testing.T) {
		existing := &admin.BoundsCache{InitialScanStarted: &earlier}
		newCache := &admin.BoundsCache{}
		updateCacheTimestamps(newCache, existing, false, true, now)

		require.NotNil(t, newCache.InitialScanStarted)
		assert.Equal(t, earlier, *newCache.InitialScanStarted)
	})

	t.Run("full scan with existing cache but nil initial start", func(t *testing.T) {
		existing := &admin.BoundsCache{InitialScanStarted: nil}
		newCache := &admin.BoundsCache{}
		updateCacheTimestamps(newCache, existing, false, true, now)

		// Neither the first-scan nor the preserve branch sets it.
		assert.Nil(t, newCache.InitialScanStarted)
	})
}

// TestExecute_CommandError covers the executeCommand error path through Execute.
func TestExecute_CommandError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{TableExists: true}
	mockModels := &testutil.FakeModelsService{EnvVars: &[]string{}}
	mockAdmin := &adminfake.FakeAdminService{}

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	taskCtx := &tasks.ExecutionContext{
		Transformation: &testutil.FakeTransformation{
			ID:     "test.model",
			Type:   transformation.TypeExec,
			Value:  "exit 1", // non-zero exit triggers CombinedOutput error
			Config: transformation.Config{Database: "test", Table: "model"},
		},
		Position:      100,
		Interval:      50,
		ExecutionTime: time.Now(),
	}

	err := executor.Execute(context.Background(), taskCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "command execution failed")
}

// envErrModels overrides GetTransformationEnvironmentVariables to return an error,
// which the base FakeModelsService does not support.
type envErrModels struct {
	*testutil.FakeModelsService
	err error
}

func (m *envErrModels) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	return nil, m.err
}

// TestExecuteCommand_EnvVarsError covers the GetTransformationEnvironmentVariables error.
func TestExecuteCommand_EnvVarsError(t *testing.T) {
	mockCH := &testutil.FakeClickHouseClient{TableExists: true}
	mockModels := &envErrModels{FakeModelsService: &testutil.FakeModelsService{}, err: errEnvVars}
	mockAdmin := &adminfake.FakeAdminService{}

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	taskCtx := &tasks.ExecutionContext{
		Transformation: &testutil.FakeTransformation{
			ID:     "test.model",
			Type:   transformation.TypeExec,
			Value:  "echo ok",
			Config: transformation.Config{Database: "test", Table: "model"},
		},
		ExecutionTime: time.Now(),
	}

	err := executor.executeCommand(context.Background(), taskCtx)
	require.ErrorIs(t, err, errEnvVars)
	assert.Contains(t, err.Error(), "failed to render SQL template")
}

// TestExecuteSQL_EmptyStatementsAndTruncation covers the empty-statement skip and the
// >500 char SQL preview truncation branches.
func TestExecuteSQL_EmptyStatementsAndTruncation(t *testing.T) {
	longStmt := "SELECT " + strings.Repeat("a", 600)
	// Two empty statements (leading semicolon and a blank one) plus one long statement.
	rendered := "  ;  ;" + longStmt

	mockCH := &testutil.FakeClickHouseClient{TableExists: true}
	mockModels := &testutil.FakeModelsService{RenderedSQL: rendered}
	mockAdmin := &adminfake.FakeAdminService{}

	executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
	taskCtx := &tasks.ExecutionContext{
		Transformation: &testutil.FakeTransformation{
			ID:     "test.model",
			Type:   transformation.TypeSQL,
			Config: transformation.Config{Database: "test", Table: "model"},
		},
		ExecutionTime: time.Now(),
	}

	err := executor.Execute(context.Background(), taskCtx)
	require.NoError(t, err)
}
