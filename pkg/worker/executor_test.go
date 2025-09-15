package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
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
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
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
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
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
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
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
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
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
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
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
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "record completion fails",
			setupMocks: func(ch *mockExecutorClickhouseClient, m *mockExecutorModelsService, a *mockExecutorAdminService) {
				ch.tableExists = true
				m.renderedSQL = "SELECT 1"
				a.recordErr = errMockExecute
			},
			taskCtx: &tasks.TaskContext{
				Transformation: &mockExecutorTransformation{
					id:   "test.model",
					typ:  transformation.TransformationTypeSQL,
					sql:  "SELECT 1",
					conf: transformation.Config{Database: "test", Table: "model"},
				},
				Position:  100,
				Interval:  50,
				StartTime: time.Now(),
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
				ch.queryResult = &struct {
					Min validation.FlexUint64 `json:"min"`
					Max validation.FlexUint64 `json:"max"`
				}{Min: 1, Max: 100}
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
				ch.queryResult = &struct {
					Min validation.FlexUint64 `json:"min"`
					Max validation.FlexUint64 `json:"max"`
				}{Min: 100, Max: 200}
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
				ch.queryResult = &struct {
					Min validation.FlexUint64 `json:"min"`
					Max validation.FlexUint64 `json:"max"`
				}{Min: 100, Max: 250}
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
			err := executor.UpdateBounds(context.Background(), tt.modelID)

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
		Position:  100,
		Interval:  50,
		StartTime: time.Now(),
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
	queryResult    interface{}
}

func (m *mockExecutorClickhouseClient) QueryOne(_ context.Context, query string, result interface{}) error {
	if m.queryOneErr != nil {
		return m.queryOneErr
	}
	if m.tableExistsErr != nil {
		return m.tableExistsErr
	}
	// Handle TableExists query
	if strings.Contains(query, "system.tables") {
		r, ok := result.(*struct {
			Count uint64 `json:"count,string"`
		})
		if !ok {
			return nil
		}
		if m.tableExists {
			r.Count = 1
		} else {
			r.Count = 0
		}
		return nil
	}
	// Handle bounds query
	if m.queryResult != nil {
		if boundsResult, ok := result.(*struct {
			Min validation.FlexUint64 `json:"min"`
			Max validation.FlexUint64 `json:"max"`
		}); ok {
			if mockResult, ok := m.queryResult.(*struct {
				Min validation.FlexUint64 `json:"min"`
				Max validation.FlexUint64 `json:"max"`
			}); ok {
				boundsResult.Min = mockResult.Min
				boundsResult.Max = mockResult.Max
			}
		}
	}
	return nil
}
func (m *mockExecutorClickhouseClient) QueryMany(_ context.Context, _ string, _ interface{}) error {
	return nil
}
func (m *mockExecutorClickhouseClient) Execute(_ context.Context, _ string) error {
	return m.executeErr
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
func (m *mockExecutorModelsService) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time, _ string) (string, error) {
	if m.renderErr != nil {
		return "", m.renderErr
	}
	return m.renderedSQL, nil
}
func (m *mockExecutorModelsService) RenderExternal(_ models.External, _ map[string]interface{}) (string, error) {
	return "", nil
}
func (m *mockExecutorModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time, _ string) (*[]string, error) {
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

func (m *mockExecutorAdminService) GetLastProcessedEndPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
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
func (m *mockExecutorAdminService) GetAdminDatabase() string { return "admin_db" }
func (m *mockExecutorAdminService) GetAdminTable() string    { return "admin_table" }

var _ admin.Service = (*mockExecutorAdminService)(nil)

type mockExecutorTransformation struct {
	id    string
	typ   string
	value string
	sql   string
	conf  transformation.Config
}

func (m *mockExecutorTransformation) GetID() string                     { return m.id }
func (m *mockExecutorTransformation) GetConfig() *transformation.Config { return &m.conf }
func (m *mockExecutorTransformation) GetValue() string                  { return m.value }
func (m *mockExecutorTransformation) GetDependencies() []string         { return []string{} }
func (m *mockExecutorTransformation) GetSQL() string                    { return m.sql }
func (m *mockExecutorTransformation) GetType() string                   { return m.typ }
func (m *mockExecutorTransformation) GetEnvironmentVariables() []string { return []string{} }
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

func (m *mockExternal) GetID() string              { return m.id }
func (m *mockExternal) GetConfig() external.Config { return m.conf }
func (m *mockExternal) GetValue() string           { return m.val }
func (m *mockExternal) GetType() string            { return m.typ }
func (m *mockExternal) SetDefaultDatabase(defaultDB string) {
	if m.conf.Database == "" {
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
