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
)

// Test NewModelExecutor
func TestNewModelExecutor(t *testing.T) {
	log := logrus.New()
	mockCH := &mockExecutorClickhouseClient{}
	mockModels := &mockExecutorModelsService{}
	mockAdmin := &mockExecutorAdminService{}

	executor := NewModelExecutor(log, mockCH, mockModels, mockAdmin)

	assert.NotNil(t, executor)
	assert.NotNil(t, executor.logger)
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
}

func (m *mockExecutorClickhouseClient) QueryOne(_ context.Context, query string, result interface{}) error {
	if m.tableExistsErr != nil {
		return m.tableExistsErr
	}
	// Handle TableExists query
	if !strings.Contains(query, "system.tables") {
		return nil
	}
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
}

func (m *mockExecutorModelsService) Start() error { return nil }
func (m *mockExecutorModelsService) Stop() error  { return nil }
func (m *mockExecutorModelsService) GetDAG() models.DAGReader {
	return &mockDAGReader{}
}
func (m *mockExecutorModelsService) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time) (string, error) {
	if m.renderErr != nil {
		return "", m.renderErr
	}
	return m.renderedSQL, nil
}
func (m *mockExecutorModelsService) RenderExternal(_ models.External) (string, error) {
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
	recordErr error
}

func (m *mockExecutorAdminService) GetLastPosition(_ context.Context, _ string) (uint64, error) {
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
func (m *mockExecutorAdminService) GetCacheManager() *admin.CacheManager { return nil }
func (m *mockExecutorAdminService) GetAdminDatabase() string             { return "admin_db" }
func (m *mockExecutorAdminService) GetAdminTable() string                { return "admin_table" }

var _ admin.Service = (*mockExecutorAdminService)(nil)

type mockExecutorTransformation struct {
	id    string
	typ   string
	value string
	sql   string
	conf  transformation.Config
}

func (m *mockExecutorTransformation) GetID() string                     { return m.id }
func (m *mockExecutorTransformation) GetConfig() transformation.Config  { return m.conf }
func (m *mockExecutorTransformation) GetValue() string                  { return m.value }
func (m *mockExecutorTransformation) GetDependencies() []string         { return []string{} }
func (m *mockExecutorTransformation) GetSQL() string                    { return m.sql }
func (m *mockExecutorTransformation) GetType() string                   { return m.typ }
func (m *mockExecutorTransformation) GetEnvironmentVariables() []string { return []string{} }

var _ models.Transformation = (*mockExecutorTransformation)(nil)
