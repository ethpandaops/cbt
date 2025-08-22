package worker

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test service creation (ethPandaOps requirement)
func TestNewService(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Tags:            []string{"test"},
				Concurrency:     5,
				ShutdownTimeout: 30,
			},
			wantErr: false,
		},
		{
			name: "invalid config - zero concurrency",
			cfg: &Config{
				Tags:            []string{"test"},
				Concurrency:     0,
				ShutdownTimeout: 30,
			},
			wantErr: true,
		},
		{
			name: "invalid config - negative concurrency",
			cfg: &Config{
				Tags:            []string{"test"},
				Concurrency:     -1,
				ShutdownTimeout: 30,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			mockCH := &mockClickhouseClient{}
			mockAdmin := &mockAdminService{}
			mockModels := &mockModelsService{
				transformations: []models.Transformation{},
			}
			var redisOpt *redis.Options // nil for unit tests
			mockValidator := validation.NewMockValidator()

			svc, err := NewService(log, tt.cfg, mockCH, mockAdmin, mockModels, redisOpt, mockValidator)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, svc)

				// Verify it implements the Service interface
				var _ = svc
			}
		})
	}
}

// Test service structure initialization
func TestServiceInitialization(t *testing.T) {
	log := logrus.New()
	cfg := &Config{
		Tags:            []string{"test"},
		Concurrency:     5,
		ShutdownTimeout: 30,
	}
	mockCH := &mockClickhouseClient{}
	mockAdmin := &mockAdminService{}
	mockModels := &mockModelsService{
		transformations: []models.Transformation{},
	}
	var redisOpt *redis.Options
	mockValidator := validation.NewMockValidator()

	svc, err := NewService(log, cfg, mockCH, mockAdmin, mockModels, redisOpt, mockValidator)
	require.NoError(t, err)

	serviceCast := svc.(*service)

	// Verify internal structures are initialized
	assert.NotNil(t, serviceCast.done)
	assert.NotNil(t, serviceCast.log)
	assert.NotNil(t, serviceCast.config)
	assert.Equal(t, cfg, serviceCast.config)
	assert.NotNil(t, serviceCast.chClient)
	assert.NotNil(t, serviceCast.admin)
	assert.NotNil(t, serviceCast.models)
	assert.NotNil(t, serviceCast.validator)
}

// Test Config validation
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Concurrency:     5,
				ShutdownTimeout: 30,
			},
			wantErr: false,
		},
		{
			name: "invalid - zero concurrency",
			cfg: &Config{
				Concurrency:     0,
				ShutdownTimeout: 30,
			},
			wantErr: true,
		},
		{
			name: "invalid - negative concurrency",
			cfg: &Config{
				Concurrency:     -1,
				ShutdownTimeout: 30,
			},
			wantErr: true,
		},
		{
			name: "valid - high concurrency",
			cfg: &Config{
				Concurrency:     100,
				ShutdownTimeout: 60,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test filteredTransformations function
func TestFilteredTransformations(t *testing.T) {
	trans1 := &mockTransformation{
		id:   "model.test1",
		tags: []string{"tag1", "tag2"},
	}
	trans2 := &mockTransformation{
		id:   "model.test2",
		tags: []string{"tag2", "tag3"},
	}
	trans3 := &mockTransformation{
		id:   "model.test3",
		tags: []string{"tag3", "tag4"},
	}

	mockModels := &mockModelsService{
		transformations: []models.Transformation{trans1, trans2, trans3},
	}

	tests := []struct {
		name     string
		tags     []string
		expected []string // expected model IDs
	}{
		{
			name:     "no tags - returns all",
			tags:     []string{},
			expected: []string{"model.test1", "model.test2", "model.test3"},
		},
		{
			name:     "single tag match",
			tags:     []string{"tag1"},
			expected: []string{"model.test1"},
		},
		{
			name:     "multiple tags - returns union",
			tags:     []string{"tag1", "tag3"},
			expected: []string{"model.test1", "model.test2", "model.test3"},
		},
		{
			name:     "no matching tags",
			tags:     []string{"tag5"},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filteredTransformations(mockModels, tt.tags)

			resultIDs := make([]string, len(result))
			for i, trans := range result {
				resultIDs[i] = trans.GetID()
			}

			assert.ElementsMatch(t, tt.expected, resultIDs)
		})
	}
}

// Benchmark tests (ethPandaOps performance requirement)
func BenchmarkNewService(b *testing.B) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	cfg := &Config{
		Tags:            []string{"test"},
		Concurrency:     5,
		ShutdownTimeout: 30,
	}
	mockCH := &mockClickhouseClient{}
	mockAdmin := &mockAdminService{}
	mockModels := &mockModelsService{
		transformations: []models.Transformation{},
	}
	var redisOpt *redis.Options
	mockValidator := validation.NewMockValidator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewService(log, cfg, mockCH, mockAdmin, mockModels, redisOpt, mockValidator)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Mock implementations for testing

type mockClickhouseClient struct{}

func (m *mockClickhouseClient) QueryOne(_ context.Context, _ string, _ interface{}) error { return nil }
func (m *mockClickhouseClient) QueryMany(_ context.Context, _ string, _ interface{}) error {
	return nil
}
func (m *mockClickhouseClient) Execute(_ context.Context, _ string) error { return nil }
func (m *mockClickhouseClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}
func (m *mockClickhouseClient) Start() error { return nil }
func (m *mockClickhouseClient) Stop() error  { return nil }

var _ clickhouse.ClientInterface = (*mockClickhouseClient)(nil)

type mockAdminService struct{}

func (m *mockAdminService) GetLastProcessedEndPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockAdminService) GetNextUnprocessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockAdminService) GetLastProcessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockAdminService) GetFirstPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockAdminService) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return nil
}
func (m *mockAdminService) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return true, nil
}
func (m *mockAdminService) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return []admin.GapInfo{}, nil
}
func (m *mockAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}
func (m *mockAdminService) GetCacheManager() *admin.CacheManager { return nil }
func (m *mockAdminService) GetAdminDatabase() string             { return "admin_db" }
func (m *mockAdminService) GetAdminTable() string                { return "admin_table" }

var _ admin.Service = (*mockAdminService)(nil)

type mockModelsService struct {
	transformations []models.Transformation
}

func (m *mockModelsService) Start() error { return nil }
func (m *mockModelsService) Stop() error  { return nil }
func (m *mockModelsService) GetDAG() models.DAGReader {
	return &mockDAGReader{transformations: m.transformations}
}
func (m *mockModelsService) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time) (string, error) {
	return "", nil
}
func (m *mockModelsService) RenderExternal(_ models.External) (string, error) {
	return "", nil
}
func (m *mockModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	vars := []string{}
	return &vars, nil
}

var _ models.Service = (*mockModelsService)(nil)

type mockTransformation struct {
	id       string
	tags     []string
	interval uint64
}

func (m *mockTransformation) GetID() string { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config {
	return &transformation.Config{
		Database: "test_db",
		Table:    "test_table",
		ForwardFill: &transformation.ForwardFillConfig{
			Interval: m.interval,
			Schedule: "@every 1m",
		},
		Tags:         m.tags,
		Dependencies: []string{},
	}
}
func (m *mockTransformation) GetValue() string                  { return "" }
func (m *mockTransformation) GetDependencies() []string         { return []string{} }
func (m *mockTransformation) GetSQL() string                    { return "" }
func (m *mockTransformation) GetType() string                   { return "transformation" }
func (m *mockTransformation) GetEnvironmentVariables() []string { return []string{} }

var _ models.Transformation = (*mockTransformation)(nil)

type mockDAGReader struct {
	transformations []models.Transformation
}

func (m *mockDAGReader) GetNode(id string) (models.Node, error) {
	for _, t := range m.transformations {
		if t.GetID() == id {
			return models.Node{NodeType: "transformation", Model: t}, nil
		}
	}
	return models.Node{}, nil
}

func (m *mockDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	for _, t := range m.transformations {
		if t.GetID() == id {
			return t, nil
		}
	}
	return nil, nil
}

func (m *mockDAGReader) GetExternalNode(_ string) (models.External, error) {
	return nil, nil
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

func (m *mockDAGReader) IsPathBetween(_, _ string) bool {
	return false
}

var _ models.DAGReader = (*mockDAGReader)(nil)
