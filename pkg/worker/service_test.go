package worker

import (
	"context"
	"fmt"
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

// TestFilteredTransformations_NoDuplicates verifies that transformations with multiple
// matching tags are only returned once (regression test for duplicate bug fix).
func TestFilteredTransformations_NoDuplicates(t *testing.T) {
	// Transformation has both tag1 and tag2
	trans := &mockTransformation{
		id:   "model.multi_tag",
		tags: []string{"tag1", "tag2", "tag3"},
	}

	mockModels := &mockModelsService{
		transformations: []models.Transformation{trans},
	}

	tests := []struct {
		name          string
		tags          []string
		expectedCount int
	}{
		{
			name:          "single matching tag",
			tags:          []string{"tag1"},
			expectedCount: 1,
		},
		{
			name:          "two matching tags - should still return once",
			tags:          []string{"tag1", "tag2"},
			expectedCount: 1,
		},
		{
			name:          "all three matching tags - should still return once",
			tags:          []string{"tag1", "tag2", "tag3"},
			expectedCount: 1,
		},
		{
			name:          "duplicate tags in filter - should still return once",
			tags:          []string{"tag1", "tag1", "tag1"},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filteredTransformations(mockModels, tt.tags)
			assert.Len(t, result, tt.expectedCount,
				"transformation should only appear once regardless of matching tag count")
		})
	}
}

// TestFilteredTransformations_EdgeCases tests edge cases and boundary conditions.
func TestFilteredTransformations_EdgeCases(t *testing.T) {
	t.Run("empty transformations list", func(t *testing.T) {
		mockModels := &mockModelsService{
			transformations: []models.Transformation{},
		}

		result := filteredTransformations(mockModels, []string{"tag1"})
		assert.Empty(t, result)
	})

	t.Run("nil tags slice", func(t *testing.T) {
		trans := &mockTransformation{id: "model.test", tags: []string{"tag1"}}
		mockModels := &mockModelsService{
			transformations: []models.Transformation{trans},
		}

		// nil tags should behave like empty tags - return all
		result := filteredTransformations(mockModels, nil)
		assert.Len(t, result, 1)
	})

	t.Run("transformation with nil handler", func(t *testing.T) {
		trans := &mockTransformation{
			id:      "model.nil_handler",
			tags:    []string{"tag1"},
			handler: nil, // Explicitly nil - GetHandler will return mockHandler with tags
		}
		mockModels := &mockModelsService{
			transformations: []models.Transformation{trans},
		}

		result := filteredTransformations(mockModels, []string{"tag1"})
		// Should still work because mockTransformation returns mockHandler when handler is nil
		assert.Len(t, result, 1)
	})

	t.Run("transformation with empty tags", func(t *testing.T) {
		trans := &mockTransformation{
			id:   "model.no_tags",
			tags: []string{}, // Empty tags
		}
		mockModels := &mockModelsService{
			transformations: []models.Transformation{trans},
		}

		result := filteredTransformations(mockModels, []string{"tag1"})
		assert.Empty(t, result, "transformation with no tags should not match")
	})

	t.Run("case sensitivity", func(t *testing.T) {
		trans := &mockTransformation{
			id:   "model.case_test",
			tags: []string{"Tag1", "TAG2"},
		}
		mockModels := &mockModelsService{
			transformations: []models.Transformation{trans},
		}

		// Tags are case-sensitive
		result := filteredTransformations(mockModels, []string{"tag1"})
		assert.Empty(t, result, "tag matching should be case-sensitive")

		result = filteredTransformations(mockModels, []string{"Tag1"})
		assert.Len(t, result, 1, "exact case match should work")
	})

	t.Run("special characters in tags", func(t *testing.T) {
		trans := &mockTransformation{
			id:   "model.special",
			tags: []string{"tag-with-dashes", "tag_with_underscores", "tag.with.dots"},
		}
		mockModels := &mockModelsService{
			transformations: []models.Transformation{trans},
		}

		tests := []string{"tag-with-dashes", "tag_with_underscores", "tag.with.dots"}
		for _, tag := range tests {
			result := filteredTransformations(mockModels, []string{tag})
			assert.Len(t, result, 1, "special characters should match exactly: %s", tag)
		}
	})
}

// TestFilteredTransformations_LargeScale tests with many transformations and tags.
func TestFilteredTransformations_LargeScale(t *testing.T) {
	t.Run("many transformations few tags", func(t *testing.T) {
		// 1000 transformations, each with 5 tags
		transformations := make([]models.Transformation, 1000)
		for i := 0; i < 1000; i++ {
			tags := make([]string, 5)
			for j := 0; j < 5; j++ {
				tags[j] = fmt.Sprintf("tag_%d_%d", i%10, j) // Creates groups of similar tags
			}
			transformations[i] = &mockTransformation{
				id:   fmt.Sprintf("model.test_%d", i),
				tags: tags,
			}
		}

		mockModels := &mockModelsService{transformations: transformations}

		// Filter by tags that should match ~100 transformations each
		result := filteredTransformations(mockModels, []string{"tag_0_0", "tag_1_0"})

		// Should match transformations where i%10 == 0 or i%10 == 1
		assert.Len(t, result, 200)
	})

	t.Run("few transformations many tags", func(t *testing.T) {
		trans := &mockTransformation{
			id:   "model.many_tags",
			tags: []string{"target_tag"},
		}
		mockModels := &mockModelsService{
			transformations: []models.Transformation{trans},
		}

		// 100 filter tags, only one matches
		filterTags := make([]string, 100)
		for i := 0; i < 100; i++ {
			filterTags[i] = fmt.Sprintf("filter_tag_%d", i)
		}
		filterTags[50] = "target_tag" // One matching tag

		result := filteredTransformations(mockModels, filterTags)
		assert.Len(t, result, 1)
	})

	t.Run("many transformations many tags", func(t *testing.T) {
		// 500 transformations with 10 tags each
		transformations := make([]models.Transformation, 500)
		for i := 0; i < 500; i++ {
			tags := make([]string, 10)
			for j := 0; j < 10; j++ {
				tags[j] = fmt.Sprintf("tag_%d", (i*10+j)%100) // 100 unique tags total
			}
			transformations[i] = &mockTransformation{
				id:   fmt.Sprintf("model.test_%d", i),
				tags: tags,
			}
		}

		mockModels := &mockModelsService{transformations: transformations}

		// Filter by 50 tags
		filterTags := make([]string, 50)
		for i := 0; i < 50; i++ {
			filterTags[i] = fmt.Sprintf("tag_%d", i)
		}

		result := filteredTransformations(mockModels, filterTags)
		// Result count depends on tag distribution, just verify no panic
		assert.NotNil(t, result)
		// With our distribution, most transformations should match
		assert.Greater(t, len(result), 0)
	})
}

// TestFilteredTransformations_OrderPreservation verifies that the original order is preserved.
func TestFilteredTransformations_OrderPreservation(t *testing.T) {
	transformations := make([]models.Transformation, 100)
	for i := 0; i < 100; i++ {
		transformations[i] = &mockTransformation{
			id:   fmt.Sprintf("model.test_%03d", i), // Zero-padded for easy sorting verification
			tags: []string{"common_tag"},
		}
	}

	mockModels := &mockModelsService{transformations: transformations}

	result := filteredTransformations(mockModels, []string{"common_tag"})

	require.Len(t, result, 100)

	// Verify order is preserved
	for i, trans := range result {
		expectedID := fmt.Sprintf("model.test_%03d", i)
		assert.Equal(t, expectedID, trans.GetID(),
			"transformation order should be preserved, expected %s at index %d", expectedID, i)
	}
}

// BenchmarkFilteredTransformations benchmarks the filtering performance.
func BenchmarkFilteredTransformations(b *testing.B) {
	// Setup: 1000 transformations with 5 tags each, filter by 10 tags
	transformations := make([]models.Transformation, 1000)
	for i := 0; i < 1000; i++ {
		tags := make([]string, 5)
		for j := 0; j < 5; j++ {
			tags[j] = fmt.Sprintf("tag_%d_%d", i%50, j)
		}
		transformations[i] = &mockTransformation{
			id:   fmt.Sprintf("model.test_%d", i),
			tags: tags,
		}
	}
	mockModels := &mockModelsService{transformations: transformations}
	filterTags := []string{
		"tag_0_0", "tag_1_0", "tag_2_0", "tag_3_0", "tag_4_0",
		"tag_5_0", "tag_6_0", "tag_7_0", "tag_8_0", "tag_9_0",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filteredTransformations(mockModels, filterTags)
	}
}

// BenchmarkFilteredTransformations_Scaling benchmarks with different scales.
func BenchmarkFilteredTransformations_Scaling(b *testing.B) {
	benchmarks := []struct {
		name               string
		numTransformations int
		tagsPerTrans       int
		numFilterTags      int
	}{
		{"small_10x3x2", 10, 3, 2},
		{"medium_100x5x10", 100, 5, 10},
		{"large_1000x5x10", 1000, 5, 10},
		{"xlarge_1000x10x50", 1000, 10, 50},
		{"many_tags_100x5x100", 100, 5, 100},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			transformations := make([]models.Transformation, bm.numTransformations)
			for i := 0; i < bm.numTransformations; i++ {
				tags := make([]string, bm.tagsPerTrans)
				for j := 0; j < bm.tagsPerTrans; j++ {
					tags[j] = fmt.Sprintf("tag_%d_%d", i%50, j)
				}
				transformations[i] = &mockTransformation{
					id:   fmt.Sprintf("model.test_%d", i),
					tags: tags,
				}
			}
			mockModels := &mockModelsService{transformations: transformations}

			filterTags := make([]string, bm.numFilterTags)
			for i := 0; i < bm.numFilterTags; i++ {
				filterTags[i] = fmt.Sprintf("tag_%d_0", i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = filteredTransformations(mockModels, filterTags)
			}
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
func (m *mockAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}
func (m *mockAdminService) GetExternalBounds(_ context.Context, _ string) (*admin.BoundsCache, error) {
	return nil, nil
}
func (m *mockAdminService) SetExternalBounds(_ context.Context, _ *admin.BoundsCache) error {
	return nil
}
func (m *mockAdminService) GetIncrementalAdminDatabase() string { return "admin_db" }
func (m *mockAdminService) GetIncrementalAdminTable() string    { return "admin_table" }
func (m *mockAdminService) GetScheduledAdminDatabase() string   { return "admin" }
func (m *mockAdminService) GetScheduledAdminTable() string      { return "cbt_scheduled" }
func (m *mockAdminService) RecordScheduledCompletion(_ context.Context, _ string, _ time.Time) error {
	return nil
}
func (m *mockAdminService) GetLastScheduledExecution(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (m *mockAdminService) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}
func (m *mockAdminService) AcquireBoundsLock(_ context.Context, _ string) (admin.BoundsLock, error) {
	return &mockServiceBoundsLock{}, nil
}

// mockServiceBoundsLock implements admin.BoundsLock for testing
type mockServiceBoundsLock struct{}

func (m *mockServiceBoundsLock) Unlock(_ context.Context) error {
	return nil
}

var _ admin.BoundsLock = (*mockServiceBoundsLock)(nil)
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
func (m *mockModelsService) RenderExternal(_ models.External, _ map[string]interface{}) (string, error) {
	return "", nil
}
func (m *mockModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	vars := []string{}
	return &vars, nil
}

var _ models.Service = (*mockModelsService)(nil)

// Mock handler for testing
type mockHandler struct {
	tags []string
}

func (m *mockHandler) Type() transformation.Type {
	return transformation.TypeIncremental
}

func (m *mockHandler) Config() any {
	return &transformation.Config{
		Type:     transformation.TypeIncremental,
		Database: "test_db",
		Table:    "test_table",
	}
}

func (m *mockHandler) Validate() error {
	return nil
}

func (m *mockHandler) ShouldTrackPosition() bool {
	return true
}

func (m *mockHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return map[string]any{}
}

func (m *mockHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{
		Database: "admin",
		Table:    "cbt",
	}
}

func (m *mockHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

func (m *mockHandler) GetTags() []string {
	return m.tags
}

type mockTransformation struct {
	id      string
	tags    []string
	handler transformation.Handler
}

func (m *mockTransformation) GetID() string { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config {
	return &transformation.Config{
		Type:     transformation.TypeIncremental,
		Database: "test_db",
		Table:    "test_table",
	}
}
func (m *mockTransformation) GetHandler() transformation.Handler {
	if m.handler != nil {
		return m.handler
	}
	// Return a mock handler with tags
	return &mockHandler{tags: m.tags}
}
func (m *mockTransformation) GetValue() string                  { return "" }
func (m *mockTransformation) GetDependencies() []string         { return []string{} }
func (m *mockTransformation) GetSQL() string                    { return "" }
func (m *mockTransformation) GetType() string                   { return "transformation" }
func (m *mockTransformation) GetEnvironmentVariables() []string { return []string{} }
func (m *mockTransformation) SetDefaultDatabase(_ string) {
	// No-op for mock
}

var _ models.Transformation = (*mockTransformation)(nil)

// mockDAGReader is defined in executor_test.go and shared across tests
// We'll use the more comprehensive version from executor_test.go
