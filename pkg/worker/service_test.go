package worker

import (
	"fmt"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models"
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
			mockCH := &testutil.FakeClickHouseClient{}
			mockAdmin := &adminfake.FakeAdminService{}
			mockModels := &testutil.FakeModelsService{
				Transformations: []models.Transformation{},
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
	mockCH := &testutil.FakeClickHouseClient{}
	mockAdmin := &adminfake.FakeAdminService{}
	mockModels := &testutil.FakeModelsService{
		Transformations: []models.Transformation{},
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
	trans1 := &testutil.FakeTransformation{
		ID:   "model.test1",
		Tags: []string{"tag1", "tag2"},
	}
	trans2 := &testutil.FakeTransformation{
		ID:   "model.test2",
		Tags: []string{"tag2", "tag3"},
	}
	trans3 := &testutil.FakeTransformation{
		ID:   "model.test3",
		Tags: []string{"tag3", "tag4"},
	}

	mockModels := &testutil.FakeModelsService{
		Transformations: []models.Transformation{trans1, trans2, trans3},
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
	trans := &testutil.FakeTransformation{
		ID:   "model.multi_tag",
		Tags: []string{"tag1", "tag2", "tag3"},
	}

	mockModels := &testutil.FakeModelsService{
		Transformations: []models.Transformation{trans},
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
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{},
		}

		result := filteredTransformations(mockModels, []string{"tag1"})
		assert.Empty(t, result)
	})

	t.Run("nil tags slice", func(t *testing.T) {
		trans := &testutil.FakeTransformation{ID: "model.test", Tags: []string{"tag1"}}
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{trans},
		}

		// nil tags should behave like empty tags - return all
		result := filteredTransformations(mockModels, nil)
		assert.Len(t, result, 1)
	})

	t.Run("transformation with nil handler", func(t *testing.T) {
		trans := &testutil.FakeTransformation{
			ID:      "model.nil_handler",
			Tags:    []string{"tag1"},
			Handler: nil, // Explicitly nil - GetHandler will return mockHandler with tags
		}
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{trans},
		}

		result := filteredTransformations(mockModels, []string{"tag1"})
		// Should still work because mockTransformation returns mockHandler when handler is nil
		assert.Len(t, result, 1)
	})

	t.Run("transformation with empty tags", func(t *testing.T) {
		trans := &testutil.FakeTransformation{
			ID:   "model.no_tags",
			Tags: []string{}, // Empty tags
		}
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{trans},
		}

		result := filteredTransformations(mockModels, []string{"tag1"})
		assert.Empty(t, result, "transformation with no tags should not match")
	})

	t.Run("case sensitivity", func(t *testing.T) {
		trans := &testutil.FakeTransformation{
			ID:   "model.case_test",
			Tags: []string{"Tag1", "TAG2"},
		}
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{trans},
		}

		// Tags are case-sensitive
		result := filteredTransformations(mockModels, []string{"tag1"})
		assert.Empty(t, result, "tag matching should be case-sensitive")

		result = filteredTransformations(mockModels, []string{"Tag1"})
		assert.Len(t, result, 1, "exact case match should work")
	})

	t.Run("special characters in tags", func(t *testing.T) {
		trans := &testutil.FakeTransformation{
			ID:   "model.special",
			Tags: []string{"tag-with-dashes", "tag_with_underscores", "tag.with.dots"},
		}
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{trans},
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
		for i := range 1000 {
			tags := make([]string, 5)
			for j := range 5 {
				tags[j] = fmt.Sprintf("tag_%d_%d", i%10, j) // Creates groups of similar tags
			}
			transformations[i] = &testutil.FakeTransformation{
				ID:   fmt.Sprintf("model.test_%d", i),
				Tags: tags,
			}
		}

		mockModels := &testutil.FakeModelsService{Transformations: transformations}

		// Filter by tags that should match ~100 transformations each
		result := filteredTransformations(mockModels, []string{"tag_0_0", "tag_1_0"})

		// Should match transformations where i%10 == 0 or i%10 == 1
		assert.Len(t, result, 200)
	})

	t.Run("few transformations many tags", func(t *testing.T) {
		trans := &testutil.FakeTransformation{
			ID:   "model.many_tags",
			Tags: []string{"target_tag"},
		}
		mockModels := &testutil.FakeModelsService{
			Transformations: []models.Transformation{trans},
		}

		// 100 filter tags, only one matches
		filterTags := make([]string, 100)
		for i := range 100 {
			filterTags[i] = fmt.Sprintf("filter_tag_%d", i)
		}
		filterTags[50] = "target_tag" // One matching tag

		result := filteredTransformations(mockModels, filterTags)
		assert.Len(t, result, 1)
	})

	t.Run("many transformations many tags", func(t *testing.T) {
		// 500 transformations with 10 tags each
		transformations := make([]models.Transformation, 500)
		for i := range 500 {
			tags := make([]string, 10)
			for j := range 10 {
				tags[j] = fmt.Sprintf("tag_%d", (i*10+j)%100) // 100 unique tags total
			}
			transformations[i] = &testutil.FakeTransformation{
				ID:   fmt.Sprintf("model.test_%d", i),
				Tags: tags,
			}
		}

		mockModels := &testutil.FakeModelsService{Transformations: transformations}

		// Filter by 50 tags
		filterTags := make([]string, 50)
		for i := range 50 {
			filterTags[i] = fmt.Sprintf("tag_%d", i)
		}

		result := filteredTransformations(mockModels, filterTags)
		// Result count depends on tag distribution, just verify no panic
		assert.NotNil(t, result)
		// With our distribution, most transformations should match
		assert.NotEmpty(t, result)
	})
}

// TestFilteredTransformations_OrderPreservation verifies that the original order is preserved.
func TestFilteredTransformations_OrderPreservation(t *testing.T) {
	transformations := make([]models.Transformation, 100)
	for i := range 100 {
		transformations[i] = &testutil.FakeTransformation{
			ID:   fmt.Sprintf("model.test_%03d", i), // Zero-padded for easy sorting verification
			Tags: []string{"common_tag"},
		}
	}

	mockModels := &testutil.FakeModelsService{Transformations: transformations}

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
	for i := range 1000 {
		tags := make([]string, 5)
		for j := range 5 {
			tags[j] = fmt.Sprintf("tag_%d_%d", i%50, j)
		}
		transformations[i] = &testutil.FakeTransformation{
			ID:   fmt.Sprintf("model.test_%d", i),
			Tags: tags,
		}
	}
	mockModels := &testutil.FakeModelsService{Transformations: transformations}
	filterTags := []string{
		"tag_0_0", "tag_1_0", "tag_2_0", "tag_3_0", "tag_4_0",
		"tag_5_0", "tag_6_0", "tag_7_0", "tag_8_0", "tag_9_0",
	}

	b.ResetTimer()
	for range b.N {
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
			for i := range bm.numTransformations {
				tags := make([]string, bm.tagsPerTrans)
				for j := range bm.tagsPerTrans {
					tags[j] = fmt.Sprintf("tag_%d_%d", i%50, j)
				}
				transformations[i] = &testutil.FakeTransformation{
					ID:   fmt.Sprintf("model.test_%d", i),
					Tags: tags,
				}
			}
			mockModels := &testutil.FakeModelsService{Transformations: transformations}

			filterTags := make([]string, bm.numFilterTags)
			for i := range bm.numFilterTags {
				filterTags[i] = fmt.Sprintf("tag_%d_0", i)
			}

			b.ResetTimer()
			for range b.N {
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
	mockCH := &testutil.FakeClickHouseClient{}
	mockAdmin := &adminfake.FakeAdminService{}
	mockModels := &testutil.FakeModelsService{
		Transformations: []models.Transformation{},
	}
	var redisOpt *redis.Options
	mockValidator := validation.NewMockValidator()

	b.ResetTimer()
	for range b.N {
		_, err := NewService(log, cfg, mockCH, mockAdmin, mockModels, redisOpt, mockValidator)
		if err != nil {
			b.Fatal(err)
		}
	}
}
