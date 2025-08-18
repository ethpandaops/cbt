package validation

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockCacheManager for testing
type MockCacheManager struct {
	mock.Mock
}

func (m *MockCacheManager) Get(ctx context.Context, modelID string) (*clickhouse.ExternalModelCache, error) {
	args := m.Called(ctx, modelID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*clickhouse.ExternalModelCache), args.Error(1)
}

func (m *MockCacheManager) Set(ctx context.Context, cache clickhouse.ExternalModelCache) error {
	args := m.Called(ctx, cache)
	return args.Error(0)
}

func (m *MockCacheManager) Invalidate(ctx context.Context, modelID string) error {
	args := m.Called(ctx, modelID)
	return args.Error(0)
}

func TestGetMinMax_WithCache(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name        string
		modelConfig models.ModelConfig
		cachedData  *clickhouse.ExternalModelCache
		cacheError  error
		queryResult struct {
			MinPos uint64
			MaxPos uint64
		}
		shouldQueryDB bool
		expectedMin   uint64
		expectedMax   uint64
		description   string
	}{
		{
			name: "cache hit with valid TTL",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				TTL:       60 * time.Second,
				Lag:       0,
			},
			cachedData: &clickhouse.ExternalModelCache{
				ModelID:   "test_db.test_table",
				Min:       1000,
				Max:       2000,
				UpdatedAt: time.Now(),
				TTL:       60 * time.Second,
			},
			shouldQueryDB: false,
			expectedMin:   1000,
			expectedMax:   2000,
			description:   "Should use cached values when cache is valid",
		},
		{
			name: "cache hit with lag applied",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				TTL:       60 * time.Second,
				Lag:       30,
			},
			cachedData: &clickhouse.ExternalModelCache{
				ModelID:   "test_db.test_table",
				Min:       1000,
				Max:       2000,
				UpdatedAt: time.Now(),
				TTL:       60 * time.Second,
			},
			shouldQueryDB: false,
			expectedMin:   1000,
			expectedMax:   1970, // 2000 - 30
			description:   "Should apply lag to cached max value",
		},
		{
			name: "cache miss - no cached data",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				TTL:       60 * time.Second,
				Lag:       0,
			},
			cachedData: nil,
			queryResult: struct {
				MinPos uint64
				MaxPos uint64
			}{
				MinPos: 1500,
				MaxPos: 2500,
			},
			shouldQueryDB: true,
			expectedMin:   1500,
			expectedMax:   2500,
			description:   "Should query DB when cache miss",
		},
		{
			name: "no TTL configured - skip cache",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				TTL:       0, // No TTL
				Lag:       0,
			},
			queryResult: struct {
				MinPos uint64
				MaxPos uint64
			}{
				MinPos: 1500,
				MaxPos: 2500,
			},
			shouldQueryDB: true,
			expectedMin:   1500,
			expectedMax:   2500,
			description:   "Should skip cache when TTL is 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			mockClient := new(MockClickHouseClient)
			mockCache := new(MockCacheManager)

			// Create executor with cache
			executor := NewExternalModelExecutor(mockClient, logger, mockCache)

			modelID := "test_db.test_table"

			// Setup cache expectations
			if tt.modelConfig.TTL > 0 {
				mockCache.On("Get", ctx, modelID).Return(tt.cachedData, tt.cacheError)

				if tt.shouldQueryDB {
					// Expect cache set after query
					mockCache.On("Set", ctx, mock.MatchedBy(func(cache clickhouse.ExternalModelCache) bool {
						return cache.ModelID == modelID &&
							cache.Min == tt.queryResult.MinPos &&
							cache.Max == tt.queryResult.MaxPos &&
							cache.TTL == tt.modelConfig.TTL
					})).Return(nil)
				}
			}

			// Setup database query if needed
			if tt.shouldQueryDB {
				mockClient.On("QueryOne", ctx, mock.Anything, mock.Anything).Return(
					func(dest interface{}) {
						if result, ok := dest.(*struct {
							MinPos uint64 `json:"min_pos"`
							MaxPos uint64 `json:"max_pos"`
						}); ok {
							result.MinPos = tt.queryResult.MinPos
							result.MaxPos = tt.queryResult.MaxPos
						}
					},
					nil,
				)
			}

			// Execute GetMinMax
			minPos, maxPos, err := executor.GetMinMax(ctx, &tt.modelConfig)

			// Assertions
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMin, minPos, tt.description+" (min)")
			assert.Equal(t, tt.expectedMax, maxPos, tt.description+" (max)")

			// Verify mock expectations
			mockClient.AssertExpectations(t)
			mockCache.AssertExpectations(t)
		})
	}
}

func TestInvalidateCache(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	tests := []struct {
		name        string
		modelID     string
		cacheError  error
		expectError bool
	}{
		{
			name:        "successful invalidation",
			modelID:     "test_db.test_table",
			cacheError:  nil,
			expectError: false,
		},
		{
			name:        "invalidation error",
			modelID:     "test_db.test_table",
			cacheError:  assert.AnError,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockClickHouseClient)
			mockCache := new(MockCacheManager)

			executor := NewExternalModelExecutor(mockClient, logger, mockCache)

			// Setup cache invalidation expectation
			mockCache.On("Invalidate", ctx, tt.modelID).Return(tt.cacheError)

			// Execute invalidation
			err := executor.InvalidateCache(ctx, tt.modelID)

			// Assertions
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "failed to invalidate cache")
			} else {
				assert.NoError(t, err)
			}

			mockCache.AssertExpectations(t)
		})
	}
}

func TestInvalidateCacheForModel(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	mockClient := new(MockClickHouseClient)
	mockCache := new(MockCacheManager)

	executor := NewExternalModelExecutor(mockClient, logger, mockCache)

	modelConfig := models.ModelConfig{
		Database: "test_db",
		Table:    "test_table",
	}

	expectedModelID := "test_db.test_table"

	// Setup expectation
	mockCache.On("Invalidate", ctx, expectedModelID).Return(nil)

	// Execute
	err := executor.InvalidateCacheForModel(ctx, &modelConfig)

	// Assert
	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
}

func TestGetMinMax_NoCacheManager(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	mockClient := new(MockClickHouseClient)

	// Create executor WITHOUT cache manager
	executor := NewExternalModelExecutor(mockClient, logger, nil)

	modelConfig := models.ModelConfig{
		Database:  "test_db",
		Table:     "test_table",
		Partition: "timestamp",
		External:  true,
		TTL:       60 * time.Second, // TTL configured but no cache manager
	}

	// Setup database query expectation
	mockClient.On("QueryOne", ctx, mock.Anything, mock.Anything).Return(
		func(dest interface{}) {
			if result, ok := dest.(*struct {
				MinPos uint64 `json:"min_pos"`
				MaxPos uint64 `json:"max_pos"`
			}); ok {
				result.MinPos = 1000
				result.MaxPos = 2000
			}
		},
		nil,
	)

	// Execute
	minPos, maxPos, err := executor.GetMinMax(ctx, &modelConfig)

	// Should work without cache manager
	assert.NoError(t, err)
	assert.Equal(t, uint64(1000), minPos)
	assert.Equal(t, uint64(2000), maxPos)

	// Test invalidation without cache manager (should be no-op)
	err = executor.InvalidateCache(ctx, "test_db.test_table")
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}
