package validation

import (
	"context"
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var errTestConnectionFailed = errors.New("connection failed")

// MockClickHouseClient for testing
type MockClickHouseClient struct {
	mock.Mock
}

func (m *MockClickHouseClient) Execute(ctx context.Context, query string) error {
	args := m.Called(ctx, query)
	return args.Error(0)
}

func (m *MockClickHouseClient) QueryOne(ctx context.Context, query string, dest interface{}) error {
	args := m.Called(ctx, query, dest)
	// If mock provides a result function, use it to populate dest
	if fn := args.Get(0); fn != nil {
		if populateFn, ok := fn.(func(interface{})); ok {
			populateFn(dest)
		}
	}
	return args.Error(1)
}

func (m *MockClickHouseClient) QueryMany(ctx context.Context, query string, dest interface{}) error {
	args := m.Called(ctx, query, dest)
	return args.Error(0)
}

func (m *MockClickHouseClient) BulkInsert(ctx context.Context, table string, data interface{}) error {
	args := m.Called(ctx, table, data)
	return args.Error(0)
}

func (m *MockClickHouseClient) IsStorageEmpty(ctx context.Context, table string, conditions map[string]interface{}) (bool, error) {
	args := m.Called(ctx, table, conditions)
	return args.Bool(0), args.Error(1)
}

func (m *MockClickHouseClient) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseClient) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClickHouseClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestGetMinMax_WithLag(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name        string
		modelConfig models.ModelConfig
		queryResult struct {
			MinPos uint64
			MaxPos uint64
		}
		expectedMin uint64
		expectedMax uint64
		description string
	}{
		{
			name: "external model with lag applied",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				Lag:       30,
			},
			queryResult: struct {
				MinPos uint64
				MaxPos uint64
			}{
				MinPos: 1000,
				MaxPos: 2000,
			},
			expectedMin: 1000,
			expectedMax: 1970, // 2000 - 30
			description: "Should subtract lag from max position",
		},
		{
			name: "external model without lag",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				Lag:       0,
			},
			queryResult: struct {
				MinPos uint64
				MaxPos uint64
			}{
				MinPos: 1000,
				MaxPos: 2000,
			},
			expectedMin: 1000,
			expectedMax: 2000,
			description: "Should return original max when lag is 0",
		},
		{
			name: "external model with lag exceeding max",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  true,
				Lag:       3000,
			},
			queryResult: struct {
				MinPos uint64
				MaxPos uint64
			}{
				MinPos: 1000,
				MaxPos: 2000,
			},
			expectedMin: 1000,
			expectedMax: 1000, // Should set to min when lag > max
			description: "Should set max to min when lag exceeds max",
		},
		{
			name: "transformation model with lag ignored",
			modelConfig: models.ModelConfig{
				Database:  "test_db",
				Table:     "test_table",
				Partition: "timestamp",
				External:  false,
				Lag:       30, // Should be ignored for non-external
			},
			queryResult: struct {
				MinPos uint64
				MaxPos uint64
			}{
				MinPos: 1000,
				MaxPos: 2000,
			},
			expectedMin: 1000,
			expectedMax: 2000,
			description: "Should ignore lag for non-external models",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock
			mockClient := new(MockClickHouseClient)

			// Setup expected query
			expectedQuery := mock.MatchedBy(func(_ string) bool {
				return true // Accept any query for simplicity
			})

			// Mock the query response
			mockClient.On("QueryOne", ctx, expectedQuery, mock.Anything).Return(
				func(dest interface{}) {
					// Populate the result struct
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

			// Create executor without cache for these tests
			executor := NewExternalModelExecutor(mockClient, logger, nil)

			// Execute GetMinMax
			minPos, maxPos, err := executor.GetMinMax(ctx, &tt.modelConfig)

			// Assertions
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMin, minPos, tt.description+" (min)")
			assert.Equal(t, tt.expectedMax, maxPos, tt.description+" (max)")

			// Verify mock expectations
			mockClient.AssertExpectations(t)
		})
	}
}

func TestGetMinMax_QueryError(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	mockClient := new(MockClickHouseClient)

	modelConfig := models.ModelConfig{
		Database:  "test_db",
		Table:     "test_table",
		Partition: "timestamp",
		External:  true,
		Lag:       30,
	}

	// Mock query error
	mockClient.On("QueryOne", ctx, mock.Anything, mock.Anything).Return(
		nil,
		errTestConnectionFailed,
	)

	executor := NewExternalModelExecutor(mockClient, logger, nil)

	minPos, maxPos, err := executor.GetMinMax(ctx, &modelConfig)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get min/max")
	assert.Equal(t, uint64(0), minPos)
	assert.Equal(t, uint64(0), maxPos)

	mockClient.AssertExpectations(t)
}
