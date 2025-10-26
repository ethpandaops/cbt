package validation

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Simple mock for testing query parsing
type testClickHouseClient struct {
	expectedQuery string
	mockResponse  string
}

func (m *testClickHouseClient) QueryOne(_ context.Context, query string, dest interface{}) error {
	// Check that the query was cleaned properly
	if query != m.expectedQuery {
		return assert.AnError
	}
	// Unmarshal mock response into dest
	return json.Unmarshal([]byte(m.mockResponse), dest)
}

func (m *testClickHouseClient) QueryMany(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *testClickHouseClient) Execute(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

func (m *testClickHouseClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *testClickHouseClient) Start() error {
	return nil
}

func (m *testClickHouseClient) Stop() error {
	return nil
}

// Simple mock admin service
type testAdminService struct {
	externalBounds *admin.BoundsCache
}

func (m *testAdminService) GetLastProcessedEndPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *testAdminService) GetNextUnprocessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *testAdminService) GetLastProcessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *testAdminService) GetFirstPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *testAdminService) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return nil
}

func (m *testAdminService) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return false, nil
}

func (m *testAdminService) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return nil, nil
}

func (m *testAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *testAdminService) GetExternalBounds(_ context.Context, _ string) (*admin.BoundsCache, error) {
	return nil, nil
}

func (m *testAdminService) SetExternalBounds(_ context.Context, _ *admin.BoundsCache) error {
	return nil
}

func (m *testAdminService) GetIncrementalAdminDatabase() string {
	return "admin"
}

func (m *testAdminService) GetIncrementalAdminTable() string {
	return "cbt"
}

func (m *testAdminService) GetScheduledAdminDatabase() string {
	return "admin"
}

func (m *testAdminService) GetScheduledAdminTable() string {
	return "cbt_scheduled"
}

func (m *testAdminService) RecordScheduledCompletion(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (m *testAdminService) GetLastScheduledExecution(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (m *testAdminService) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}

// Simple mock models service
type testModelsService struct {
	renderedQuery string
}

func (m *testModelsService) GetDAG() interface{} {
	return nil
}

func (m *testModelsService) Start() error {
	return nil
}

func (m *testModelsService) Stop() error {
	return nil
}

func (m *testModelsService) RenderTransformation(_ interface{}, _, _ uint64, _ time.Time) (string, error) {
	return "", nil
}

func (m *testModelsService) RenderExternal(_ interface{}) (string, error) {
	return m.renderedQuery, nil
}

// Adapter to make testModelsService work with the actual models.Service interface
type modelServiceAdapter struct {
	testModels *testModelsService
}

func (a *modelServiceAdapter) RenderExternal(model models.External, _ map[string]interface{}) (string, error) {
	return a.testModels.RenderExternal(model)
}

func (a *modelServiceAdapter) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time) (string, error) {
	return "", nil
}

func (a *modelServiceAdapter) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	return nil, nil
}

func (a *modelServiceAdapter) GetDAG() models.DAGReader {
	if dag := a.testModels.GetDAG(); dag != nil {
		return dag.(models.DAGReader)
	}
	return nil
}

func (a *modelServiceAdapter) Start() error {
	return a.testModels.Start()
}

func (a *modelServiceAdapter) Stop() error {
	return a.testModels.Stop()
}

func (m *testModelsService) GetTransformationEnvironmentVariables(_ interface{}, _, _ uint64, _ time.Time) (*[]string, error) {
	return nil, nil
}

// Simple mock external model
type testExternalModel struct {
	config external.Config
}

func (m *testExternalModel) GetID() string {
	return m.config.Database + "." + m.config.Table
}

func (m *testExternalModel) GetConfig() external.Config {
	return m.config
}

func (m *testExternalModel) GetType() string {
	return string(external.ExternalTypeSQL)
}

func (m *testExternalModel) GetValue() string {
	return ""
}

// Adapter to make testExternalModel work with the models.External interface
type externalModelAdapter struct {
	testModel *testExternalModel
}

func (a *externalModelAdapter) GetID() string {
	return a.testModel.GetID()
}

func (a *externalModelAdapter) GetConfig() external.Config {
	return a.testModel.GetConfig()
}

func (a *externalModelAdapter) GetConfigMutable() *external.Config {
	return &a.testModel.config
}

func (a *externalModelAdapter) GetType() string {
	return a.testModel.GetType()
}

func (a *externalModelAdapter) GetValue() string {
	return a.testModel.GetValue()
}

func (a *externalModelAdapter) SetDefaultDatabase(defaultDB string) {
	// Update the underlying config if needed
	config := a.testModel.GetConfig()
	if config.Database == "" {
		config.Database = defaultDB
		a.testModel.config = config
	}
}

func (a *externalModelAdapter) SetDefaults(_, defaultDB string) {
	config := a.testModel.GetConfig()
	if config.Database == "" && defaultDB != "" {
		config.Database = defaultDB
		a.testModel.config = config
	}
}

func TestQueryParsing(t *testing.T) {
	tests := []struct {
		name          string
		inputQuery    string
		expectedQuery string
		mockResponse  string
		expectError   bool
	}{
		{
			name: "simple query without semicolon",
			inputQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL`,
			mockResponse: `{"min": 1000, "max": 2000}`,
			expectError:  false,
		},
		{
			name: "query with semicolon",
			inputQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL;`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL`,
			mockResponse: `{"min": 1000, "max": 2000}`,
			expectError:  false,
		},
		{
			name: "query with semicolon and newlines",
			inputQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL;

`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL`,
			mockResponse: `{"min": 1000, "max": 2000}`,
			expectError:  false,
		},
		{
			name: "query with multiple statements",
			inputQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL;

-- This should be ignored
SELECT * FROM other_table;`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL`,
			mockResponse: `{"min": 1000, "max": 2000}`,
			expectError:  false,
		},
		{
			name: "query with semicolon in string literal",
			inputQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks 
WHERE comment != 'test;value' FINAL;`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks 
WHERE comment != 'test`,
			mockResponse: `{"min": 1000, "max": 2000}`,
			expectError:  false,
		},
		{
			name: "query with whitespace and comments",
			inputQuery: `  SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL  ;  
-- Comment after semicolon
`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks FINAL`,
			mockResponse: `{"min": 1000, "max": 2000}`,
			expectError:  false,
		},
		{
			name: "query returning null values (no data)",
			inputQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks 
WHERE 1=0;`,
			expectedQuery: `SELECT 
    min(block_number) as min,
    max(block_number) as max
FROM ethereum.blocks 
WHERE 1=0`,
			mockResponse: `{"min": null, "max": null}`,
			expectError:  true, // Should error on null values
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup simple mocks
			mockCH := &testClickHouseClient{
				expectedQuery: tt.expectedQuery,
				mockResponse:  tt.mockResponse,
			}

			// Create nil cache manager for testing (cache miss scenario)
			mockAdmin := &testAdminService{
				externalBounds: nil,
			}

			mockModels := &testModelsService{
				renderedQuery: tt.inputQuery,
			}

			// Configure the external model
			externalModel := &testExternalModel{
				config: external.Config{
					Database: "ethereum",
					Table:    "blocks",
					Cache: &external.CacheConfig{
						IncrementalScanInterval: 10 * time.Second,
						FullScanInterval:        5 * time.Minute,
					},
					Lag: 0,
				},
			}

			// Create validator and test
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			// Create validator with mocks - type conversion happens inside NewExternalModelExecutor
			validator := &ExternalModelValidator{
				log:      logger,
				admin:    mockAdmin,
				chClient: mockCH,
				models: &modelServiceAdapter{
					testModels: mockModels,
				},
			}

			ctx := context.Background()
			minPos, maxPos, err := validator.GetMinMax(ctx, &externalModelAdapter{
				testModel: externalModel,
			})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, uint64(1000), minPos)
				assert.Equal(t, uint64(2000), maxPos)
			}
		})
	}
}

func TestFlexUint64(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    uint64
		expectError bool
	}{
		{
			name:        "numeric value",
			input:       "12345",
			expected:    12345,
			expectError: false,
		},
		{
			name:        "string value",
			input:       `"67890"`,
			expected:    67890,
			expectError: false,
		},
		{
			name:        "large numeric value",
			input:       "18446744073709551615", // max uint64
			expected:    18446744073709551615,
			expectError: false,
		},
		{
			name:        "invalid string",
			input:       `"not_a_number"`,
			expected:    0,
			expectError: true,
		},
		{
			name:        "negative number",
			input:       "-100",
			expected:    0,
			expectError: true,
		},
		{
			name:        "float value",
			input:       "123.45",
			expected:    0,
			expectError: true,
		},
		{
			name:        "null value",
			input:       "null",
			expected:    0,
			expectError: true, // We explicitly reject null values as they likely indicate missing data
		},
		{
			name:        "empty string",
			input:       `""`,
			expected:    0,
			expectError: true, // Empty strings can't be parsed as uint64
		},
		{
			name:        "boolean value",
			input:       "true",
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f FlexUint64
			err := json.Unmarshal([]byte(tt.input), &f)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, uint64(f))
			}
		})
	}
}

func TestApplyLag(t *testing.T) {
	tests := []struct {
		name        string
		lag         uint64
		minPos      uint64
		maxPos      uint64
		expectedMin uint64
		expectedMax uint64
	}{
		{
			name:        "no lag",
			lag:         0,
			minPos:      1000,
			maxPos:      2000,
			expectedMin: 1000,
			expectedMax: 2000,
		},
		{
			name:        "lag smaller than max",
			lag:         100,
			minPos:      1000,
			maxPos:      2000,
			expectedMin: 1000,
			expectedMax: 1900,
		},
		{
			name:        "lag equals max",
			lag:         2000,
			minPos:      1000,
			maxPos:      2000,
			expectedMin: 1000,
			expectedMax: 1000, // Set to min when lag exceeds max
		},
		{
			name:        "lag exceeds max",
			lag:         3000,
			minPos:      1000,
			maxPos:      2000,
			expectedMin: 1000,
			expectedMax: 1000, // Set to min when lag exceeds max
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			externalModel := &testExternalModel{
				config: external.Config{
					Database: "test",
					Table:    "test",
					Lag:      tt.lag,
				},
			}

			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)
			validator := &ExternalModelValidator{
				log: logger,
			}

			minPos, maxPos := validator.applyLag(&externalModelAdapter{
				testModel: externalModel,
			}, tt.minPos, tt.maxPos, false)
			assert.Equal(t, tt.expectedMin, minPos)
			assert.Equal(t, tt.expectedMax, maxPos)
		})
	}
}
