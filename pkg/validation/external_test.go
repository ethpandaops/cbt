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

// Simple mock admin service
type testAdminService struct {
	externalBounds *admin.BoundsCache
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

func TestFlexUint64Scan(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expected    uint64
		expectError bool
	}{
		{
			name:        "int64 value",
			input:       int64(12345),
			expected:    12345,
			expectError: false,
		},
		{
			name:        "uint64 value",
			input:       uint64(67890),
			expected:    67890,
			expectError: false,
		},
		{
			name:        "int value",
			input:       int(111),
			expected:    111,
			expectError: false,
		},
		{
			name:        "int8 value",
			input:       int8(42),
			expected:    42,
			expectError: false,
		},
		{
			name:        "uint8 value",
			input:       uint8(255),
			expected:    255,
			expectError: false,
		},
		{
			name:        "int16 value",
			input:       int16(1000),
			expected:    1000,
			expectError: false,
		},
		{
			name:        "uint16 value",
			input:       uint16(65535),
			expected:    65535,
			expectError: false,
		},
		{
			name:        "int32 value",
			input:       int32(222),
			expected:    222,
			expectError: false,
		},
		{
			name:        "uint32 value",
			input:       uint32(333),
			expected:    333,
			expectError: false,
		},
		{
			name:        "float64 value",
			input:       float64(444),
			expected:    444,
			expectError: false,
		},
		{
			name:        "string value",
			input:       "12345",
			expected:    12345,
			expectError: false,
		},
		{
			name:        "[]byte value",
			input:       []byte("67890"),
			expected:    67890,
			expectError: false,
		},
		{
			name:        "nil value",
			input:       nil,
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid string",
			input:       "not_a_number",
			expected:    0,
			expectError: true,
		},
		{
			name:        "unsupported type",
			input:       true,
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f FlexUint64
			err := f.Scan(tt.input)

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
			}, tt.minPos, tt.maxPos)
			assert.Equal(t, tt.expectedMin, minPos)
			assert.Equal(t, tt.expectedMax, maxPos)
		})
	}
}
