package validation

import (
	"context"
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	// errConnectionFailed is used for testing connection failures
	errConnectionFailed = errors.New("connection error")
)

// MockAdminTableManager is a mock implementation of core.AdminTableManager
type MockAdminTableManager struct {
	mock.Mock
}

func (m *MockAdminTableManager) RecordCompletion(ctx context.Context, modelID string, position, interval uint64) error {
	args := m.Called(ctx, modelID, position, interval)
	return args.Error(0)
}

func (m *MockAdminTableManager) GetFirstPosition(ctx context.Context, modelID string) (uint64, error) {
	args := m.Called(ctx, modelID)
	if args.Get(0) == nil {
		return 0, args.Error(1)
	}
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockAdminTableManager) GetLastPosition(ctx context.Context, modelID string) (uint64, error) {
	args := m.Called(ctx, modelID)
	if args.Get(0) == nil {
		return 0, args.Error(1)
	}
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockAdminTableManager) GetCoverage(ctx context.Context, modelID string, startPos, endPos uint64) (bool, error) {
	args := m.Called(ctx, modelID, startPos, endPos)
	return args.Bool(0), args.Error(1)
}

func (m *MockAdminTableManager) DeleteRange(ctx context.Context, modelID string, startPos, endPos uint64) error {
	args := m.Called(ctx, modelID, startPos, endPos)
	return args.Error(0)
}

func (m *MockAdminTableManager) FindGaps(ctx context.Context, modelID string, minPos, maxPos, interval uint64) ([]clickhouse.GapInfo, error) {
	args := m.Called(ctx, modelID, minPos, maxPos, interval)
	if gaps := args.Get(0); gaps != nil {
		return gaps.([]clickhouse.GapInfo), args.Error(1)
	}
	return nil, args.Error(1)
}

// MockExternalModelExecutor is a mock implementation of ExternalModelExecutor
type MockExternalModelExecutor struct {
	mock.Mock
}

func (m *MockExternalModelExecutor) GetMinMax(ctx context.Context, modelConfig *models.ModelConfig) (minPos, maxPos uint64, err error) {
	args := m.Called(ctx, modelConfig)
	return args.Get(0).(uint64), args.Get(1).(uint64), args.Error(2)
}

func (m *MockExternalModelExecutor) HasDataInRange(ctx context.Context, modelConfig *models.ModelConfig, startPos, endPos uint64) (bool, error) {
	args := m.Called(ctx, modelConfig, startPos, endPos)
	return args.Bool(0), args.Error(1)
}

// Test setup helper that builds dependency graph with models
func setupValidatorWithModels(modelList []models.ModelConfig) (*DependencyValidatorImpl, *MockAdminTableManager, *MockExternalModelExecutor) {
	adminMgr := new(MockAdminTableManager)
	externalMgr := new(MockExternalModelExecutor)
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create dependency manager and build graph
	depManager := dependencies.NewDependencyGraph()
	if err := depManager.BuildGraph(modelList); err != nil {
		panic(err) // Test setup should not fail
	}

	validator := NewDependencyValidator(adminMgr, externalMgr, depManager, logger)
	return validator, adminMgr, externalMgr
}

// Scenario 1: Successful Validation ✅
func TestValidateDependencies_Success(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "beacon_blocks",
			Partition: "slot_start_date_time",
			External:  true,
		},
		{
			Database:  "analytics",
			Table:     "processed_blocks",
			Partition: "time",
			External:  false,
			Interval:  3600,
		},
		{
			Database:     "analytics",
			Table:        "block_aggregation",
			Partition:    "time",
			External:     false,
			Interval:     3600,
			Dependencies: []string{"ethereum.beacon_blocks", "analytics.processed_blocks"},
		},
	}

	validator, adminMgr, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Test validation at position 18000 with interval 3600
	position := uint64(18000)
	interval := uint64(3600)

	// Mock external model validation - beacon_blocks has data
	beaconConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "beacon_blocks",
		Partition: "slot_start_date_time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, beaconConfig).Return(uint64(0), uint64(25200), nil)
	externalMgr.On("HasDataInRange", ctx, beaconConfig, position, position+interval).Return(true, nil)

	// Mock transformation dependency - processed_blocks is covered
	adminMgr.On("GetCoverage", ctx, "analytics.processed_blocks", position, position+interval).Return(true, nil)

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.block_aggregation", position, interval)

	assert.NoError(t, err)
	assert.True(t, result.CanProcess, "Should be able to process when all dependencies are satisfied")
	assert.Len(t, result.Dependencies, 2)
	assert.Empty(t, result.Errors)
}

// Scenario 2: Failed Validation - Missing Dependency ❌
func TestValidateDependencies_MissingTransformationDependency(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "beacon_blocks",
			Partition: "slot_start_date_time",
			External:  true,
		},
		{
			Database:  "analytics",
			Table:     "processed_blocks",
			Partition: "time",
			External:  false,
			Interval:  3600,
		},
		{
			Database:     "analytics",
			Table:        "block_aggregation",
			Partition:    "time",
			External:     false,
			Interval:     3600,
			Dependencies: []string{"ethereum.beacon_blocks", "analytics.processed_blocks"},
		},
	}

	validator, adminMgr, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Test validation at position 14400 - trying to process ahead of processed_blocks
	position := uint64(14400)
	interval := uint64(3600)

	// Mock external model validation - beacon_blocks has data
	beaconConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "beacon_blocks",
		Partition: "slot_start_date_time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, beaconConfig).Return(uint64(0), uint64(25200), nil)
	externalMgr.On("HasDataInRange", ctx, beaconConfig, position, position+interval).Return(true, nil)

	// Mock transformation dependency - processed_blocks NOT covered (only up to 10800)
	adminMgr.On("GetCoverage", ctx, "analytics.processed_blocks", position, position+interval).Return(false, nil)

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.block_aggregation", position, interval)

	assert.NoError(t, err)
	assert.False(t, result.CanProcess, "Should not be able to process when transformation dependency is missing")
	assert.Len(t, result.Dependencies, 2)

	// Check that the unavailable dependency has an error in its status
	for _, dep := range result.Dependencies {
		if dep.ModelID == "analytics.processed_blocks" {
			assert.False(t, dep.Available, "processed_blocks should not be available")
			assert.NotNil(t, dep.Error, "unavailable dependency should have error in status")
		}
	}
}

// Scenario 3: Sparse External Data - Validation Failure ❌
func TestValidateDependencies_SparseExternalData(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "validator_performance",
			Partition: "time",
			External:  true,
		},
		{
			Database:     "analytics",
			Table:        "performance_aggregation",
			Partition:    "time",
			External:     false,
			Interval:     3600,
			Dependencies: []string{"ethereum.validator_performance"},
		},
	}

	validator, _, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Test validation at position 3600 - empty interval in sparse data
	position := uint64(3600)
	interval := uint64(3600)

	// Mock external model validation - data is sparse
	perfConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "validator_performance",
		Partition: "time",
		External:  true,
	}
	// Phase 1: Bounds check passes (min=0, max=25200)
	externalMgr.On("GetMinMax", ctx, perfConfig).Return(uint64(0), uint64(25200), nil)
	// Phase 2: No actual data in this range
	externalMgr.On("HasDataInRange", ctx, perfConfig, position, position+interval).Return(false, nil)

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.performance_aggregation", position, interval)

	assert.NoError(t, err)
	assert.False(t, result.CanProcess, "Should not be able to process when no data exists in range despite being within bounds")
	assert.Len(t, result.Dependencies, 1)
	assert.False(t, result.Dependencies[0].Available)
	assert.NotNil(t, result.Dependencies[0].Error)
	assert.Contains(t, result.Dependencies[0].Error.Error(), "no data in range")
}

// Scenario 4: Multiple Dependencies with Different Coverage
func TestValidateDependencies_MultipleDependencies(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "beacon_blocks",
			Partition: "time",
			External:  true,
		},
		{
			Database:  "ethereum",
			Table:     "validator_data",
			Partition: "time",
			External:  true,
		},
		{
			Database:  "analytics",
			Table:     "network_metrics",
			Partition: "time",
			External:  false,
			Interval:  3600,
		},
		{
			Database:  "analytics",
			Table:     "comprehensive_analysis",
			Partition: "time",
			External:  false,
			Interval:  3600,
			Dependencies: []string{
				"ethereum.beacon_blocks",
				"ethereum.validator_data",
				"analytics.network_metrics",
			},
		},
	}

	validator, adminMgr, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Test validation at position 7200
	position := uint64(7200)
	interval := uint64(3600)

	// Mock beacon_blocks - fully available
	beaconConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "beacon_blocks",
		Partition: "time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, beaconConfig).Return(uint64(0), uint64(25200), nil)
	externalMgr.On("HasDataInRange", ctx, beaconConfig, position, position+interval).Return(true, nil)

	// Mock validator_data - available up to 18000
	validatorConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "validator_data",
		Partition: "time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, validatorConfig).Return(uint64(0), uint64(18000), nil)
	externalMgr.On("HasDataInRange", ctx, validatorConfig, position, position+interval).Return(true, nil)

	// Mock network_metrics - NOT available (only up to 7200)
	adminMgr.On("GetCoverage", ctx, "analytics.network_metrics", position, position+interval).Return(false, nil)

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.comprehensive_analysis", position, interval)

	assert.NoError(t, err)
	assert.False(t, result.CanProcess, "Should not process when any dependency is missing")
	assert.Len(t, result.Dependencies, 3)

	// Check that beacon_blocks and validator_data are available
	for _, dep := range result.Dependencies {
		if dep.ModelID == "ethereum.beacon_blocks" || dep.ModelID == "ethereum.validator_data" {
			assert.True(t, dep.Available, "External dependencies with data should be available")
		}
		if dep.ModelID == "analytics.network_metrics" {
			assert.False(t, dep.Available, "network_metrics should not be available")
		}
	}
}

// Scenario 6: Initial Position Calculation
func TestGetInitialPosition(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "beacon_blocks",
			Partition: "time",
			External:  true,
		},
		{
			Database:  "ethereum",
			Table:     "validator_entities",
			Partition: "time",
			External:  true,
		},
		{
			Database:  "analytics",
			Table:     "network_stats",
			Partition: "time",
			External:  false,
			Interval:  3600,
		},
		{
			Database:  "analytics",
			Table:     "entity_analysis",
			Partition: "time",
			External:  false,
			Interval:  3600,
			Dependencies: []string{
				"ethereum.beacon_blocks",
				"ethereum.validator_entities",
				"analytics.network_stats",
			},
		},
	}

	validator, adminMgr, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Mock beacon_blocks starts at 0
	beaconConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "beacon_blocks",
		Partition: "time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, beaconConfig).Return(uint64(0), uint64(25200), nil)

	// Mock validator_entities starts at 7200
	validatorConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "validator_entities",
		Partition: "time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, validatorConfig).Return(uint64(7200), uint64(25200), nil)

	// Mock network_stats starts at 10800
	adminMgr.On("GetFirstPosition", ctx, "analytics.network_stats").Return(uint64(10800), nil)

	// Calculate initial position
	initialPos, err := validator.GetInitialPosition(ctx, "analytics.entity_analysis")

	assert.NoError(t, err)
	assert.Equal(t, uint64(10800), initialPos, "Should start at the maximum of all dependency minimums")
}

// Test initial position calculation with alignment
func TestGetInitialPosition_WithAlignment(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "source_data",
			Partition: "time",
			External:  true,
		},
		{
			Database:     "analytics",
			Table:        "aligned_model",
			Partition:    "time",
			External:     false,
			Interval:     3600, // 1 hour intervals
			Dependencies: []string{"ethereum.source_data"},
		},
	}

	validator, _, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Mock source starts at 7500 (not aligned to 3600)
	sourceConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "source_data",
		Partition: "time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, sourceConfig).Return(uint64(7500), uint64(25200), nil)

	// Calculate initial position
	initialPos, err := validator.GetInitialPosition(ctx, "analytics.aligned_model")

	assert.NoError(t, err)
	// 7500 / 3600 = 2.08... → rounds down to 2 → 2 * 3600 = 7200
	// But 7200 < 7500, so we round up to 10800
	assert.Equal(t, uint64(10800), initialPos, "Should align to interval boundary after dependency minimum")
}

// Test validation with no dependencies
func TestValidateDependencies_NoDependencies(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "analytics",
			Table:     "independent_model",
			Partition: "time",
			External:  false,
			Interval:  3600,
		},
	}

	validator, _, _ := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.independent_model", 0, 3600)

	assert.NoError(t, err)
	assert.True(t, result.CanProcess, "Model with no dependencies should always be able to process")
	assert.Empty(t, result.Dependencies)
	assert.Empty(t, result.Errors)
}

// Test validation with missing dependency model
func TestValidateDependencies_DependencyNotFound(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:     "analytics",
			Table:        "dependent_model",
			Partition:    "time",
			External:     false,
			Interval:     3600,
			Dependencies: []string{"missing.model"},
		},
	}

	// This should fail during BuildGraph
	adminMgr := new(MockAdminTableManager)
	externalMgr := new(MockExternalModelExecutor)
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	depManager := dependencies.NewDependencyGraph()
	err := depManager.BuildGraph(modelList)
	assert.Error(t, err, "Should fail to build graph with missing dependency")
	assert.Contains(t, err.Error(), "non-existent model")

	// Test with a valid graph but querying for non-existent model
	validModels := []models.ModelConfig{
		{
			Database:  "analytics",
			Table:     "valid_model",
			Partition: "time",
			External:  false,
			Interval:  3600,
		},
	}
	depManager = dependencies.NewDependencyGraph()
	err = depManager.BuildGraph(validModels)
	assert.NoError(t, err)

	validator := NewDependencyValidator(adminMgr, externalMgr, depManager, logger)
	ctx := context.Background()

	// Try to validate a non-existent model
	result, err := validator.ValidateDependencies(ctx, "non.existent", 0, 3600)
	assert.NoError(t, err)
	assert.False(t, result.CanProcess)
	assert.NotEmpty(t, result.Errors)
}

// Test external validation when completely outside bounds
func TestValidateDependencies_ExternalOutsideBounds(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "external_source",
			Partition: "time",
			External:  true,
		},
		{
			Database:     "analytics",
			Table:        "dependent",
			Partition:    "time",
			External:     false,
			Interval:     3600,
			Dependencies: []string{"ethereum.external_source"},
		},
	}

	validator, _, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	// Test validation at position 30000 - beyond max bounds
	position := uint64(30000)
	interval := uint64(3600)

	sourceConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "external_source",
		Partition: "time",
		External:  true,
	}
	// External data only available from 0 to 25200
	externalMgr.On("GetMinMax", ctx, sourceConfig).Return(uint64(0), uint64(25200), nil)
	// No need to check HasDataInRange since it's outside bounds

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.dependent", position, interval)

	assert.NoError(t, err)
	assert.False(t, result.CanProcess, "Should not process when position is outside external data bounds")
	assert.Len(t, result.Dependencies, 1)
	assert.False(t, result.Dependencies[0].Available)
	assert.Contains(t, result.Dependencies[0].Error.Error(), "required range")
}

// Test fallback behavior when HasDataInRange fails
func TestValidateDependencies_HasDataInRangeFallback(t *testing.T) {
	modelList := []models.ModelConfig{
		{
			Database:  "ethereum",
			Table:     "external_source",
			Partition: "time",
			External:  true,
		},
		{
			Database:     "analytics",
			Table:        "dependent",
			Partition:    "time",
			External:     false,
			Interval:     3600,
			Dependencies: []string{"ethereum.external_source"},
		},
	}

	validator, _, externalMgr := setupValidatorWithModels(modelList)
	ctx := context.Background()

	position := uint64(3600)
	interval := uint64(3600)

	sourceConfig := &models.ModelConfig{
		Database:  "ethereum",
		Table:     "external_source",
		Partition: "time",
		External:  true,
	}
	externalMgr.On("GetMinMax", ctx, sourceConfig).Return(uint64(0), uint64(7200), nil)
	// HasDataInRange returns error - should fallback to bounds check
	externalMgr.On("HasDataInRange", ctx, sourceConfig, position, position+interval).Return(false, errConnectionFailed)

	// Validate
	result, err := validator.ValidateDependencies(ctx, "analytics.dependent", position, interval)

	assert.NoError(t, err)
	// Should use bounds check as fallback: 3600-7200 is within 0-7200
	assert.True(t, result.CanProcess, "Should fallback to bounds check when HasDataInRange fails")
}
