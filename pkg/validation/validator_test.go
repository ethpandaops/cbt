package validation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors
var (
	errNodeNotFound       = errors.New("node not found")
	errNotATransformation = errors.New("not a transformation")
	errNotAnExternalNode  = errors.New("not an external node")
)

// Table-driven tests following ethPandaOps standards
func TestValidateDependencies(t *testing.T) {
	// Test table structure
	tests := []struct {
		name           string
		modelID        string
		position       uint64
		interval       uint64
		setupMocks     func(*mockDAGReader, *mockAdmin)
		expectedResult Result
		expectedError  error
		wantErr        bool
	}{
		{
			name:     "no dependencies - should not allow processing (no valid range)",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.dependencies = []string{}
				dag.nodes["model.test"] = models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}}
			},
			expectedResult: Result{
				CanProcess: false, // No dependencies means no valid range
			},
			wantErr: false,
		},
		{
			name:     "all dependencies satisfied",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1", "dep.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				// Set up data for dependencies
				admin.firstPositions = map[string]uint64{
					"dep.model1": 500,
					"dep.model2": 600,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 2000,
					"dep.model2": 2000,
				}
			},
			expectedResult: Result{
				CanProcess: true,
			},
			wantErr: false,
		},
		{
			name:     "dependency not satisfied - position before data",
			modelID:  "model.test",
			position: 100,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
				}
				// Dependency has data starting from 1000
				admin.firstPositions = map[string]uint64{
					"dep.model1": 1000,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 2000,
				}
			},
			expectedResult: Result{
				CanProcess: false,
			},
			wantErr: false,
		},
		{
			name:     "dependency not found in DAG",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.dependencies = []string{"dep.missing"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
				}
			},
			expectedResult: Result{
				CanProcess: false,
			},
			wantErr: false,
		},
		{
			name:     "model not found in DAG",
			modelID:  "model.nonexistent",
			position: 1000,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{} // No nodes, model not found
			},
			expectedResult: Result{
				CanProcess: false,
			},
			wantErr: false,
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			ctx := context.Background()
			mockDAG := newMockDAGReader()
			mockAdmin := newMockAdmin()

			// Apply test-specific setup
			tt.setupMocks(mockDAG, mockAdmin)

			// Create validator
			validator := &dependencyValidator{
				log:   logrus.New().WithField("test", tt.name),
				dag:   mockDAG,
				admin: mockAdmin,
				externalManager: &ExternalModelValidator{
					admin: mockAdmin,
				},
			}

			// Execute
			result, err := validator.ValidateDependencies(ctx, tt.modelID, tt.position, tt.interval)

			// Assert
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedError != nil {
					assert.True(t, errors.Is(err, tt.expectedError),
						"expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult.CanProcess, result.CanProcess)
			}
		})
	}
}

// Test context cancellation (ethPandaOps requirement)
func TestValidateDependencies_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockDAG := newMockDAGReader()
	mockAdmin := newMockAdmin()

	// Setup a model with dependencies
	mockDAG.nodes["model.test"] = models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}}
	mockDAG.dependencies = []string{"dep.model1"}
	mockDAG.nodes["dep.model1"] = models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}}

	// Setup slow dependency check
	mockAdmin.slowOperation = true
	mockAdmin.operationDelay = 100 * time.Millisecond

	validator := &dependencyValidator{
		log:   logrus.New(),
		dag:   mockDAG,
		admin: mockAdmin,
		externalManager: &ExternalModelValidator{
			admin: mockAdmin,
		},
	}

	// Cancel context immediately
	cancel()

	// Should handle context cancellation gracefully
	result, err := validator.ValidateDependencies(ctx, "model.test", 1000, 100)
	// The validator should complete but with coverage check returning false due to context
	assert.NoError(t, err)             // No error returned
	assert.False(t, result.CanProcess) // Should not be able to process
}

// Test with race detector (run with: go test -race)
func TestValidateDependencies_Concurrent(t *testing.T) {
	ctx := context.Background()
	mockDAG := newMockDAGReader()
	mockAdmin := newMockAdmin()

	// Setup models in DAG for concurrent test
	for i := 0; i < 10; i++ {
		modelID := fmt.Sprintf("model.%d", i)
		mockDAG.nodes[modelID] = models.Node{
			NodeType: models.NodeTypeTransformation,
			Model:    &mockTransformation{id: modelID, interval: 100},
		}
	}
	mockAdmin.coverage = true

	validator := &dependencyValidator{
		log:   logrus.New(),
		dag:   mockDAG,
		admin: mockAdmin,
		externalManager: &ExternalModelValidator{
			admin: mockAdmin,
		},
	}

	// Run multiple validations concurrently
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			modelID := fmt.Sprintf("model.%d", id)
			_, err := validator.ValidateDependencies(ctx, modelID, uint64(id*100), 10)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// Test GetInitialPosition with table-driven tests
func TestGetInitialPosition(t *testing.T) {
	tests := []struct {
		name          string
		modelID       string
		setupMocks    func(*mockDAGReader, *mockAdmin)
		expectedPos   uint64
		expectedError error
		wantErr       bool
	}{
		{
			name:    "no dependencies returns 0",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.dependencies = []string{}
				dag.nodes["model.test"] = models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}}
			},
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:    "single dependency",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 5000,
				}
			},
			expectedPos: 4900, // lastPos minus one interval
			wantErr:     false,
		},
		{
			name:    "multiple dependencies - returns based on minimum",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1", "dep.model2", "dep.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
					"dep.model3": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model3", interval: 100}},
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 3000,
					"dep.model2": 5000,
					"dep.model3": 4000,
				}
			},
			expectedPos: 2900, // min of all deps minus one interval
			wantErr:     false,
		},
		{
			name:    "external dependency with limited range - starts at minPos",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// Simulates the real scenario where external data starts at a high position
				// and the range is too small for the interval
				dag.dependencies = []string{"ext.model1", "ext.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 500000}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
				}
				// External models with limited historical data (similar to real scenario)
				// Range: 1752623999 - 1752537611 = 86388 (less than interval of 500000)
				admin.firstPositions = map[string]uint64{
					"ext.model1": 1752537611,
					"ext.model2": 1752537611,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 1752623999,
					"ext.model2": 1752623999,
				}
			},
			expectedPos: 1752537611, // Should start at minPos when range < interval
			wantErr:     false,
		},
		{
			name:    "external dependency with sufficient range",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// Simulates scenario where external data starts at a high position
				// but has enough range for at least one interval
				dag.dependencies = []string{"ext.model1", "ext.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 500000}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
				}
				// External models with limited historical data but enough for interval
				admin.firstPositions = map[string]uint64{
					"ext.model1": 1752300000,
					"ext.model2": 1752300000,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 1752900000, // 600000 range (> interval)
					"ext.model2": 1752900000,
				}
			},
			expectedPos: 1752400000, // Should start one interval back from max (1752900000 minus 500000)
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockDAG := newMockDAGReader()
			mockAdmin := newMockAdmin()

			tt.setupMocks(mockDAG, mockAdmin)

			validator := &dependencyValidator{
				log:   logrus.New(),
				dag:   mockDAG,
				admin: mockAdmin,
				externalManager: &mockExternalModelValidator{
					admin: mockAdmin,
				},
			}

			pos, err := validator.GetInitialPosition(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// Benchmark tests (ethPandaOps performance requirement)
func BenchmarkValidateDependencies(b *testing.B) {
	ctx := context.Background()
	mockDAG := newMockDAGReader()
	mockAdmin := newMockAdmin()

	// Setup complex dependency tree
	mockDAG.dependencies = []string{"dep1", "dep2", "dep3", "dep4", "dep5"}
	for i := 1; i <= 5; i++ {
		depID := fmt.Sprintf("dep%d", i)
		mockDAG.nodes[depID] = models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: depID, interval: 100}}
		mockAdmin.lastPositions[depID] = uint64(i * 1000)
	}

	validator := &dependencyValidator{
		log:   logrus.New(),
		dag:   mockDAG,
		admin: mockAdmin,
		externalManager: &ExternalModelValidator{
			admin: mockAdmin,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = validator.ValidateDependencies(ctx, "model.test", 100, 10)
	}
}

// Mock implementations for testing

type mockDAGReader struct {
	nodes        map[string]models.Node
	dependencies []string
	getNodeError error
}

func newMockDAGReader() *mockDAGReader {
	return &mockDAGReader{
		nodes:        make(map[string]models.Node),
		dependencies: []string{},
	}
}

func (m *mockDAGReader) GetNode(id string) (models.Node, error) {
	if m.getNodeError != nil {
		return models.Node{}, m.getNodeError
	}
	node, ok := m.nodes[id]
	if !ok {
		return models.Node{}, errNodeNotFound
	}
	return node, nil
}

func (m *mockDAGReader) GetDependencies(_ string) []string {
	return m.dependencies
}

func (m *mockDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	node, err := m.GetNode(id)
	if err != nil {
		return nil, err
	}
	if node.NodeType == models.NodeTypeTransformation {
		if trans, ok := node.Model.(models.Transformation); ok {
			return trans, nil
		}
	}
	return nil, errNotATransformation
}

// Other DAGReader methods...
func (m *mockDAGReader) GetExternalNode(id string) (models.External, error) {
	node, err := m.GetNode(id)
	if err != nil {
		return nil, err
	}
	if node.NodeType == models.NodeTypeExternal {
		if ext, ok := node.Model.(models.External); ok {
			return ext, nil
		}
	}
	return nil, errNotAnExternalNode
}
func (m *mockDAGReader) GetDependents(_ string) []string                 { return []string{} }
func (m *mockDAGReader) GetAllDependencies(_ string) []string            { return []string{} }
func (m *mockDAGReader) GetAllDependents(_ string) []string              { return []string{} }
func (m *mockDAGReader) GetTransformationNodes() []models.Transformation { return nil }
func (m *mockDAGReader) GetExternalNodes() []models.Node                 { return []models.Node{} }
func (m *mockDAGReader) IsPathBetween(_, _ string) bool                  { return false }

type mockAdmin struct {
	lastPositions  map[string]uint64
	firstPositions map[string]uint64
	coverage       bool
	slowOperation  bool
	operationDelay time.Duration
}

func newMockAdmin() *mockAdmin {
	return &mockAdmin{
		lastPositions:  make(map[string]uint64),
		firstPositions: make(map[string]uint64),
	}
}

func (m *mockAdmin) GetLastProcessedEndPosition(ctx context.Context, modelID string) (uint64, error) {
	if m.slowOperation {
		select {
		case <-time.After(m.operationDelay):
			// Operation completed
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	pos, ok := m.lastPositions[modelID]
	if !ok {
		return 0, nil
	}
	return pos, nil
}

func (m *mockAdmin) GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) {
	// For mock purposes, this is the same as GetLastProcessedEndPosition
	if m.slowOperation {
		select {
		case <-time.After(m.operationDelay):
			// Operation completed
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	pos, ok := m.lastPositions[modelID]
	if !ok {
		return 0, nil
	}
	return pos, nil
}

func (m *mockAdmin) GetLastProcessedPosition(_ context.Context, modelID string) (uint64, error) {
	// For mock purposes, return the last position if it exists
	pos, ok := m.lastPositions[modelID]
	if !ok {
		return 0, nil
	}
	return pos, nil
}

func (m *mockAdmin) GetFirstPosition(_ context.Context, modelID string) (uint64, error) {
	if pos, ok := m.firstPositions[modelID]; ok {
		return pos, nil
	}
	return 0, nil
}

func (m *mockAdmin) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return nil
}

func (m *mockAdmin) GetCoverage(ctx context.Context, _ string, _, _ uint64) (bool, error) {
	if m.slowOperation {
		select {
		case <-time.After(m.operationDelay):
			// Operation completed
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return m.coverage, nil
}

func (m *mockAdmin) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return []admin.GapInfo{}, nil
}

func (m *mockAdmin) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *mockAdmin) GetExternalBounds(_ context.Context, _ string) (*admin.BoundsCache, error) {
	return nil, nil
}
func (m *mockAdmin) SetExternalBounds(_ context.Context, _ *admin.BoundsCache) error {
	return nil
}

func (m *mockAdmin) GetAdminDatabase() string {
	return "admin_db"
}

func (m *mockAdmin) GetAdminTable() string {
	return "admin_table"
}

type mockTransformation struct {
	id       string
	interval uint64
}

func (m *mockTransformation) GetID() string { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config {
	if m.interval == 0 {
		m.interval = 100 // Default interval
	}
	return &transformation.Config{
		Database: "test_db",
		Table:    "test_table",
		Interval: &transformation.IntervalConfig{
			Max: m.interval,
			Min: 0,
		},
		Schedules: &transformation.SchedulesConfig{
			ForwardFill: "@every 1m",
		},
		Dependencies: []string{},
	}
}
func (m *mockTransformation) GetValue() string                  { return "" }
func (m *mockTransformation) GetDependencies() []string         { return []string{} }
func (m *mockTransformation) GetSQL() string                    { return "" }
func (m *mockTransformation) GetType() string                   { return "transformation" }
func (m *mockTransformation) GetEnvironmentVariables() []string { return []string{} }
func (m *mockTransformation) SetDefaultDatabase(_ string) {
	// No-op for mock
}

// Test NewDependencyValidator creation
func TestNewDependencyValidator(t *testing.T) {
	log := logrus.New()
	mockCH := &mockClickhouseClient{}
	mockAdmin := newMockAdmin()
	mockModels := &mockModelsService{
		dag: newMockDAGReader(),
	}

	validator := NewDependencyValidator(log, mockCH, mockAdmin, mockModels)
	assert.NotNil(t, validator)

	// Verify it implements the Validator interface
	var _ = validator
}

// Test GetEarliestPosition
func TestGetEarliestPosition(t *testing.T) {
	tests := []struct {
		name          string
		modelID       string
		setupMocks    func(*mockDAGReader, *mockAdmin)
		expectedPos   uint64
		expectedError error
		wantErr       bool
	}{
		{
			name:    "no dependencies returns 0",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.dependencies = []string{}
				dag.nodes["model.test"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.test", interval: 100},
				}
			},
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:    "single dependency returns its first position",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"dep.model1": 100,
				}
			},
			expectedPos: 100,
			wantErr:     false,
		},
		{
			name:    "only external dependencies - returns min of external mins",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "ext.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
					"ext.model3": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model3"}},
				}
				// External models return their bounds from GetMinMax
				admin.firstPositions = map[string]uint64{
					"ext.model1": 100,
					"ext.model2": 200,
					"ext.model3": 150,
				}
			},
			expectedPos: 100, // Minimum of external mins (can start from earliest external data)
			wantErr:     false,
		},
		{
			name:    "only transformation dependencies - returns max of transformation mins",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1", "dep.model2", "dep.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
					"dep.model3": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model3", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"dep.model1": 100,
					"dep.model2": 300,
					"dep.model3": 200,
				}
			},
			expectedPos: 300, // Maximum of transformation mins
			wantErr:     false,
		},
		{
			name:    "mixed dependencies - returns max(external_min, transformation_max)",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1": 50, // External min will be 50 (MIN of externals)
					"ext.model2": 100,
					"dep.model1": 200, // Transformation max will be 250
					"dep.model2": 250,
				}
			},
			expectedPos: 250, // Result: max of (external_min=50, transformation_max=250)
			wantErr:     false,
		},
		{
			name:    "mixed dependencies where external_min is higher",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1": 300, // External MIN will be 300 (can start from earliest)
					"ext.model2": 350,
					"dep.model1": 100, // Transformation max will be 150
					"dep.model2": 150,
				}
			},
			expectedPos: 300, // Result: max of (external_min=300, transformation_max=150)
			wantErr:     false,
		},
		{
			name:    "model not found returns 0",
			modelID: "model.nonexistent",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{}
				dag.dependencies = []string{} // No dependencies for non-existent model
			},
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:    "not a transformation model returns 0",
			modelID: "model.external",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{
					"model.external": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "model.external"}},
				}
				dag.dependencies = []string{} // No dependencies for external model
			},
			expectedPos: 0,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockDAG := newMockDAGReader()
			mockAdmin := newMockAdmin()

			tt.setupMocks(mockDAG, mockAdmin)

			validator := &dependencyValidator{
				log:   logrus.New(),
				dag:   mockDAG,
				admin: mockAdmin,
				externalManager: &mockExternalModelValidator{
					admin: mockAdmin,
				},
			}

			pos, err := validator.GetEarliestPosition(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// Mock external model for testing
type mockExternal struct {
	id string
}

func (m *mockExternal) GetID() string { return m.id }
func (m *mockExternal) GetConfig() external.Config {
	return external.Config{
		Database: "test",
		Table:    "test",
	}
}
func (m *mockExternal) GetValue() string { return "" }
func (m *mockExternal) GetType() string  { return "sql" }
func (m *mockExternal) SetDefaultDatabase(_ string) {
	// No-op for mock
}

// mockExternalModelValidator is a mock implementation for testing
type mockExternalModelValidator struct {
	admin *mockAdmin
}

// GetMinMax returns mock min/max values for external models from admin's firstPositions
func (m *mockExternalModelValidator) GetMinMax(_ context.Context, model models.External) (minPos, maxPos uint64, err error) {
	// Use firstPositions as min and lastPositions as max for testing
	minPos = m.admin.firstPositions[model.GetID()]
	maxPos = m.admin.lastPositions[model.GetID()]
	if maxPos == 0 {
		maxPos = minPos + 10000 // Default range for testing
	}
	return minPos, maxPos, nil
}

// Mock models service
type mockModelsService struct {
	dag models.DAGReader
}

func (m *mockModelsService) GetDAG() models.DAGReader { return m.dag }
func (m *mockModelsService) Start() error             { return nil }
func (m *mockModelsService) Stop() error              { return nil }
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

// Mock clickhouse client
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

// TestGetValidRange specifically tests the corrected validation formula
// min = MAX(MIN(external_mins), MAX(transformation_mins))
// max = MIN(all dependency maxes)
func TestGetValidRange(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		setupMocks  func(*mockDAGReader, *mockAdmin)
		expectedMin uint64
		expectedMax uint64
		wantErr     bool
	}{
		{
			name:    "no dependencies returns 0,0",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.dependencies = []string{}
				dag.nodes["model.test"] = models.Node{
					NodeType: models.NodeTypeTransformation,
					Model:    &mockTransformation{id: "model.test", interval: 100},
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     false,
		},
		{
			name:    "only external deps - min=MIN(externals), max=MIN(all maxes)",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "ext.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
					"ext.model3": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model3"}},
				}
				// External models: mins and maxes
				admin.firstPositions = map[string]uint64{
					"ext.model1": 100,
					"ext.model2": 200,
					"ext.model3": 150,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000,
					"ext.model2": 4000,
					"ext.model3": 4500,
				}
			},
			expectedMin: 100,  // Minimum of externals: 100, 200, 150 results in 100
			expectedMax: 4000, // Minimum of all maxes: 5000, 4000, 4500 results in 4000
			wantErr:     false,
		},
		{
			name:    "only transformation deps - min=MAX(transformations), max=MIN(all maxes)",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1", "dep.model2", "dep.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
					"dep.model3": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model3", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"dep.model1": 100,
					"dep.model2": 300,
					"dep.model3": 200,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 5000,
					"dep.model2": 4000,
					"dep.model3": 6000,
				}
			},
			expectedMin: 300,  // Maximum of transformations: 100, 300, 200 results in 300
			expectedMax: 4000, // Minimum of all maxes: 5000, 4000, 6000 results in 4000
			wantErr:     false,
		},
		{
			name:    "mixed deps - formula applied correctly",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1": 50, // External mins: MIN(50, 100) = 50
					"ext.model2": 100,
					"dep.model1": 200, // Transformation mins: MAX(200, 250) = 250
					"dep.model2": 250,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000, // All maxes: MIN(5000, 3000, 4000, 3500) = 3000
					"ext.model2": 3000,
					"dep.model1": 4000,
					"dep.model2": 3500,
				}
			},
			expectedMin: 250,  // MAX(MIN(externals)=50, MAX(transformations)=250) = 250
			expectedMax: 3000, // MIN(all maxes) = 3000
			wantErr:     false,
		},
		{
			name:    "max calculation uses MIN of ALL dependency maxes",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "dep.model1", "dep.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1": 100,
					"dep.model1": 100,
					"dep.model2": 100,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 2000, // External max
					"dep.model1": 5000, // Transformation max (higher)
					"dep.model2": 1500, // Transformation max (lowest - should be used)
				}
			},
			expectedMin: 100,  // MAX(MIN(externals)=100, MAX(transformations)=100) = 100
			expectedMax: 1500, // MIN(2000, 5000, 1500) = 1500 (uses MIN of ALL)
			wantErr:     false,
		},
		{
			name:    "model not found",
			modelID: "model.nonexistent",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
		},
		{
			name:    "not a transformation model",
			modelID: "model.external",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{
					"model.external": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "model.external"}},
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockDAG := newMockDAGReader()
			mockAdmin := newMockAdmin()

			tt.setupMocks(mockDAG, mockAdmin)

			validator := &dependencyValidator{
				log:   logrus.New(),
				dag:   mockDAG,
				admin: mockAdmin,
				externalManager: &mockExternalModelValidator{
					admin: mockAdmin,
				},
			}

			minPos, maxPos, err := validator.GetValidRange(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMin, minPos, "min position mismatch")
				assert.Equal(t, tt.expectedMax, maxPos, "max position mismatch")
			}
		})
	}
}
