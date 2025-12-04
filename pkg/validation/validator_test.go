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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.model1", "dep.model2"}}},
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
		{
			name:     "uninitialized transformation dependency - should fail with error",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.uninitialized"}
				dag.nodes = map[string]models.Node{
					"model.test":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.uninitialized"}}},
					"dep.uninitialized": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninitialized", interval: 100}},
				}
				// Uninitialized transformation has no data (0, 0)
				admin.firstPositions = map[string]uint64{
					"dep.uninitialized": 0,
				}
				admin.lastPositions = map[string]uint64{
					"dep.uninitialized": 0,
				}
			},
			expectedResult: Result{
				CanProcess: false,
			},
			wantErr: false, // GetValidRange returns error but ValidateDependencies handles it gracefully
		},
		{
			name:     "mixed dependencies - external initialized, transformation uninitialized",
			modelID:  "model.test",
			position: 1000,
			interval: 100,
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "dep.uninitialized"}
				dag.nodes = map[string]models.Node{
					"model.test":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "dep.uninitialized"}}},
					"ext.model1":        models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"dep.uninitialized": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninitialized", interval: 100}},
				}
				// External has data
				admin.firstPositions = map[string]uint64{
					"ext.model1": 500,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 2000,
				}
				// Transformation has no data (uninitialized)
				admin.firstPositions["dep.uninitialized"] = 0
				admin.lastPositions["dep.uninitialized"] = 0
			},
			expectedResult: Result{
				CanProcess: false,
			},
			wantErr: false,
		},
		{
			name:     "position below limits.min - returns NextValidPos to skip gap",
			modelID:  "model.test",
			position: 500,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// Model has processed up to 500, dependencies available [0-1000]
				// But limits.min=700 creates a gap [500-700] that should be skipped
				dag.dependencies = []string{"dep.model1"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model: &mockTransformation{
							id:       "model.test",
							interval: 50,
							handler: &mockHandler{
								interval:     50,
								dependencies: []string{"dep.model1"},
								limits: &transformation.Limits{
									Min: 700, // Configured minimum limit
									Max: 0,   // No max limit
								},
							},
						},
					},
					"dep.model1": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model:    &mockTransformation{id: "dep.model1", interval: 100},
					},
				}
				// Dependency has data from 0 to 1000
				admin.firstPositions = map[string]uint64{
					"dep.model1": 0,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 1000,
				}
			},
			expectedResult: Result{
				CanProcess:   false,
				NextValidPos: 700, // Should skip to limits.min
			},
			wantErr: false,
		},
		{
			name:     "position at limits.min boundary - can process",
			modelID:  "model.test",
			position: 700,
			interval: 50,
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model: &mockTransformation{
							id:       "model.test",
							interval: 50,
							handler: &mockHandler{
								interval:     50,
								dependencies: []string{"dep.model1"},
								limits: &transformation.Limits{
									Min: 700,
									Max: 0,
								},
							},
						},
					},
					"dep.model1": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model:    &mockTransformation{id: "dep.model1", interval: 100},
					},
				}
				admin.firstPositions = map[string]uint64{
					"dep.model1": 0,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 1000,
				}
			},
			expectedResult: Result{
				CanProcess:   true,
				NextValidPos: 0,
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
				externalManager: &mockExternalModelValidator{
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
				assert.Equal(t, tt.expectedResult.CanProcess, result.CanProcess, "CanProcess mismatch")
				assert.Equal(t, tt.expectedResult.NextValidPos, result.NextValidPos, "NextValidPos mismatch")
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
		externalManager: &mockExternalModelValidator{
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
		externalManager: &mockExternalModelValidator{
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.model1"}}},
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.model1", "dep.model2", "dep.model3"}}},
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 500000, dependencies: []string{"ext.model1", "ext.model2"}}},
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 500000, dependencies: []string{"ext.model1", "ext.model2"}}},
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

			pos, err := validator.GetStartPosition(ctx, tt.modelID)

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
		externalManager: &mockExternalModelValidator{
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

func (m *mockDAGReader) GetStructuredDependencies(_ string) []transformation.Dependency {
	// For tests, return nil (structured dependencies not needed in validation tests)
	return nil
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
	gaps           map[string][]admin.GapInfo
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

func (m *mockAdmin) GetNextUnprocessedPosition(ctx context.Context, modelID string) (uint64, error) {
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

func (m *mockAdmin) FindGaps(_ context.Context, modelID string, _, _, _ uint64) ([]admin.GapInfo, error) {
	if m.gaps != nil {
		if gaps, ok := m.gaps[modelID]; ok {
			return gaps, nil
		}
	}
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

func (m *mockAdmin) GetIncrementalAdminDatabase() string {
	return "admin_db"
}

func (m *mockAdmin) GetIncrementalAdminTable() string {
	return "admin_table"
}

func (m *mockAdmin) GetScheduledAdminDatabase() string {
	return "admin"
}

func (m *mockAdmin) GetScheduledAdminTable() string {
	return "cbt_scheduled"
}

func (m *mockAdmin) RecordScheduledCompletion(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (m *mockAdmin) GetLastScheduledExecution(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (m *mockAdmin) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}

// Mock handler for tests
type mockHandler struct {
	interval       uint64
	dependencies   []string
	orDependencies []transformation.Dependency
	limits         *transformation.Limits
}

// Mock scheduled handler for tests - simulates scheduled transformations
type mockScheduledHandler struct{}

func (h *mockScheduledHandler) Type() transformation.Type { return "scheduled" }
func (h *mockScheduledHandler) Config() any               { return nil }
func (h *mockScheduledHandler) Validate() error           { return nil }

// ShouldTrackPosition returns false for scheduled transformations
func (h *mockScheduledHandler) ShouldTrackPosition() bool { return false }
func (h *mockScheduledHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *mockScheduledHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (h *mockScheduledHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

func (h *mockHandler) GetMaxInterval() uint64 {
	if h.interval == 0 {
		return 100 // Default interval
	}
	return h.interval
}

func (h *mockHandler) GetFlattenedDependencies() []string {
	if len(h.orDependencies) > 0 {
		var result []string
		for _, dep := range h.orDependencies {
			result = append(result, dep.GetAllDependencies()...)
		}
		return result
	}
	return h.dependencies
}

func (h *mockHandler) GetDependencies() []transformation.Dependency {
	if len(h.orDependencies) > 0 {
		return h.orDependencies
	}
	// Convert string dependencies to Dependency structs
	deps := make([]transformation.Dependency, len(h.dependencies))
	for i, dep := range h.dependencies {
		deps[i] = transformation.Dependency{
			IsGroup:   false,
			SingleDep: dep,
		}
	}
	return deps
}

func (h *mockHandler) GetLimits() *transformation.Limits {
	return h.limits
}

// Other handler methods (not used in validation tests)
func (h *mockHandler) Type() transformation.Type { return "incremental" }
func (h *mockHandler) Config() any               { return nil }
func (h *mockHandler) Validate() error           { return nil }
func (h *mockHandler) ShouldTrackPosition() bool { return true }
func (h *mockHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *mockHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (h *mockHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

type mockTransformation struct {
	id             string
	handler        transformation.Handler
	interval       uint64
	dependencies   []string
	orDependencies []transformation.Dependency // Support for OR groups
}

func (m *mockTransformation) GetID() string { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config {
	return &transformation.Config{
		Database: "test_db",
		Table:    "test_table",
	}
}
func (m *mockTransformation) GetHandler() transformation.Handler {
	if m.handler != nil {
		return m.handler
	}
	// Create default handler from the mock's properties
	return &mockHandler{
		interval:       m.interval,
		dependencies:   m.dependencies,
		orDependencies: m.orDependencies,
	}
}
func (m *mockTransformation) GetValue() string { return "" }
func (m *mockTransformation) GetSQL() string   { return "" }
func (m *mockTransformation) GetType() string  { return "transformation" }
func (m *mockTransformation) SetDefaultDatabase(_ string) {
	// No-op for mock
}

// Test NewDependencyValidator creation
func TestNewDependencyValidator(t *testing.T) {
	log := logrus.New()
	mockAdminSvc := newMockAdmin()
	mockExtVal := &mockExternalModelValidator{}
	mockDAG := newMockDAGReader()

	validator := NewDependencyValidator(log, mockAdminSvc, mockExtVal, mockDAG)
	assert.NotNil(t, validator)
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.model1"}}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"dep.model1": 100,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 1000, // Add last position to show it's initialized
				}
			},
			expectedPos: 100,
			wantErr:     false,
		},
		{
			name:    "only external dependencies - returns max of external mins (intersection)",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "ext.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "ext.model2", "ext.model3"}}},
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
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000,
					"ext.model2": 5000,
					"ext.model3": 5000,
				}
			},
			expectedPos: 200, // Maximum of external mins (intersection where all have data)
			wantErr:     false,
		},
		{
			name:    "only transformation dependencies - returns max of transformation mins",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.model1", "dep.model2", "dep.model3"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.model1", "dep.model2", "dep.model3"}}},
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
					"dep.model1": 1000,
					"dep.model2": 1000,
					"dep.model3": 1000,
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}}},
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
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000,
					"ext.model2": 5000,
					"dep.model1": 2000,
					"dep.model2": 2000,
				}
			},
			expectedPos: 250, // Result: max of (external_min=50, transformation_max=250)
			wantErr:     false,
		},
		{
			name:    "mixed dependencies - returns max of all mins (intersection)",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}}},
					"ext.model1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1": 300,
					"ext.model2": 350, // MAX of all mins is 350
					"dep.model1": 100,
					"dep.model2": 150,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000,
					"ext.model2": 5000,
					"dep.model1": 2000,
					"dep.model2": 2000,
				}
			},
			expectedPos: 350, // Result: max of all mins (intersection where ALL deps have data)
			wantErr:     false,
		},
		{
			name:    "model not found returns error",
			modelID: "model.nonexistent",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{}
				dag.dependencies = []string{}
			},
			expectedPos: 0,
			wantErr:     true,
		},
		{
			name:    "not a transformation model returns error",
			modelID: "model.external",
			setupMocks: func(dag *mockDAGReader, _ *mockAdmin) {
				dag.nodes = map[string]models.Node{
					"model.external": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "model.external"}},
				}
				dag.dependencies = []string{}
			},
			expectedPos: 0,
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

			pos, _, err := validator.GetValidRange(ctx, tt.modelID, Intersection)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if pos != tt.expectedPos {
					logPositionDebugInfo(ctx, t, validator, mockDAG, tt.modelID, pos, tt.expectedPos)
				}
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// logPositionDebugInfo logs debug information when position doesn't match expected
func logPositionDebugInfo(ctx context.Context, t *testing.T, validator *dependencyValidator, mockDAG *mockDAGReader, modelID string, pos, expectedPos uint64) {
	t.Logf("GetValidRange(Intersection) returned min=%d, expected %d", pos, expectedPos)
	// Try GetValidRange directly to see what it returns
	minPos, maxPos, err2 := validator.GetValidRange(ctx, modelID, Union)
	t.Logf("GetValidRange(Union) returned min=%d, max=%d, err=%v", minPos, maxPos, err2)
	// Check what the model's dependencies are
	node, err3 := mockDAG.GetNode(modelID)
	if err3 != nil {
		return
	}
	trans, ok := node.Model.(models.Transformation)
	if !ok {
		return
	}
	handler := trans.GetHandler()
	if handler == nil {
		return
	}
	depProvider, ok := handler.(interface {
		GetDependencies() []transformation.Dependency
	})
	if !ok {
		return
	}
	deps := depProvider.GetDependencies()
	t.Logf("Model dependencies count: %d", len(deps))
	for i, dep := range deps {
		if dep.IsGroup {
			t.Logf("  Dep[%d] (group): %v", i, dep.GroupDeps)
		} else {
			t.Logf("  Dep[%d]: %s", i, dep.SingleDep)
		}
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
func (m *mockExternal) GetConfigMutable() *external.Config {
	config := external.Config{
		Database: "test",
		Table:    "test",
	}
	return &config
}
func (m *mockExternal) GetValue() string { return "" }
func (m *mockExternal) GetType() string  { return "sql" }
func (m *mockExternal) SetDefaultDatabase(_ string) {
	// No-op for mock
}

func (m *mockExternal) SetDefaults(_, _ string) {
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
	// Only apply default range if lastPositions wasn't explicitly set to 0
	// (i.e., if the key doesn't exist, we use default; if it exists with value 0, we keep 0)
	if _, exists := m.admin.lastPositions[model.GetID()]; !exists && maxPos == 0 {
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

// TestGetValidRangeForForwardFill specifically tests the forward fill validation formula
// min = MAX(MIN(external_mins), MAX(transformation_mins)) - union semantics for externals
// max = MIN(all dependency maxes)
func TestGetValidRangeForForwardFill(t *testing.T) {
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "ext.model2", "ext.model3"}}},
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.model1", "dep.model2", "dep.model3"}}},
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "ext.model2", "dep.model1", "dep.model2"}}},
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
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "dep.model1", "dep.model2"}}},
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
		{
			name:    "uninitialized transformation dependency - should return error",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"dep.uninitialized"}
				dag.nodes = map[string]models.Node{
					"model.test":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"dep.uninitialized"}}},
					"dep.uninitialized": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninitialized", interval: 100}},
				}
				// Uninitialized transformation has no data (0, 0)
				admin.firstPositions = map[string]uint64{
					"dep.uninitialized": 0,
				}
				admin.lastPositions = map[string]uint64{
					"dep.uninitialized": 0,
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true, // Should error due to uninitialized transformation
		},
		{
			name:    "mixed deps with one uninitialized transformation - should error",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "dep.initialized", "dep.uninitialized"}
				dag.nodes = map[string]models.Node{
					"model.test":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "dep.initialized", "dep.uninitialized"}}},
					"ext.model1":        models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"dep.initialized":   models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.initialized", interval: 100}},
					"dep.uninitialized": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninitialized", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1":        100,
					"dep.initialized":   200,
					"dep.uninitialized": 0, // No data
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1":        5000,
					"dep.initialized":   4000,
					"dep.uninitialized": 0, // No data
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true, // Should error due to uninitialized transformation in AND dependency
		},
		{
			name:    "incremental with scheduled dependency - scheduled excluded from bounds",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.raw_data", "reference.exchange_rates"}
				// Create scheduled handler for exchange_rates
				scheduledHandler := &mockScheduledHandler{}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model: &mockTransformation{
							id:           "model.test",
							interval:     3600,
							dependencies: []string{"ext.raw_data", "reference.exchange_rates"},
						},
					},
					"ext.raw_data": models.Node{
						NodeType: models.NodeTypeExternal,
						Model:    &mockExternal{id: "ext.raw_data"},
					},
					"reference.exchange_rates": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model: &mockTransformation{
							id:      "reference.exchange_rates",
							handler: scheduledHandler,
						},
					},
				}
				// External has data from 1000 to 5000
				admin.firstPositions = map[string]uint64{
					"ext.raw_data": 1000,
				}
				admin.lastPositions = map[string]uint64{
					"ext.raw_data": 5000,
				}
				// Scheduled transformation returns 0,0 (doesn't track positions)
				admin.firstPositions["reference.exchange_rates"] = 0
				admin.lastPositions["reference.exchange_rates"] = 0
			},
			expectedMin: 1000, // Should use external min only, scheduled excluded
			expectedMax: 5000, // Should use external max only, scheduled excluded
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

			minPos, maxPos, err := validator.GetValidRange(ctx, tt.modelID, Union)

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

// TestGetValidRangeUnionWithORGroups specifically tests OR group dependency handling
func TestGetValidRangeUnionWithORGroups(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		setupMocks  func(*mockDAGReader, *mockAdmin)
		expectedMin uint64
		expectedMax uint64
		wantErr     bool
		errMessage  string
	}{
		{
			name:    "OR group with all dependencies initialized - uses union of all ranges",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// Create a model with an OR dependency group
				orDeps := []transformation.Dependency{
					{
						IsGroup:   true,
						GroupDeps: []string{"dep.model1", "dep.model2"},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"dep.model1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
					"dep.model2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model2", interval: 100}},
				}
				// Both transformations have data
				admin.firstPositions = map[string]uint64{
					"dep.model1": 100,
					"dep.model2": 200,
				}
				admin.lastPositions = map[string]uint64{
					"dep.model1": 3000, // Wider range (2900)
					"dep.model2": 1500, // Narrower range (1300)
				}
			},
			expectedMin: 100,  // Takes minimum of all OR members: min(100, 200) = 100
			expectedMax: 3000, // Takes maximum of all OR members: max(3000, 1500) = 3000 (union semantics)
			wantErr:     false,
		},
		{
			name:    "OR group where widest range doesn't have highest max - uses union not widest",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// Reproduces the bug: ModelA has widest range but ModelB has higher max
				// This is the exact scenario from the user's issue
				orDeps := []transformation.Dependency{
					{
						IsGroup: true,
						GroupDeps: []string{
							"ext.modelA", "ext.modelB", "ext.modelC", "ext.modelD",
						},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test": models.Node{
						NodeType: models.NodeTypeTransformation,
						Model:    &mockTransformation{id: "model.test", interval: 50000, orDependencies: orDeps},
					},
					"ext.modelA": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.modelA"}}, // Widest range: 28188
					"ext.modelB": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.modelB"}}, // Narrower but higher max: 2592
					"ext.modelC": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.modelC"}}, // Range: 27588
					"ext.modelD": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.modelD"}}, // Same as B
				}
				// Set bounds for external models via admin mock
				admin.firstPositions = map[string]uint64{
					"ext.modelA": 0,     // Widest range: 0 to 28188 (28188 units)
					"ext.modelB": 27396, // Narrower range but higher max: 27396 to 29988 (2592 units)
					"ext.modelC": 0,     // Range: 0 to 27588 (27588 units)
					"ext.modelD": 27396, // Same as B: 27396 to 29988 (2592 units)
				}
				admin.lastPositions = map[string]uint64{
					"ext.modelA": 28188, // ModelA has widest range
					"ext.modelB": 29988, // ModelB has highest max
					"ext.modelC": 27588,
					"ext.modelD": 29988, // Same as ModelB
				}
			},
			expectedMin: 0,     // Takes minimum across all OR members: min(0, 27396, 0, 27396) = 0
			expectedMax: 29988, // Takes maximum across all OR members: max(28188, 29988, 27588, 29988) = 29988 not 28188
			wantErr:     false,
		},
		{
			name:    "OR group with one uninitialized - uses initialized one",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				orDeps := []transformation.Dependency{
					{
						IsGroup:   true,
						GroupDeps: []string{"dep.uninitialized", "dep.initialized"},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"dep.uninitialized": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninitialized", interval: 100}},
					"dep.initialized":   models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.initialized", interval: 100}},
				}
				// One has no data, one has data
				admin.firstPositions = map[string]uint64{
					"dep.uninitialized": 0,
					"dep.initialized":   500,
				}
				admin.lastPositions = map[string]uint64{
					"dep.uninitialized": 0,
					"dep.initialized":   2000,
				}
			},
			expectedMin: 500,  // Uses the initialized one
			expectedMax: 2000, // Uses the initialized one's max
			wantErr:     false,
		},
		{
			name:    "OR group with all uninitialized - should error",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				orDeps := []transformation.Dependency{
					{
						IsGroup:   true,
						GroupDeps: []string{"dep.uninit1", "dep.uninit2"},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test":  models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"dep.uninit1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninit1", interval: 100}},
					"dep.uninit2": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.uninit2", interval: 100}},
				}
				// Both have no data
				admin.firstPositions = map[string]uint64{
					"dep.uninit1": 0,
					"dep.uninit2": 0,
				}
				admin.lastPositions = map[string]uint64{
					"dep.uninit1": 0,
					"dep.uninit2": 0,
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
			errMessage:  "no dependencies in OR group are available",
		},
		{
			name:    "Mixed OR and AND dependencies - AND has uninitialized",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// OR group (at least one available) + AND dependency (uninitialized)
				orDeps := []transformation.Dependency{
					{
						IsGroup:   true,
						GroupDeps: []string{"dep.or1", "dep.or2"},
					},
					{
						IsGroup:   false,
						SingleDep: "dep.and_uninit",
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test":     models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"dep.or1":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.or1", interval: 100}},
					"dep.or2":        models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.or2", interval: 100}},
					"dep.and_uninit": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.and_uninit", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"dep.or1":        100,
					"dep.or2":        200,
					"dep.and_uninit": 0, // Uninitialized AND dependency
				}
				admin.lastPositions = map[string]uint64{
					"dep.or1":        3000,
					"dep.or2":        2000,
					"dep.and_uninit": 0,
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
			errMessage:  "transformation dependency has not been initialized",
		},
		{
			name:    "OR group with external and transformation deps mixed",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				orDeps := []transformation.Dependency{
					{
						IsGroup:   true,
						GroupDeps: []string{"ext.model1", "dep.transform1"},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test":     models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"ext.model1":     models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"dep.transform1": models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.transform1", interval: 100}},
				}
				// External always has data, transformation is uninitialized
				admin.firstPositions = map[string]uint64{
					"ext.model1":     100,
					"dep.transform1": 0, // Uninitialized
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1":     5000,
					"dep.transform1": 0,
				}
			},
			expectedMin: 100,  // Uses external since transformation is uninitialized
			expectedMax: 5000, // Uses external's max
			wantErr:     false,
		},
		{
			name:    "OR group with all external deps having no data - should error",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// All external models have (0, 0) bounds - no data indexed yet
				// This reproduces the bug where backfill would scan from 0 forever
				orDeps := []transformation.Dependency{
					{
						IsGroup: true,
						GroupDeps: []string{
							"ext.data_column_sidecar",
							"ext.blob_sidecar",
							"ext.gossip_data_column",
							"ext.gossip_blob",
						},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test":              models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"ext.data_column_sidecar": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.data_column_sidecar"}},
					"ext.blob_sidecar":        models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.blob_sidecar"}},
					"ext.gossip_data_column":  models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.gossip_data_column"}},
					"ext.gossip_blob":         models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.gossip_blob"}},
				}
				// All external models have no data indexed yet
				admin.firstPositions = map[string]uint64{
					"ext.data_column_sidecar": 0,
					"ext.blob_sidecar":        0,
					"ext.gossip_data_column":  0,
					"ext.gossip_blob":         0,
				}
				admin.lastPositions = map[string]uint64{
					"ext.data_column_sidecar": 0,
					"ext.blob_sidecar":        0,
					"ext.gossip_data_column":  0,
					"ext.gossip_blob":         0,
				}
			},
			expectedMin: 0,
			expectedMax: 0,
			wantErr:     true,
			errMessage:  "no dependencies in OR group are available",
		},
		{
			name:    "OR group with one external having data - uses that one",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// Only one external has data, others have (0, 0)
				orDeps := []transformation.Dependency{
					{
						IsGroup: true,
						GroupDeps: []string{
							"ext.no_data_1",
							"ext.has_data",
							"ext.no_data_2",
						},
					},
				}
				dag.nodes = map[string]models.Node{
					"model.test":    models.Node{NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, orDependencies: orDeps}},
					"ext.no_data_1": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.no_data_1"}},
					"ext.has_data":  models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.has_data"}},
					"ext.no_data_2": models.Node{NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.no_data_2"}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.no_data_1": 0,
					"ext.has_data":  1000, // Only this one has data
					"ext.no_data_2": 0,
				}
				admin.lastPositions = map[string]uint64{
					"ext.no_data_1": 0,
					"ext.has_data":  5000, // Only this one has data
					"ext.no_data_2": 0,
				}
			},
			expectedMin: 1000, // Uses the one with data
			expectedMax: 5000, // Uses the one with data
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

			minPos, maxPos, err := validator.GetValidRange(ctx, tt.modelID, Union)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMin, minPos, "min position mismatch")
				assert.Equal(t, tt.expectedMax, maxPos, "max position mismatch")
			}
		})
	}
}

func TestApplyFillBuffer(t *testing.T) {
	tests := []struct {
		name        string
		buffer      uint64
		finalMin    uint64
		finalMax    uint64
		expectedMax uint64
	}{
		{
			name:        "no buffer configured",
			buffer:      0,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 2000,
		},
		{
			name:        "buffer smaller than max",
			buffer:      100,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 1900, // 2000 - 100
		},
		{
			name:        "buffer equals max",
			buffer:      2000,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 1000, // Set to min when buffer >= max
		},
		{
			name:        "buffer larger than max",
			buffer:      3000,
			finalMin:    1000,
			finalMax:    2000,
			expectedMax: 1000, // Set to min when buffer > max
		},
		{
			name:        "buffer applied to large range",
			buffer:      500,
			finalMin:    0,
			finalMax:    10000,
			expectedMax: 9500, // 10000 - 500
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := &dependencyValidator{
				log: logrus.New(),
			}

			handler := &mockTransformationHandlerWithBuffer{
				buffer: tt.buffer,
			}

			result := validator.applyFillBuffer(handler, "test_model", tt.finalMin, tt.finalMax)
			assert.Equal(t, tt.expectedMax, result)
		})
	}
}

// mockTransformationHandlerWithBuffer mocks a handler with buffer configuration
type mockTransformationHandlerWithBuffer struct {
	mockHandler
	buffer uint64
}

func (m *mockTransformationHandlerWithBuffer) GetFillBuffer() uint64 {
	return m.buffer
}

func TestGetTransformationModel(t *testing.T) {
	tests := []struct {
		name      string
		modelID   string
		setupMock func(*mockDAGReader)
		wantErr   bool
		errType   error
	}{
		{
			name:    "valid transformation model",
			modelID: "test.model",
			setupMock: func(dag *mockDAGReader) {
				dag.nodes = map[string]models.Node{
					"test.model": {
						NodeType: models.NodeTypeTransformation,
						Model:    &mockTransformation{id: "test.model", interval: 100},
					},
				}
			},
			wantErr: false,
		},
		{
			name:    "model not found",
			modelID: "missing.model",
			setupMock: func(dag *mockDAGReader) {
				dag.nodes = map[string]models.Node{}
			},
			wantErr: true,
			errType: ErrModelNotFound,
		},
		{
			name:    "not a transformation model",
			modelID: "ext.model",
			setupMock: func(dag *mockDAGReader) {
				dag.nodes = map[string]models.Node{
					"ext.model": {
						NodeType: models.NodeTypeExternal,
						Model:    &mockExternal{id: "ext.model"},
					},
				}
			},
			wantErr: true,
			errType: ErrNotTransformationModel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDAG := newMockDAGReader()
			tt.setupMock(mockDAG)

			validator := &dependencyValidator{
				log: logrus.New(),
				dag: mockDAG,
			}

			model, err := validator.getTransformationModel(tt.modelID)
			if tt.wantErr {
				require.Error(t, err)

				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}

				assert.Nil(t, model)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, model)
				assert.Equal(t, tt.modelID, model.GetID())
			}
		})
	}
}

// TestGetValidRangeIntersection tests the backfill-specific range calculation
// which uses intersection semantics (MAX of all mins) instead of union semantics.
// min = MAX(all dependency mins) - requires ALL dependencies to have data
// max = MIN(all dependency maxes)
func TestGetValidRangeIntersection(t *testing.T) {
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
			name:    "multiple externals - uses MAX of mins (intersection)",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "ext.model2"}
				dag.nodes = map[string]models.Node{
					"model.test": {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "ext.model2"}}},
					"ext.model1": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"ext.model2": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model2"}},
				}
				// External 1: starts at 100, ends at 5000
				// External 2: starts at 500, ends at 4000
				// For backfill, min should be MAX(100, 500) = 500 (intersection)
				// max should be MIN(5000, 4000) = 4000
				admin.firstPositions = map[string]uint64{
					"ext.model1": 100,
					"ext.model2": 500,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000,
					"ext.model2": 4000,
				}
			},
			expectedMin: 500,  // MAX of mins (intersection where both have data)
			expectedMax: 4000, // MIN of maxes
			wantErr:     false,
		},
		{
			name:    "compare with forward fill - backfill uses intersection",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				// This test demonstrates the difference between forward fill and backfill
				// custody_probe: 13,164,545 -> 13,165,835
				// synthetic_heartbeat: 13,140,598 -> 13,165,835
				dag.dependencies = []string{"custody_probe", "synthetic_heartbeat"}
				dag.nodes = map[string]models.Node{
					"model.test":          {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"custody_probe", "synthetic_heartbeat"}}},
					"custody_probe":       {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "custody_probe"}},
					"synthetic_heartbeat": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "synthetic_heartbeat"}},
				}
				admin.firstPositions = map[string]uint64{
					"custody_probe":       13164545,
					"synthetic_heartbeat": 13140598,
				}
				admin.lastPositions = map[string]uint64{
					"custody_probe":       13165835,
					"synthetic_heartbeat": 13165835,
				}
			},
			// GetValidRangeForForwardFill would return min of 13140598 (MIN of externals, i.e. union)
			// GetValidRangeForBackfill returns min of 13164545 (MAX of all mins, i.e. intersection)
			expectedMin: 13164545,
			expectedMax: 13165835,
			wantErr:     false,
		},
		{
			name:    "mixed external and transformation deps",
			modelID: "model.test",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"ext.model1", "dep.model1"}
				dag.nodes = map[string]models.Node{
					"model.test": {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"ext.model1", "dep.model1"}}},
					"ext.model1": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "ext.model1"}},
					"dep.model1": {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "dep.model1", interval: 100}},
				}
				admin.firstPositions = map[string]uint64{
					"ext.model1": 100,
					"dep.model1": 300,
				}
				admin.lastPositions = map[string]uint64{
					"ext.model1": 5000,
					"dep.model1": 4000,
				}
			},
			expectedMin: 300,  // MAX of mins gives intersection
			expectedMax: 4000, // MIN of maxes
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			log := logrus.New()
			log.SetLevel(logrus.DebugLevel)

			mockDAG := &mockDAGReader{
				nodes:        make(map[string]models.Node),
				dependencies: []string{},
			}
			mockAdmin := &mockAdmin{
				firstPositions: make(map[string]uint64),
				lastPositions:  make(map[string]uint64),
			}

			tt.setupMocks(mockDAG, mockAdmin)

			validator := &dependencyValidator{
				log:   log.WithField("test", tt.name),
				dag:   mockDAG,
				admin: mockAdmin,
				externalManager: &mockExternalModelValidator{
					admin: mockAdmin,
				},
			}

			minPos, maxPos, err := validator.GetValidRange(ctx, tt.modelID, Intersection)

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

// TestValidateDependenciesUsesIntersection verifies that ValidateDependencies
// uses intersection semantics (GetValidRangeForBackfill) to ensure positions
// are only processed where ALL dependencies have data.
func TestValidateDependenciesUsesIntersection(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		position    uint64
		interval    uint64
		setupMocks  func(*mockDAGReader, *mockAdmin)
		canProcess  bool
		description string
	}{
		{
			name:        "rejects position below intersection min",
			modelID:     "model.test",
			position:    13150000, // Between synthetic_heartbeat min and custody_probe min
			interval:    100,
			description: "Position 13,150,000 is within synthetic_heartbeat (13,140,598) but NOT custody_probe (13,164,545)",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"custody_probe", "synthetic_heartbeat"}
				dag.nodes = map[string]models.Node{
					"model.test":          {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"custody_probe", "synthetic_heartbeat"}}},
					"custody_probe":       {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "custody_probe"}},
					"synthetic_heartbeat": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "synthetic_heartbeat"}},
				}
				admin.firstPositions = map[string]uint64{
					"custody_probe":       13164545,
					"synthetic_heartbeat": 13140598,
				}
				admin.lastPositions = map[string]uint64{
					"custody_probe":       13165835,
					"synthetic_heartbeat": 13165835,
				}
			},
			canProcess: false, // Should reject because custody_probe has no data at this position
		},
		{
			name:        "accepts position within intersection",
			modelID:     "model.test",
			position:    13165000, // Within BOTH dependencies
			interval:    100,
			description: "Position 13,165,000 is within both custody_probe and synthetic_heartbeat ranges",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"custody_probe", "synthetic_heartbeat"}
				dag.nodes = map[string]models.Node{
					"model.test":          {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"custody_probe", "synthetic_heartbeat"}}},
					"custody_probe":       {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "custody_probe"}},
					"synthetic_heartbeat": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "synthetic_heartbeat"}},
				}
				admin.firstPositions = map[string]uint64{
					"custody_probe":       13164545,
					"synthetic_heartbeat": 13140598,
				}
				admin.lastPositions = map[string]uint64{
					"custody_probe":       13165835,
					"synthetic_heartbeat": 13165835,
				}
				admin.coverage = true // Mark as having coverage
			},
			canProcess: true, // Should accept because both have data at this position
		},
		{
			name:        "accepts position at intersection boundary",
			modelID:     "model.test",
			position:    13164545, // Exactly at custody_probe min (intersection start)
			interval:    100,
			description: "Position exactly at custody_probe min (13,164,545) which is the intersection start",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"custody_probe", "synthetic_heartbeat"}
				dag.nodes = map[string]models.Node{
					"model.test":          {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"custody_probe", "synthetic_heartbeat"}}},
					"custody_probe":       {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "custody_probe"}},
					"synthetic_heartbeat": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "synthetic_heartbeat"}},
				}
				admin.firstPositions = map[string]uint64{
					"custody_probe":       13164545,
					"synthetic_heartbeat": 13140598,
				}
				admin.lastPositions = map[string]uint64{
					"custody_probe":       13165835,
					"synthetic_heartbeat": 13165835,
				}
				admin.coverage = true
			},
			canProcess: true, // Should accept because both have data at intersection start
		},
		{
			name:        "rejects position beyond max",
			modelID:     "model.test",
			position:    13170000, // Beyond both dependencies
			interval:    100,
			description: "Position 13,170,000 is beyond both dependencies' max",
			setupMocks: func(dag *mockDAGReader, admin *mockAdmin) {
				dag.dependencies = []string{"custody_probe", "synthetic_heartbeat"}
				dag.nodes = map[string]models.Node{
					"model.test":          {NodeType: models.NodeTypeTransformation, Model: &mockTransformation{id: "model.test", interval: 100, dependencies: []string{"custody_probe", "synthetic_heartbeat"}}},
					"custody_probe":       {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "custody_probe"}},
					"synthetic_heartbeat": {NodeType: models.NodeTypeExternal, Model: &mockExternal{id: "synthetic_heartbeat"}},
				}
				admin.firstPositions = map[string]uint64{
					"custody_probe":       13164545,
					"synthetic_heartbeat": 13140598,
				}
				admin.lastPositions = map[string]uint64{
					"custody_probe":       13165835,
					"synthetic_heartbeat": 13165835,
				}
			},
			canProcess: false, // Should reject because position is beyond max
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			log := logrus.New()
			log.SetLevel(logrus.DebugLevel)

			mockDAG := &mockDAGReader{
				nodes:        make(map[string]models.Node),
				dependencies: []string{},
			}
			mockAdmin := &mockAdmin{
				firstPositions: make(map[string]uint64),
				lastPositions:  make(map[string]uint64),
			}

			tt.setupMocks(mockDAG, mockAdmin)

			validator := &dependencyValidator{
				log:   log.WithField("test", tt.name),
				dag:   mockDAG,
				admin: mockAdmin,
				externalManager: &mockExternalModelValidator{
					admin: mockAdmin,
				},
			}

			result, err := validator.ValidateDependencies(ctx, tt.modelID, tt.position, tt.interval)
			require.NoError(t, err, "ValidateDependencies should not error")

			assert.Equal(t, tt.canProcess, result.CanProcess,
				"%s: expected CanProcess=%v, got %v", tt.description, tt.canProcess, result.CanProcess)
		})
	}
}
