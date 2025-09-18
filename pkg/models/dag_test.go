package models

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock handler for testing
type mockHandler struct {
	dependencies []string
}

func (h *mockHandler) GetFlattenedDependencies() []string {
	return h.dependencies
}

// Implement the full Handler interface (these are no-op for tests)
func (h *mockHandler) Type() transformation.Type { return "incremental" }
func (h *mockHandler) Config() any               { return nil }
func (h *mockHandler) Validate() error           { return nil }
func (h *mockHandler) ShouldTrackPosition() bool { return true }
func (h *mockHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *mockHandler) GetAdminTable() transformation.AdminTable { return transformation.AdminTable{} }
func (h *mockHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

// Mock transformation for testing
type mockTransformation struct {
	id      string
	config  transformation.Config
	handler transformation.Handler
}

func (m *mockTransformation) GetID() string                       { return m.id }
func (m *mockTransformation) GetConfig() *transformation.Config   { return &m.config }
func (m *mockTransformation) GetSQL() string                      { return "" }
func (m *mockTransformation) GetType() string                     { return "transformation" }
func (m *mockTransformation) GetValue() string                    { return "" }
func (m *mockTransformation) GetHandler() transformation.Handler  { return m.handler }
func (m *mockTransformation) GetDependencies() []string           { return []string{} }
func (m *mockTransformation) GetEnvironmentVariables() []string   { return []string{} }
func (m *mockTransformation) SetDefaultDatabase(defaultDB string) { m.config.SetDefaults(defaultDB) }

// Mock external for testing
type mockExternal struct {
	id     string
	config external.Config
	typ    string
}

func (m *mockExternal) GetID() string                       { return m.id }
func (m *mockExternal) GetConfig() external.Config          { return m.config }
func (m *mockExternal) GetType() string                     { return m.typ }
func (m *mockExternal) GetSQL() string                      { return "" }
func (m *mockExternal) GetValue() string                    { return "" }
func (m *mockExternal) GetEnvironmentVariables() []string   { return []string{} }
func (m *mockExternal) SetDefaultDatabase(defaultDB string) { m.config.SetDefaults(defaultDB) }

// Test NewDependencyGraph
func TestNewDependencyGraph(t *testing.T) {
	dg := NewDependencyGraph()

	assert.NotNil(t, dg)
	assert.NotNil(t, dg.dag)
}

// Test BuildGraph
func TestBuildGraph(t *testing.T) {
	tests := []struct {
		name              string
		transformations   []Transformation
		externals         []External
		wantErr           bool
		expectedNodeCount int
	}{
		{
			name: "build simple graph",
			transformations: []Transformation{
				&mockTransformation{
					id:      "db.trans1",
					config:  transformation.Config{Database: "db", Table: "trans1"},
					handler: &mockHandler{dependencies: []string{}},
				},
				&mockTransformation{
					id:      "db.trans2",
					config:  transformation.Config{Database: "db", Table: "trans2"},
					handler: &mockHandler{dependencies: []string{"db.trans1"}},
				},
			},
			externals: []External{
				&mockExternal{id: "db.ext1", typ: external.ExternalTypeSQL},
			},
			wantErr:           false,
			expectedNodeCount: 3,
		},
		{
			name: "build with cyclic dependency",
			transformations: []Transformation{
				&mockTransformation{
					id:      "db.trans1",
					config:  transformation.Config{Database: "db", Table: "trans1"},
					handler: &mockHandler{dependencies: []string{"db.trans2"}},
				},
				&mockTransformation{
					id:      "db.trans2",
					config:  transformation.Config{Database: "db", Table: "trans2"},
					handler: &mockHandler{dependencies: []string{"db.trans1"}},
				},
			},
			externals: []External{},
			wantErr:   true,
		},
		{
			name:              "empty graph",
			transformations:   []Transformation{},
			externals:         []External{},
			wantErr:           false,
			expectedNodeCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dg := NewDependencyGraph()
			err := dg.BuildGraph(tt.transformations, tt.externals)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expectedNodeCount > 0 {
					// Verify nodes were added
					for _, trans := range tt.transformations {
						node, err := dg.GetNode(trans.GetID())
						assert.NoError(t, err)
						assert.Equal(t, NodeTypeTransformation, node.NodeType)
					}
					for _, ext := range tt.externals {
						node, err := dg.GetNode(ext.GetID())
						assert.NoError(t, err)
						assert.Equal(t, NodeTypeExternal, node.NodeType)
					}
				}
			}
		})
	}
}

// Test GetNode
func TestGetNode(t *testing.T) {
	dg := NewDependencyGraph()
	trans := &mockTransformation{
		id:     "db.trans1",
		config: transformation.Config{Database: "db", Table: "trans1"},
	}
	ext := &mockExternal{id: "db.ext1", typ: external.ExternalTypeSQL}

	err := dg.BuildGraph([]Transformation{trans}, []External{ext})
	require.NoError(t, err)

	tests := []struct {
		name     string
		nodeID   string
		wantErr  bool
		nodeType NodeType
	}{
		{
			name:     "get transformation node",
			nodeID:   "db.trans1",
			wantErr:  false,
			nodeType: NodeTypeTransformation,
		},
		{
			name:     "get external node",
			nodeID:   "db.ext1",
			wantErr:  false,
			nodeType: NodeTypeExternal,
		},
		{
			name:    "get non-existent node",
			nodeID:  "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := dg.GetNode(tt.nodeID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.nodeType, node.NodeType)
			}
		})
	}
}

// Test GetTransformationNode
func TestGetTransformationNode(t *testing.T) {
	dg := NewDependencyGraph()
	trans := &mockTransformation{
		id:     "db.trans1",
		config: transformation.Config{Database: "db", Table: "trans1"},
	}
	ext := &mockExternal{id: "db.ext1", typ: external.ExternalTypeSQL}

	err := dg.BuildGraph([]Transformation{trans}, []External{ext})
	require.NoError(t, err)

	tests := []struct {
		name    string
		nodeID  string
		wantErr bool
	}{
		{
			name:    "get valid transformation",
			nodeID:  "db.trans1",
			wantErr: false,
		},
		{
			name:    "get external as transformation",
			nodeID:  "db.ext1",
			wantErr: true,
		},
		{
			name:    "get non-existent node",
			nodeID:  "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := dg.GetTransformationNode(tt.nodeID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, trans.GetID(), node.GetID())
			}
		})
	}
}

// Test GetExternalNode
func TestGetExternalNode(t *testing.T) {
	dg := NewDependencyGraph()
	trans := &mockTransformation{
		id:     "db.trans1",
		config: transformation.Config{Database: "db", Table: "trans1"},
	}
	ext := &mockExternal{id: "db.ext1", typ: external.ExternalTypeSQL}

	err := dg.BuildGraph([]Transformation{trans}, []External{ext})
	require.NoError(t, err)

	tests := []struct {
		name    string
		nodeID  string
		wantErr bool
	}{
		{
			name:    "get valid external",
			nodeID:  "db.ext1",
			wantErr: false,
		},
		{
			name:    "get transformation as external",
			nodeID:  "db.trans1",
			wantErr: true,
		},
		{
			name:    "get non-existent node",
			nodeID:  "missing",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := dg.GetExternalNode(tt.nodeID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, ext.GetID(), node.GetID())
			}
		})
	}
}

// Test GetDependencies and GetDependents
func TestGetDependenciesAndDependents(t *testing.T) {
	dg := NewDependencyGraph()

	trans1 := &mockTransformation{
		id:      "db.trans1",
		config:  transformation.Config{Database: "db", Table: "trans1"},
		handler: &mockHandler{dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:      "db.trans2",
		config:  transformation.Config{Database: "db", Table: "trans2"},
		handler: &mockHandler{dependencies: []string{"db.trans1"}},
	}
	trans3 := &mockTransformation{
		id:      "db.trans3",
		config:  transformation.Config{Database: "db", Table: "trans3"},
		handler: &mockHandler{dependencies: []string{"db.trans1", "db.trans2"}},
	}

	err := dg.BuildGraph([]Transformation{trans1, trans2, trans3}, []External{})
	require.NoError(t, err)

	// Test GetDependencies
	deps := dg.GetDependencies("db.trans3")
	assert.ElementsMatch(t, []string{"db.trans1", "db.trans2"}, deps)

	deps = dg.GetDependencies("db.trans2")
	assert.ElementsMatch(t, []string{"db.trans1"}, deps)

	deps = dg.GetDependencies("db.trans1")
	assert.NotNil(t, deps)
	assert.Empty(t, deps)

	// Test GetDependents
	dependents := dg.GetDependents("db.trans1")
	assert.ElementsMatch(t, []string{"db.trans2", "db.trans3"}, dependents)

	dependents = dg.GetDependents("db.trans2")
	assert.ElementsMatch(t, []string{"db.trans3"}, dependents)

	dependents = dg.GetDependents("db.trans3")
	assert.NotNil(t, dependents)
	assert.Empty(t, dependents)

	// Test with non-existent node
	deps = dg.GetDependencies("non-existent")
	assert.Nil(t, deps)

	dependents = dg.GetDependents("non-existent")
	assert.Nil(t, dependents)
}

// Test GetAllDependencies and GetAllDependents
func TestGetAllDependenciesAndDependents(t *testing.T) {
	dg := NewDependencyGraph()

	// Create a diamond dependency graph
	// trans1 -> trans2 -> trans4
	//        -> trans3 -> trans4
	trans1 := &mockTransformation{
		id:      "db.trans1",
		config:  transformation.Config{Database: "db", Table: "trans1"},
		handler: &mockHandler{dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:      "db.trans2",
		config:  transformation.Config{Database: "db", Table: "trans2"},
		handler: &mockHandler{dependencies: []string{"db.trans1"}},
	}
	trans3 := &mockTransformation{
		id:      "db.trans3",
		config:  transformation.Config{Database: "db", Table: "trans3"},
		handler: &mockHandler{dependencies: []string{"db.trans1"}},
	}
	trans4 := &mockTransformation{
		id:      "db.trans4",
		config:  transformation.Config{Database: "db", Table: "trans4"},
		handler: &mockHandler{dependencies: []string{"db.trans2", "db.trans3"}},
	}

	err := dg.BuildGraph([]Transformation{trans1, trans2, trans3, trans4}, []External{})
	require.NoError(t, err)

	// Test GetAllDependencies
	allDeps := dg.GetAllDependencies("db.trans4")
	assert.ElementsMatch(t, []string{"db.trans1", "db.trans2", "db.trans3"}, allDeps)

	// Test GetAllDependents
	allDependents := dg.GetAllDependents("db.trans1")
	assert.ElementsMatch(t, []string{"db.trans2", "db.trans3", "db.trans4"}, allDependents)

	// Test with non-existent node
	allDeps = dg.GetAllDependencies("non-existent")
	assert.Nil(t, allDeps)

	allDependents = dg.GetAllDependents("non-existent")
	assert.Nil(t, allDependents)
}

// Test GetTransformationNodes
func TestGetTransformationNodes(t *testing.T) {
	dg := NewDependencyGraph()

	trans1 := &mockTransformation{
		id:      "db.trans1",
		config:  transformation.Config{Database: "db", Table: "trans1"},
		handler: &mockHandler{dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:      "db.trans2",
		config:  transformation.Config{Database: "db", Table: "trans2"},
		handler: &mockHandler{dependencies: []string{"db.trans1"}},
	}
	ext := &mockExternal{id: "ext1", typ: external.ExternalTypeSQL}

	err := dg.BuildGraph([]Transformation{trans1, trans2}, []External{ext})
	require.NoError(t, err)

	nodes := dg.GetTransformationNodes()
	assert.Len(t, nodes, 2)

	nodeIDs := make([]string, len(nodes))
	for i, node := range nodes {
		nodeIDs[i] = node.GetID()
	}
	assert.ElementsMatch(t, []string{"db.trans1", "db.trans2"}, nodeIDs)
}

// Test IsPathBetween
func TestIsPathBetween(t *testing.T) {
	dg := NewDependencyGraph()

	trans1 := &mockTransformation{
		id:      "db.trans1",
		config:  transformation.Config{Database: "db", Table: "trans1"},
		handler: &mockHandler{dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:      "db.trans2",
		config:  transformation.Config{Database: "db", Table: "trans2"},
		handler: &mockHandler{dependencies: []string{"db.trans1"}},
	}
	trans3 := &mockTransformation{
		id:      "db.trans3",
		config:  transformation.Config{Database: "db", Table: "trans3"},
		handler: &mockHandler{dependencies: []string{"db.trans2"}},
	}
	trans4 := &mockTransformation{
		id:      "db.trans4",
		config:  transformation.Config{Database: "db", Table: "trans4"},
		handler: &mockHandler{dependencies: []string{}},
	}

	err := dg.BuildGraph([]Transformation{trans1, trans2, trans3, trans4}, []External{})
	require.NoError(t, err)

	tests := []struct {
		name     string
		from     string
		to       string
		expected bool
	}{
		{
			name:     "direct path exists",
			from:     "db.trans1",
			to:       "db.trans2",
			expected: true,
		},
		{
			name:     "indirect path exists",
			from:     "db.trans1",
			to:       "db.trans3",
			expected: true,
		},
		{
			name:     "no path exists",
			from:     "db.trans1",
			to:       "db.trans4",
			expected: false,
		},
		{
			name:     "reverse path doesn't exist",
			from:     "db.trans3",
			to:       "db.trans1",
			expected: false,
		},
		{
			name:     "self path",
			from:     "db.trans1",
			to:       "db.trans1",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dg.IsPathBetween(tt.from, tt.to)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test concurrent access
func TestConcurrentAccess(t *testing.T) {
	dg := NewDependencyGraph()

	trans1 := &mockTransformation{
		id:      "db.trans1",
		config:  transformation.Config{Database: "db", Table: "trans1"},
		handler: &mockHandler{dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:      "db.trans2",
		config:  transformation.Config{Database: "db", Table: "trans2"},
		handler: &mockHandler{dependencies: []string{"db.trans1"}},
	}

	err := dg.BuildGraph([]Transformation{trans1, trans2}, []External{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = dg.GetNode("db.trans1")
			_ = dg.GetDependencies("db.trans2")
			_ = dg.GetDependents("db.trans1")
			_ = dg.IsPathBetween("db.trans1", "db.trans2")
		}()
	}

	wg.Wait()
}

// Benchmark tests
func BenchmarkBuildGraph(b *testing.B) {
	// Create a large graph
	transformations := make([]Transformation, 100)
	for i := 0; i < 100; i++ {
		deps := []string{}
		if i > 0 {
			// Each node depends on the previous one
			deps = append(deps, transformations[i-1].GetID())
		}
		transformations[i] = &mockTransformation{
			id:      fmt.Sprintf("db.trans%d", i),
			config:  transformation.Config{Database: "db", Table: fmt.Sprintf("trans%d", i)},
			handler: &mockHandler{dependencies: deps},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dg := NewDependencyGraph()
		_ = dg.BuildGraph(transformations, []External{})
	}
}

func BenchmarkGetAllDependencies(b *testing.B) {
	dg := NewDependencyGraph()

	// Create a deep dependency chain
	transformations := make([]Transformation, 50)
	for i := 0; i < 50; i++ {
		deps := []string{}
		if i > 0 {
			deps = append(deps, transformations[i-1].GetID())
		}
		transformations[i] = &mockTransformation{
			id:      fmt.Sprintf("db.trans%d", i),
			config:  transformation.Config{Database: "db", Table: fmt.Sprintf("trans%d", i)},
			handler: &mockHandler{dependencies: deps},
		}
	}

	_ = dg.BuildGraph(transformations, []External{})
	lastID := transformations[len(transformations)-1].GetID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dg.GetAllDependencies(lastID)
	}
}
