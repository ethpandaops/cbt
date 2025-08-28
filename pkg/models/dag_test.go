package models

import (
	"sync"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock transformation for testing
type mockTransformation struct {
	id           string
	dependencies []string
	config       transformation.Config
}

func (m *mockTransformation) GetID() string                       { return m.id }
func (m *mockTransformation) GetDependencies() []string           { return m.dependencies }
func (m *mockTransformation) GetConfig() *transformation.Config   { return &m.config }
func (m *mockTransformation) GetSQL() string                      { return "" }
func (m *mockTransformation) GetType() string                     { return "transformation" }
func (m *mockTransformation) GetValue() string                    { return "" }
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
					id:           "trans1",
					dependencies: []string{},
					config:       transformation.Config{Dependencies: []string{}},
				},
				&mockTransformation{
					id:           "trans2",
					dependencies: []string{"trans1"},
					config:       transformation.Config{Dependencies: []string{"trans1"}},
				},
			},
			externals: []External{
				&mockExternal{id: "ext1", typ: external.ExternalTypeSQL},
			},
			wantErr:           false,
			expectedNodeCount: 3,
		},
		{
			name: "build with cyclic dependency",
			transformations: []Transformation{
				&mockTransformation{
					id:           "trans1",
					dependencies: []string{"trans2"},
					config:       transformation.Config{Dependencies: []string{"trans2"}},
				},
				&mockTransformation{
					id:           "trans2",
					dependencies: []string{"trans1"},
					config:       transformation.Config{Dependencies: []string{"trans1"}},
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
	trans := &mockTransformation{id: "trans1"}
	ext := &mockExternal{id: "ext1", typ: external.ExternalTypeSQL}

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
			nodeID:   "trans1",
			wantErr:  false,
			nodeType: NodeTypeTransformation,
		},
		{
			name:     "get external node",
			nodeID:   "ext1",
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
	trans := &mockTransformation{id: "trans1"}
	ext := &mockExternal{id: "ext1", typ: external.ExternalTypeSQL}

	err := dg.BuildGraph([]Transformation{trans}, []External{ext})
	require.NoError(t, err)

	tests := []struct {
		name    string
		nodeID  string
		wantErr bool
	}{
		{
			name:    "get valid transformation",
			nodeID:  "trans1",
			wantErr: false,
		},
		{
			name:    "get external as transformation",
			nodeID:  "ext1",
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
	trans := &mockTransformation{id: "trans1"}
	ext := &mockExternal{id: "ext1", typ: external.ExternalTypeSQL}

	err := dg.BuildGraph([]Transformation{trans}, []External{ext})
	require.NoError(t, err)

	tests := []struct {
		name    string
		nodeID  string
		wantErr bool
	}{
		{
			name:    "get valid external",
			nodeID:  "ext1",
			wantErr: false,
		},
		{
			name:    "get transformation as external",
			nodeID:  "trans1",
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
		id:           "trans1",
		dependencies: []string{},
		config:       transformation.Config{Dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:           "trans2",
		dependencies: []string{"trans1"},
		config:       transformation.Config{Dependencies: []string{"trans1"}},
	}
	trans3 := &mockTransformation{
		id:           "trans3",
		dependencies: []string{"trans1", "trans2"},
		config:       transformation.Config{Dependencies: []string{"trans1", "trans2"}},
	}

	err := dg.BuildGraph([]Transformation{trans1, trans2, trans3}, []External{})
	require.NoError(t, err)

	// Test GetDependencies
	deps := dg.GetDependencies("trans3")
	assert.ElementsMatch(t, []string{"trans1", "trans2"}, deps)

	deps = dg.GetDependencies("trans2")
	assert.ElementsMatch(t, []string{"trans1"}, deps)

	deps = dg.GetDependencies("trans1")
	assert.NotNil(t, deps)
	assert.Empty(t, deps)

	// Test GetDependents
	dependents := dg.GetDependents("trans1")
	assert.ElementsMatch(t, []string{"trans2", "trans3"}, dependents)

	dependents = dg.GetDependents("trans2")
	assert.ElementsMatch(t, []string{"trans3"}, dependents)

	dependents = dg.GetDependents("trans3")
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
		id:           "trans1",
		dependencies: []string{},
		config:       transformation.Config{Dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:           "trans2",
		dependencies: []string{"trans1"},
		config:       transformation.Config{Dependencies: []string{"trans1"}},
	}
	trans3 := &mockTransformation{
		id:           "trans3",
		dependencies: []string{"trans1"},
		config:       transformation.Config{Dependencies: []string{"trans1"}},
	}
	trans4 := &mockTransformation{
		id:           "trans4",
		dependencies: []string{"trans2", "trans3"},
		config:       transformation.Config{Dependencies: []string{"trans2", "trans3"}},
	}

	err := dg.BuildGraph([]Transformation{trans1, trans2, trans3, trans4}, []External{})
	require.NoError(t, err)

	// Test GetAllDependencies
	allDeps := dg.GetAllDependencies("trans4")
	assert.ElementsMatch(t, []string{"trans1", "trans2", "trans3"}, allDeps)

	// Test GetAllDependents
	allDependents := dg.GetAllDependents("trans1")
	assert.ElementsMatch(t, []string{"trans2", "trans3", "trans4"}, allDependents)

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
		id:     "trans1",
		config: transformation.Config{Dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:           "trans2",
		dependencies: []string{"trans1"},
		config:       transformation.Config{Dependencies: []string{"trans1"}},
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
	assert.ElementsMatch(t, []string{"trans1", "trans2"}, nodeIDs)
}

// Test IsPathBetween
func TestIsPathBetween(t *testing.T) {
	dg := NewDependencyGraph()

	trans1 := &mockTransformation{
		id:     "trans1",
		config: transformation.Config{Dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:           "trans2",
		dependencies: []string{"trans1"},
		config:       transformation.Config{Dependencies: []string{"trans1"}},
	}
	trans3 := &mockTransformation{
		id:           "trans3",
		dependencies: []string{"trans2"},
		config:       transformation.Config{Dependencies: []string{"trans2"}},
	}
	trans4 := &mockTransformation{
		id:     "trans4",
		config: transformation.Config{Dependencies: []string{}},
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
			from:     "trans1",
			to:       "trans2",
			expected: true,
		},
		{
			name:     "indirect path exists",
			from:     "trans1",
			to:       "trans3",
			expected: true,
		},
		{
			name:     "no path exists",
			from:     "trans1",
			to:       "trans4",
			expected: false,
		},
		{
			name:     "reverse path doesn't exist",
			from:     "trans3",
			to:       "trans1",
			expected: false,
		},
		{
			name:     "self path",
			from:     "trans1",
			to:       "trans1",
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
		id:     "trans1",
		config: transformation.Config{Dependencies: []string{}},
	}
	trans2 := &mockTransformation{
		id:           "trans2",
		dependencies: []string{"trans1"},
		config:       transformation.Config{Dependencies: []string{"trans1"}},
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
			_, _ = dg.GetNode("trans1")
			_ = dg.GetDependencies("trans2")
			_ = dg.GetDependents("trans1")
			_ = dg.IsPathBetween("trans1", "trans2")
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
			id:           string(rune(i)),
			dependencies: deps,
			config:       transformation.Config{Dependencies: deps},
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
			id:           string(rune(i)),
			dependencies: deps,
			config:       transformation.Config{Dependencies: deps},
		}
	}

	_ = dg.BuildGraph(transformations, []External{})
	lastID := transformations[len(transformations)-1].GetID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dg.GetAllDependencies(lastID)
	}
}
