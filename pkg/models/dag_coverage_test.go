package models

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// structuredDepHandler implements the GetDependencies() []transformation.Dependency
// interface that GetStructuredDependencies type-asserts.
type structuredDepHandler struct {
	mockHandler
	deps []transformation.Dependency
}

func (h *structuredDepHandler) GetDependencies() []transformation.Dependency { return h.deps }

var _ transformation.Handler = (*structuredDepHandler)(nil)

func TestBuildGraphAddExternalModelsError(t *testing.T) {
	// Two external models with the same ID force AddExternalModels to error
	// (duplicate vertex), covering the error return path of BuildGraph.
	ext1 := &mockExternal{id: "db.ext", typ: external.TypeSQL}
	ext2 := &mockExternal{id: "db.ext", typ: external.TypeSQL}

	dg := NewDependencyGraph()
	err := dg.BuildGraph(nil, []External{ext1, ext2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add vertex")
}

func TestAddTransformationModelsDuplicateError(t *testing.T) {
	trans1 := &mockTransformation{id: "db.t", config: transformation.Config{Database: "db", Table: "t"}}
	trans2 := &mockTransformation{id: "db.t", config: transformation.Config{Database: "db", Table: "t"}}

	dg := NewDependencyGraph()
	err := dg.BuildGraph([]Transformation{trans1, trans2}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add vertex")
}

func TestBuildGraphSkipsNilModels(t *testing.T) {
	// nil entries in both slices exercise the `if model != nil` / `if model == nil`
	// skip branches in AddTransformationModels, AddExternalModels, AddTransformationEdges.
	trans := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{}, shouldTrackPos: true},
	}
	ext := &mockExternal{id: "db.ext", typ: external.TypeSQL}

	dg := NewDependencyGraph()
	err := dg.BuildGraph([]Transformation{nil, trans}, []External{nil, ext})
	require.NoError(t, err)

	nodes := dg.GetTransformationNodes()
	assert.Len(t, nodes, 1)
	externals := dg.GetExternalNodes()
	assert.Len(t, externals, 1)
}

func TestAddModelDependenciesNoHandlerOrProvider(t *testing.T) {
	tests := []struct {
		name    string
		handler transformation.Handler
	}{
		{name: "nil handler", handler: nil},
		{name: "handler without dependency provider", handler: noDepProviderHandler{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trans := &mockTransformation{
				id:      "db.t",
				config:  transformation.Config{Database: "db", Table: "t"},
				handler: tt.handler,
			}
			dg := NewDependencyGraph()
			err := dg.BuildGraph([]Transformation{trans}, nil)
			require.NoError(t, err)
		})
	}
}

func TestGetNodeInvalidNodeType(t *testing.T) {
	dg := NewDependencyGraph()
	// Inject a raw (non-Node) vertex directly to hit the invalid-node-type branches.
	_, err := dg.dag.AddVertex("not-a-node")
	require.NoError(t, err)

	// Find its auto-assigned ID via the vertices map.
	var badID string
	for id, v := range dg.dag.GetVertices() {
		if _, ok := v.(Node); !ok {
			badID = id
		}
	}
	require.NotEmpty(t, badID)

	_, err = dg.GetNode(badID)
	require.ErrorIs(t, err, ErrInvalidNodeType)

	_, err = dg.GetExternalNode(badID)
	require.ErrorIs(t, err, ErrInvalidNodeType)

	_, err = dg.GetTransformationNode(badID)
	require.ErrorIs(t, err, ErrInvalidNodeType)
}

func TestGetExternalNodeErrors(t *testing.T) {
	trans := &mockTransformation{id: "db.t", config: transformation.Config{Database: "db", Table: "t"}}
	dg := NewDependencyGraph()
	require.NoError(t, dg.BuildGraph([]Transformation{trans}, nil))

	// Vertex exists but is a transformation, not external.
	_, err := dg.GetExternalNode("db.t")
	require.ErrorIs(t, err, ErrNotExternalModel)

	// Missing vertex.
	_, err = dg.GetExternalNode("missing")
	require.Error(t, err)
}

func TestGetTransformationNodeErrors(t *testing.T) {
	ext := &mockExternal{id: "db.ext", typ: external.TypeSQL}
	dg := NewDependencyGraph()
	require.NoError(t, dg.BuildGraph(nil, []External{ext}))

	// Vertex exists but is external, not transformation.
	_, err := dg.GetTransformationNode("db.ext")
	require.ErrorIs(t, err, ErrNotTransformationModel)

	_, err = dg.GetTransformationNode("missing")
	require.Error(t, err)
}

func TestGetStructuredDependencies(t *testing.T) {
	structDeps := []transformation.Dependency{
		{IsGroup: false, SingleDep: "db.a"},
		{IsGroup: true, GroupDeps: []string{"db.b", "db.c"}},
	}

	transWithStruct := &mockTransformation{
		id:     "db.with_struct",
		config: transformation.Config{Database: "db", Table: "with_struct"},
		handler: &structuredDepHandler{
			mockHandler: mockHandler{dependencies: []string{}, shouldTrackPos: true},
			deps:        structDeps,
		},
	}
	transNilHandler := &mockTransformation{
		id:      "db.nil_handler",
		config:  transformation.Config{Database: "db", Table: "nil_handler"},
		handler: nil,
	}
	transNoProvider := &mockTransformation{
		id:      "db.no_provider",
		config:  transformation.Config{Database: "db", Table: "no_provider"},
		handler: &mockHandler{dependencies: []string{}, shouldTrackPos: true},
	}
	ext := &mockExternal{id: "db.ext", typ: external.TypeSQL}

	dg := NewDependencyGraph()
	require.NoError(t, dg.BuildGraph(
		[]Transformation{transWithStruct, transNilHandler, transNoProvider},
		[]External{ext},
	))

	// Handler provides structured deps.
	got := dg.GetStructuredDependencies("db.with_struct")
	assert.Equal(t, structDeps, got)

	// Missing node returns nil.
	assert.Nil(t, dg.GetStructuredDependencies("missing"))

	// External node returns nil (not a transformation).
	assert.Nil(t, dg.GetStructuredDependencies("db.ext"))

	// Nil handler returns nil.
	assert.Nil(t, dg.GetStructuredDependencies("db.nil_handler"))

	// Handler without depProvider returns nil.
	assert.Nil(t, dg.GetStructuredDependencies("db.no_provider"))
}

func TestIsPathBetweenMissingNode(t *testing.T) {
	dg := NewDependencyGraph()
	assert.False(t, dg.IsPathBetween("missing-from", "missing-to"))
}

func TestGetExternalNodesSkipsRawVertices(t *testing.T) {
	ext := &mockExternal{id: "db.ext", typ: external.TypeSQL}
	dg := NewDependencyGraph()
	require.NoError(t, dg.BuildGraph(nil, []External{ext}))

	// Inject a raw non-Node vertex; GetExternalNodes must skip it.
	_, err := dg.dag.AddVertex("not-a-node")
	require.NoError(t, err)

	nodes := dg.GetExternalNodes()
	assert.Len(t, nodes, 1)
}

func TestGetTransformationNodesSkipsRawVertices(t *testing.T) {
	trans := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{}, shouldTrackPos: true},
	}
	dg := NewDependencyGraph()
	require.NoError(t, dg.BuildGraph([]Transformation{trans}, nil))

	_, err := dg.dag.AddVertex("not-a-node")
	require.NoError(t, err)

	nodes := dg.GetTransformationNodes()
	assert.Len(t, nodes, 1)
}
