package models

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trackingNoIntervalHandler tracks position but does NOT implement GetIntervalType,
// so getModelIntervalType / getTransformationIntervalType fall through to "".
type trackingNoIntervalHandler struct{}

func (trackingNoIntervalHandler) Type() transformation.Type { return "incremental" }
func (trackingNoIntervalHandler) Config() any               { return nil }
func (trackingNoIntervalHandler) Validate() error           { return nil }
func (trackingNoIntervalHandler) ShouldTrackPosition() bool { return true }
func (trackingNoIntervalHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (trackingNoIntervalHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (trackingNoIntervalHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}
func (trackingNoIntervalHandler) GetFlattenedDependencies() []string { return nil }

var _ transformation.Handler = trackingNoIntervalHandler{}

// externalNoInterval is an External that does NOT implement GetIntervalType.
type externalNoInterval struct {
	id     string
	config external.Config
}

func (m *externalNoInterval) GetID() string                              { return m.id }
func (m *externalNoInterval) GetConfig() external.Config                 { return m.config }
func (m *externalNoInterval) GetConfigMutable() *external.Config         { return &m.config }
func (m *externalNoInterval) GetType() string                            { return external.TypeSQL }
func (m *externalNoInterval) GetValue() string                           { return "" }
func (m *externalNoInterval) SetDefaults(_ /*cluster*/, _ /*db*/ string) {}

var _ External = (*externalNoInterval)(nil)

func TestGetModelIntervalTypeNoProvider(t *testing.T) {
	d := NewDependencyGraph()
	model := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: trackingNoIntervalHandler{},
	}
	assert.Empty(t, d.getModelIntervalType(model))
}

func TestGetDependencyIntervalTypeNonNode(t *testing.T) {
	d := NewDependencyGraph()
	assert.Empty(t, d.getDependencyIntervalType("not-a-node"))
}

func TestGetDependencyIntervalTypeUnknownNodeType(t *testing.T) {
	d := NewDependencyGraph()
	// A valid Node whose NodeType matches neither case in the switch.
	node := Node{NodeType: NodeType("unknown"), Model: nil}
	assert.Empty(t, d.getDependencyIntervalType(node))
}

func TestGetExternalIntervalType(t *testing.T) {
	d := NewDependencyGraph()

	// Not an External at all.
	assert.Empty(t, d.getExternalIntervalType("not-external"))

	// External without a GetIntervalType method.
	ext := &externalNoInterval{id: "db.ext", config: external.Config{Database: "db", Table: "ext"}}
	assert.Empty(t, d.getExternalIntervalType(ext))
}

func TestGetTransformationIntervalType(t *testing.T) {
	d := NewDependencyGraph()

	// Not a Transformation at all.
	assert.Empty(t, d.getTransformationIntervalType("not-transformation"))

	// Tracking transformation with no GetIntervalType method.
	trans := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: trackingNoIntervalHandler{},
	}
	assert.Empty(t, d.getTransformationIntervalType(trans))
}

func TestAddModelDependenciesNonExistentDependency(t *testing.T) {
	trans := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{"db.missing"}, shouldTrackPos: true},
	}
	d := NewDependencyGraph()
	err := d.BuildGraph([]Transformation{trans}, nil)
	require.ErrorIs(t, err, ErrNonExistentDependency)
}

func TestGetExternalNodeWrongModelType(t *testing.T) {
	d := NewDependencyGraph()
	// Node typed external but holding a non-External model.
	require.NoError(t, d.dag.AddVertexByID("db.bad", Node{NodeType: NodeTypeExternal, Model: "not-external"}))

	_, err := d.GetExternalNode("db.bad")
	require.ErrorIs(t, err, ErrInvalidExternalNodeType)
}

func TestGetTransformationNodeWrongModelType(t *testing.T) {
	d := NewDependencyGraph()
	require.NoError(t, d.dag.AddVertexByID("db.bad", Node{NodeType: NodeTypeTransformation, Model: "not-transformation"}))

	_, err := d.GetTransformationNode("db.bad")
	require.ErrorIs(t, err, ErrInvalidTransformationNodeType)
}

func TestGetStructuredDependenciesWrongModelType(t *testing.T) {
	d := NewDependencyGraph()
	// Node typed transformation but Model is not a Transformation.
	require.NoError(t, d.dag.AddVertexByID("db.bad", Node{NodeType: NodeTypeTransformation, Model: "not-transformation"}))

	assert.Nil(t, d.GetStructuredDependencies("db.bad"))
}
