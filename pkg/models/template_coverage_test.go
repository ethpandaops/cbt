package models

import (
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// placeholderHandler implements GetFlattenedDependencies and GetOriginalDependencies,
// driving the placeholder-tracking branches in buildDependencyVariables and the
// per-dependency placeholder-alias branches in processTransformation/ExternalDependency.
type placeholderHandler struct {
	mockHandler
	original []transformation.Dependency
}

func (h *placeholderHandler) GetOriginalDependencies() []transformation.Dependency {
	return h.original
}

var _ transformation.Handler = (*placeholderHandler)(nil)

func newChCfg() *clickhouse.Config {
	return &clickhouse.Config{URL: "http://localhost:8123", Cluster: "test_cluster", LocalSuffix: "_local"}
}

func TestRenderTransformationExecuteError(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	model := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{}},
		// Parses fine, but indexing a string with extra keys fails at execution.
		value: `{{ index .self.database "x" "y" }}`,
	}

	_, err := engine.RenderTransformation(model, 0, 1, time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute template")
}

func TestRenderExternalExecuteError(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	model := &mockExternal{
		id:     "db.ext",
		typ:    external.TypeSQL,
		config: external.Config{Database: "db", Table: "ext"},
		value:  `{{ index .self.database "x" "y" }}`,
	}

	_, err := engine.RenderExternal(model, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute template")
}

func TestBuildDependencyVariablesNilHandler(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	model := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: nil,
		value:   "SELECT 1",
	}

	out, err := engine.RenderTransformation(model, 0, 1, time.Now())
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", out)
}

func TestBuildDependencyVariablesNoDepProvider(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	model := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: noDepProviderHandler{},
		value:   "SELECT 1",
	}

	out, err := engine.RenderTransformation(model, 0, 1, time.Now())
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", out)
}

func TestBuildDependencyVariablesSharedDatabaseAndPlaceholders(t *testing.T) {
	dag := NewDependencyGraph()

	// Two external deps in the SAME database exercise the addDepEntry "reuse existing
	// db map" branch. Placeholder originals exercise resolvedToOriginal + alias entries.
	ext1 := &mockExternal{
		id:     "raw.blocks",
		typ:    external.TypeSQL,
		config: external.Config{Database: "raw", Table: "blocks"},
	}
	ext2 := &mockExternal{
		id:     "raw.txs",
		typ:    external.TypeSQL,
		config: external.Config{Database: "raw", Table: "txs"},
	}
	transDep := &mockTransformation{
		id:      "proc.summary",
		config:  transformation.Config{Database: "proc", Table: "summary"},
		handler: &mockHandler{dependencies: []string{}},
		value:   "SELECT 1",
	}

	model := &mockTransformation{
		id:     "proc.main",
		config: transformation.Config{Database: "proc", Table: "main"},
		handler: &placeholderHandler{
			mockHandler: mockHandler{
				dependencies: []string{"raw.blocks", "raw.txs", "proc.summary"},
			},
			original: []transformation.Dependency{
				{IsGroup: false, SingleDep: "{{external}}.blocks"},
				{IsGroup: false, SingleDep: "{{external}}.txs"},
				{IsGroup: false, SingleDep: "{{transformation}}.summary"},
			},
		},
		value: `{{ index .dep "raw" "blocks" "table" }}
{{ index .dep "raw" "txs" "table" }}
{{ index .dep "{{external}}" "blocks" "table" }}
{{ index .dep "{{transformation}}" "summary" "table" }}`,
	}

	require.NoError(t, dag.BuildGraph(
		[]Transformation{model, transDep},
		[]External{ext1, ext2},
	))

	engine := NewTemplateEngine(newChCfg(), dag, nil)
	out, err := engine.RenderTransformation(model, 0, 1, time.Now())
	require.NoError(t, err)
	assert.Contains(t, out, "blocks")
	assert.Contains(t, out, "txs")
	assert.Contains(t, out, "summary")
}

func TestProcessSingleDependencyIDUnknownNodeType(t *testing.T) {
	dag := NewDependencyGraph()
	// Inject a Node with an unrecognized NodeType to hit the default switch branch.
	require.NoError(t, dag.dag.AddVertexByID("db.weird", Node{NodeType: NodeType("weird"), Model: nil}))

	engine := NewTemplateEngine(newChCfg(), dag, nil)
	err := engine.processSingleDependencyID("db.weird", "db.weird", func(_, _ string, _ map[string]any) {})
	require.NoError(t, err)
}

func TestProcessTransformationDependencyWrongModel(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	node := Node{NodeType: NodeTypeTransformation, Model: "not-a-transformation"}
	err := engine.processTransformationDependency(node, "db.x", "db.x", func(_, _ string, _ map[string]any) {})
	require.ErrorIs(t, err, ErrNotTransformationModel)
}

func TestProcessExternalDependencyWrongModel(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	node := Node{NodeType: NodeTypeExternal, Model: "not-an-external"}
	err := engine.processExternalDependency(node, "db.x", "db.x", func(_, _ string, _ map[string]any) {})
	require.ErrorIs(t, err, ErrNotExternalModel)
}

func TestGetTransformationEnvironmentVariablesGlobalEnv(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, map[string]string{"GLOBAL_KEY": "global_val"})

	model := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{}},
		value:   "SELECT 1",
	}

	env, err := engine.GetTransformationEnvironmentVariables(model, 0, 1, time.Now())
	require.NoError(t, err)
	assert.Contains(t, *env, "GLOBAL_KEY=global_val")
}

func TestGetTransformationEnvironmentVariablesMissingDep(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	model := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{"db.missing"}},
		value:   "SELECT 1",
	}

	_, err := engine.GetTransformationEnvironmentVariables(model, 0, 1, time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get dependency")
}

func TestGetTransformationEnvironmentVariablesWrongModelTypes(t *testing.T) {
	dag := NewDependencyGraph()
	// Inject nodes whose NodeType disagrees with their Model type.
	require.NoError(t, dag.dag.AddVertexByID("db.bad_trans", Node{NodeType: NodeTypeTransformation, Model: "x"}))
	require.NoError(t, dag.dag.AddVertexByID("db.bad_ext", Node{NodeType: NodeTypeExternal, Model: "x"}))

	engine := NewTemplateEngine(newChCfg(), dag, nil)

	modelTrans := &mockTransformation{
		id:      "db.t",
		config:  transformation.Config{Database: "db", Table: "t"},
		handler: &mockHandler{dependencies: []string{"db.bad_trans"}},
	}
	_, err := engine.GetTransformationEnvironmentVariables(modelTrans, 0, 1, time.Now())
	require.ErrorIs(t, err, ErrNotTransformationModel)

	modelExt := &mockTransformation{
		id:      "db.t2",
		config:  transformation.Config{Database: "db", Table: "t2"},
		handler: &mockHandler{dependencies: []string{"db.bad_ext"}},
	}
	_, err = engine.GetTransformationEnvironmentVariables(modelExt, 0, 1, time.Now())
	require.ErrorIs(t, err, ErrNotExternalModel)
}

func TestBuildExternalVariablesWithCacheState(t *testing.T) {
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(newChCfg(), dag, nil)

	model := &mockExternal{
		id:     "db.ext",
		typ:    external.TypeSQL,
		config: external.Config{Database: "db", Table: "ext"},
		value:  `{{ .cache.last_position }}`,
	}

	out, err := engine.RenderExternal(model, map[string]any{"last_position": 4242})
	require.NoError(t, err)
	assert.Contains(t, out, "4242")
}
