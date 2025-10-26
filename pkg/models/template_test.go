package models

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock handler for template tests
type mockTemplateHandler struct {
	dependencies []string
}

func (h *mockTemplateHandler) GetFlattenedDependencies() []string {
	return h.dependencies
}

func (h *mockTemplateHandler) Type() transformation.Type { return "incremental" }
func (h *mockTemplateHandler) Config() any               { return nil }
func (h *mockTemplateHandler) Validate() error           { return nil }
func (h *mockTemplateHandler) ShouldTrackPosition() bool { return true }
func (h *mockTemplateHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return nil
}
func (h *mockTemplateHandler) GetAdminTable() transformation.AdminTable {
	return transformation.AdminTable{}
}
func (h *mockTemplateHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}

// Mock transformation with template value
type mockTransformationWithTemplate struct {
	id      string
	config  transformation.Config
	handler transformation.Handler
	value   string
}

func (m *mockTransformationWithTemplate) GetID() string                      { return m.id }
func (m *mockTransformationWithTemplate) GetConfig() *transformation.Config  { return &m.config }
func (m *mockTransformationWithTemplate) GetSQL() string                     { return "" }
func (m *mockTransformationWithTemplate) GetType() string                    { return "transformation" }
func (m *mockTransformationWithTemplate) GetValue() string                   { return m.value }
func (m *mockTransformationWithTemplate) GetHandler() transformation.Handler { return m.handler }
func (m *mockTransformationWithTemplate) SetDefaultDatabase(defaultDB string) {
	m.config.SetDefaults(defaultDB)
}

// Mock external with template value
type mockExternalWithTemplate struct {
	id     string
	config external.Config
	typ    string
	value  string
}

func (m *mockExternalWithTemplate) GetID() string                      { return m.id }
func (m *mockExternalWithTemplate) GetConfig() external.Config         { return m.config }
func (m *mockExternalWithTemplate) GetConfigMutable() *external.Config { return &m.config }
func (m *mockExternalWithTemplate) GetType() string                    { return m.typ }
func (m *mockExternalWithTemplate) GetSQL() string                     { return "" }
func (m *mockExternalWithTemplate) GetValue() string                   { return m.value }
func (m *mockExternalWithTemplate) GetEnvironmentVariables() []string  { return []string{} }
func (m *mockExternalWithTemplate) SetDefaults(defaultCluster, defaultDB string) {
	m.config.SetDefaults(defaultCluster, defaultDB)
}

func TestTemplateEngineWithRealSQL(t *testing.T) {
	// Create a dependency graph
	dag := NewDependencyGraph()

	// Create external model
	externalModel := &mockExternalWithTemplate{
		id:    "raw_data.blocks",
		typ:   "external",
		value: "SELECT * FROM blocks",
		config: external.Config{
			Database: "raw_data",
			Table:    "blocks",
		},
	}

	// Create transformation with SQL
	transformModel := &mockTransformationWithTemplate{
		id: "processed.block_summary",
		config: transformation.Config{
			Database: "processed",
			Table:    "block_summary",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{"raw_data.blocks"},
		},
		value: `INSERT INTO {{ .self.database }}.{{ .self.table }}
SELECT 
    block_number,
    block_hash,
    count(*) as tx_count
FROM {{ index .dep "raw_data" "blocks" "database" }}.{{ index .dep "raw_data" "blocks" "table" }}
WHERE block_timestamp BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY block_number, block_hash`,
	}

	// Build the graph
	transformations := []Transformation{transformModel}
	externals := []External{externalModel}
	err := dag.BuildGraph(transformations, externals)
	require.NoError(t, err)

	// Create template engine
	clickhouseCfg := &clickhouse.Config{
		Cluster:     "",
		LocalSuffix: "",
	}
	engine := NewTemplateEngine(clickhouseCfg, dag, nil)

	// Render the template
	rendered, err := engine.RenderTransformation(transformModel, 1000, 100, time.Now())
	require.NoError(t, err)

	// Verify the SQL was rendered correctly with the resolved database
	assert.Contains(t, rendered, "INSERT INTO processed.block_summary")
	assert.Contains(t, rendered, "FROM raw_data.blocks")
	assert.Contains(t, rendered, "WHERE block_timestamp BETWEEN 1000 AND 1100")
}

func TestTemplateEngineDependencyAccess(t *testing.T) {
	// Create a dependency graph
	dag := NewDependencyGraph()

	// Create external model that will be referenced
	externalModel := &mockExternalWithTemplate{
		id:    "ethereum.beacon_blocks",
		typ:   "external",
		value: "SELECT * FROM blocks",
		config: external.Config{
			Database: "ethereum",
			Table:    "beacon_blocks",
		},
	}

	// Create transformation model that will be referenced
	refTransformModel := &mockTransformationWithTemplate{
		id: "analytics.hourly_stats",
		config: transformation.Config{
			Database: "analytics",
			Table:    "hourly_stats",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{},
		},
		value: "SELECT * FROM stats",
	}

	// Main transformation with resolved dependencies
	mainTransformModel := &mockTransformationWithTemplate{
		id: "analytics.test_transform",
		config: transformation.Config{
			Database: "analytics",
			Table:    "test_transform",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{"ethereum.beacon_blocks", "analytics.hourly_stats"},
		},
		value: `Resolved access: {{ index .dep "ethereum" "beacon_blocks" "database" }}.{{ index .dep "ethereum" "beacon_blocks" "table" }}
Transformation resolved: {{ index .dep "analytics" "hourly_stats" "database" }}.{{ index .dep "analytics" "hourly_stats" "table" }}`,
	}

	// Build the graph
	transformations := []Transformation{mainTransformModel, refTransformModel}
	externals := []External{externalModel}
	err := dag.BuildGraph(transformations, externals)
	require.NoError(t, err)

	// Create template engine
	clickhouseCfg := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}
	engine := NewTemplateEngine(clickhouseCfg, dag, nil)

	// Render the template
	rendered, err := engine.RenderTransformation(mainTransformModel, 1000, 100, time.Now())
	require.NoError(t, err)

	// Verify both forms work
	assert.Contains(t, rendered, "Resolved access: ethereum.beacon_blocks")
	assert.Contains(t, rendered, "Transformation resolved: analytics.hourly_stats")
}

// Test NewTemplateEngine
func TestNewTemplateEngine(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()

	engine := NewTemplateEngine(chConfig, dag, nil)

	assert.NotNil(t, engine)
	assert.NotNil(t, engine.funcMap)
	assert.Equal(t, dag, engine.dag)
	assert.Equal(t, chConfig, engine.clickhouseCfg)
}

// Test RenderTransformation
func TestRenderTransformation(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()

	// Setup dependencies
	dep1 := &mockTransformationWithTemplate{
		id: "dep_db.model1",
		config: transformation.Config{
			Database: "dep_db",
			Table:    "model1",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{},
		},
	}

	// Build DAG
	err := dag.BuildGraph([]Transformation{dep1}, []External{})
	require.NoError(t, err)

	engine := NewTemplateEngine(chConfig, dag, nil)

	tests := []struct {
		name        string
		model       Transformation
		position    uint64
		interval    uint64
		expectedErr bool
		contains    []string
	}{
		{
			name: "simple template rendering",
			model: &mockTransformationWithTemplate{
				id: "test_db.test_table",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table",
				},
				handler: &mockTemplateHandler{
					dependencies: []string{},
				},
				value: "SELECT * FROM {{ .self.database }}.{{ .self.table }} WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}",
			},
			position:    1000,
			interval:    100,
			expectedErr: false,
			contains:    []string{"test_db", "test_table", "1000", "1100"},
		},
		{
			name: "template with dependencies",
			model: &mockTransformationWithTemplate{
				id: "test_db.test_table2",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table2",
				},
				handler: &mockTemplateHandler{
					dependencies: []string{"dep_db.model1"},
				},
				value: "SELECT * FROM {{ .dep.dep_db.model1.database }}.{{ .dep.dep_db.model1.table }}",
			},
			position:    2000,
			interval:    200,
			expectedErr: false,
			contains:    []string{"dep_db", "model1"},
		},
		{
			name: "template with sprig functions",
			model: &mockTransformationWithTemplate{
				id: "test_db.test_table3",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table3",
				},
				handler: &mockTemplateHandler{
					dependencies: []string{},
				},
				value: "SELECT '{{ .self.table | upper }}' as table_name",
			},
			position:    3000,
			interval:    300,
			expectedErr: false,
			contains:    []string{"TEST_TABLE3"},
		},
		{
			name: "invalid template syntax",
			model: &mockTransformationWithTemplate{
				id: "test_db.test_table4",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table4",
				},
				handler: &mockTemplateHandler{
					dependencies: []string{},
				},
				value: "SELECT * FROM {{ .invalid.syntax",
			},
			position:    4000,
			interval:    400,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			result, err := engine.RenderTransformation(tt.model, tt.position, tt.interval, startTime)

			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				for _, expected := range tt.contains {
					assert.Contains(t, result, expected)
				}
			}
		})
	}
}

// Test RenderExternal
func TestRenderExternal(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(chConfig, dag, nil)

	tests := []struct {
		name        string
		model       External
		expectedErr bool
		contains    []string
	}{
		{
			name: "simple external template",
			model: &mockExternalWithTemplate{
				id: "ext.model1",
				config: external.Config{
					Database: "ext_db",
					Table:    "ext_table",
				},
				typ:   external.ExternalTypeSQL,
				value: "SELECT min(position) as min, max(position) as max FROM {{ .self.database }}.{{ .self.table }}",
			},
			expectedErr: false,
			contains:    []string{"ext_db", "ext_table", "min", "max"},
		},
		{
			name: "external with clickhouse config",
			model: &mockExternalWithTemplate{
				id: "ext.model2",
				config: external.Config{
					Database: "ext_db2",
					Table:    "ext_table2",
				},
				typ:   external.ExternalTypeSQL,
				value: "SELECT * FROM {{ .self.database }}.{{ .self.table }}{{ .clickhouse.local_suffix }}",
			},
			expectedErr: false,
			contains:    []string{"ext_db2", "ext_table2", "_local"},
		},
		{
			name: "invalid external template",
			model: &mockExternalWithTemplate{
				id: "ext.model3",
				config: external.Config{
					Database: "ext_db3",
					Table:    "ext_table3",
				},
				typ:   external.ExternalTypeSQL,
				value: "SELECT * FROM {{ .invalid",
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.RenderExternal(tt.model, nil)

			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				for _, expected := range tt.contains {
					assert.Contains(t, result, expected)
				}
			}
		})
	}
}

// Test GetTransformationEnvironmentVariables
func TestGetTransformationEnvironmentVariables(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()

	// Setup dependencies
	dep1 := &mockTransformationWithTemplate{
		id: "dep_db.model1",
		config: transformation.Config{
			Database: "dep_db",
			Table:    "model1",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{},
		},
	}

	ext1 := &mockExternalWithTemplate{
		id: "ext_db.source1",
		config: external.Config{
			Database: "ext_db",
			Table:    "source1",
		},
		typ: external.ExternalTypeSQL,
	}

	// Build DAG
	err := dag.BuildGraph([]Transformation{dep1}, []External{ext1})
	require.NoError(t, err)

	engine := NewTemplateEngine(chConfig, dag, nil)

	model := &mockTransformationWithTemplate{
		id: "test_db.test_table",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{"dep_db.model1", "ext_db.source1"},
		},
		value: "SELECT * FROM test",
	}

	startTime := time.Now()
	envVars, err := engine.GetTransformationEnvironmentVariables(model, 1000, 100, startTime)
	require.NoError(t, err)
	require.NotNil(t, envVars)

	// Check expected environment variables
	expectedVars := []string{
		"CLICKHOUSE_URL=http://localhost:8123",
		"SELF_DATABASE=test_db",
		"SELF_TABLE=test_table",
		"TASK_MODEL=test_db.test_table",
		"TASK_INTERVAL=100",
		"BOUNDS_START=1000",
		"BOUNDS_END=1100",
		"CLICKHOUSE_CLUSTER=test_cluster",
		"CLICKHOUSE_LOCAL_SUFFIX=_local",
		"DEP_DEP_DB_MODEL1_DATABASE=dep_db",
		"DEP_DEP_DB_MODEL1_TABLE=model1",
		"DEP_EXT_DB_SOURCE1_DATABASE=ext_db",
		"DEP_EXT_DB_SOURCE1_TABLE=source1",
	}

	for _, expected := range expectedVars {
		found := false
		for _, actual := range *envVars {
			if strings.Contains(actual, strings.Split(expected, "=")[0]) {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected environment variable not found: %s", expected)
	}
}

// Test GetTransformationEnvironmentVariables with custom env vars
func TestGetTransformationEnvironmentVariables_CustomEnv(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(chConfig, dag, nil)

	// Create model with transformation-specific env vars
	model := &mockTransformationWithTemplate{
		id: "test_db.test_table",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
			Env: map[string]string{
				"MODEL_SPECIFIC": "value1",
				"API_KEY":        "model_override",
			},
		},
		value: "SELECT * FROM test",
	}

	startTime := time.Now()

	envVars, err := engine.GetTransformationEnvironmentVariables(model, 1000, 100, startTime)
	require.NoError(t, err)
	require.NotNil(t, envVars)

	// Convert to map for easier checking
	envMap := make(map[string]string)
	for _, v := range *envVars {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	// Check transformation-specific env vars
	assert.Equal(t, "model_override", envMap["API_KEY"], "Transformation env should be present")
	assert.Equal(t, "value1", envMap["MODEL_SPECIFIC"], "Transformation-specific env should be present")

	// Check built-in vars are still present
	assert.Equal(t, "test_db", envMap["SELF_DATABASE"], "Built-in env var should be present")
	assert.Equal(t, "test_table", envMap["SELF_TABLE"], "Built-in env var should be present")
}

// Test buildTransformationVariables with missing dependency
func TestBuildTransformationVariables_MissingDependency(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(chConfig, dag, nil)

	model := &mockTransformationWithTemplate{
		id: "test_db.test_table",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{"missing.dep"},
		},
		value: "SELECT * FROM test",
	}

	startTime := time.Now()
	_, err := engine.RenderTransformation(model, 1000, 100, startTime)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get dependency")
}

// Benchmark template rendering
func BenchmarkRenderTransformation(b *testing.B) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(chConfig, dag, nil)

	model := &mockTransformationWithTemplate{
		id: "test_db.test_table",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{},
		},
		value: "SELECT * FROM {{ .self.database }}.{{ .self.table }} WHERE position >= {{ .bounds.start }} AND position < {{ .bounds.end }}",
	}

	startTime := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.RenderTransformation(model, 1000, 100, startTime)
	}
}

func BenchmarkRenderExternal(b *testing.B) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(chConfig, dag, nil)

	model := &mockExternalWithTemplate{
		id: "ext.model",
		config: external.Config{
			Database: "ext_db",
			Table:    "ext_table",
		},
		typ:   external.ExternalTypeSQL,
		value: "SELECT min(position) as min, max(position) as max FROM {{ .self.database }}.{{ .self.table }}",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.RenderExternal(model, nil)
	}
}

// TestTemplateRenderingWithHyphenatedDatabases tests that template rendering works correctly with hyphenated database names
func TestTemplateRenderingWithHyphenatedDatabases(t *testing.T) {
	clickhouseCfg := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}

	dag := NewDependencyGraph()
	engine := NewTemplateEngine(clickhouseCfg, dag, nil)

	// Create a mock transformation with hyphenated database
	mockTransform := &mockTransformationWithTemplate{
		config: transformation.Config{
			Database: "analytics-db",
			Table:    "hourly_stats",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{},
		},
		value: "INSERT INTO `{{ .self.database }}`.`{{ .self.table }}` SELECT * FROM source",
	}

	// Render the template
	rendered, err := engine.RenderTransformation(
		mockTransform,
		1000, // position
		3600, // interval
		time.Now(),
	)

	require.NoError(t, err)
	assert.Contains(t, rendered, "`analytics-db`.`hourly_stats`")
}

// TestFromFieldWithCluster tests the 'from' field functionality with cluster configuration
func TestFromFieldWithCluster(t *testing.T) {
	dag := NewDependencyGraph()

	// Create external model with cluster
	externalModel := &mockExternalWithTemplate{
		id:    "ethereum.beacon_blocks",
		typ:   external.ExternalTypeSQL,
		value: "SELECT * FROM {{ .self.helpers.from }}",
		config: external.Config{
			Cluster:  "my_cluster",
			Database: "ethereum",
			Table:    "beacon_blocks",
		},
	}

	// Create transformation that depends on the external model
	transformModel := &mockTransformationWithTemplate{
		id: "analytics.block_summary",
		config: transformation.Config{
			Database: "analytics",
			Table:    "block_summary",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{"ethereum.beacon_blocks"},
		},
		value: "SELECT * FROM {{ index .dep \"ethereum\" \"beacon_blocks\" \"helpers\" \"from\" }}",
	}

	// Build the graph
	err := dag.BuildGraph([]Transformation{transformModel}, []External{externalModel})
	require.NoError(t, err)

	// Create template engine with cluster config
	clickhouseCfg := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
	}
	engine := NewTemplateEngine(clickhouseCfg, dag, nil)

	// Test external model rendering
	externalRendered, err := engine.RenderExternal(externalModel, nil)
	require.NoError(t, err)
	assert.Contains(t, externalRendered, "cluster('my_cluster', `ethereum`.`beacon_blocks`)")

	// Test transformation rendering with external dependency
	transformRendered, err := engine.RenderTransformation(transformModel, 1000, 100, time.Now())
	require.NoError(t, err)
	assert.Contains(t, transformRendered, "cluster('my_cluster', `ethereum`.`beacon_blocks`)")
}

// TestFromFieldWithoutCluster tests the 'from' field functionality without cluster configuration
func TestFromFieldWithoutCluster(t *testing.T) {
	dag := NewDependencyGraph()

	// Create external model without cluster
	externalModel := &mockExternalWithTemplate{
		id:    "ethereum.transactions",
		typ:   external.ExternalTypeSQL,
		value: "SELECT * FROM {{ .self.helpers.from }}",
		config: external.Config{
			Database: "ethereum",
			Table:    "transactions",
		},
	}

	// Create transformation without cluster that references another transformation
	refTransformModel := &mockTransformationWithTemplate{
		id: "analytics.hourly_stats",
		config: transformation.Config{
			Database: "analytics",
			Table:    "hourly_stats",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{},
		},
		value: "SELECT * FROM stats",
	}

	transformModel := &mockTransformationWithTemplate{
		id: "analytics.daily_summary",
		config: transformation.Config{
			Database: "analytics",
			Table:    "daily_summary",
		},
		handler: &mockTemplateHandler{
			dependencies: []string{"ethereum.transactions", "analytics.hourly_stats"},
		},
		value: `External: {{ index .dep "ethereum" "transactions" "helpers" "from" }}
Transform: {{ index .dep "analytics" "hourly_stats" "helpers" "from" }}`,
	}

	// Build the graph
	err := dag.BuildGraph([]Transformation{transformModel, refTransformModel}, []External{externalModel})
	require.NoError(t, err)

	// Create template engine without cluster config
	clickhouseCfg := &clickhouse.Config{
		Cluster:     "",
		LocalSuffix: "",
	}
	engine := NewTemplateEngine(clickhouseCfg, dag, nil)

	// Test external model rendering (should not include cluster function)
	externalRendered, err := engine.RenderExternal(externalModel, nil)
	require.NoError(t, err)
	assert.Contains(t, externalRendered, "`ethereum`.`transactions`")
	assert.NotContains(t, externalRendered, "cluster(")

	// Test transformation rendering
	transformRendered, err := engine.RenderTransformation(transformModel, 1000, 100, time.Now())
	require.NoError(t, err)
	// External dependency without cluster should be plain database.table
	assert.Contains(t, transformRendered, "External: `ethereum`.`transactions`")
	// Transformation dependency should be plain database.table
	assert.Contains(t, transformRendered, "Transform: `analytics`.`hourly_stats`")
	assert.NotContains(t, transformRendered, "cluster(")
}

// TestEnvironmentVariablesInSQLTemplates tests that env vars are accessible in SQL templates
func TestEnvironmentVariablesInSQLTemplates(t *testing.T) {
	clickhouseCfg := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}

	// Global environment variables
	globalEnv := map[string]string{
		"API_KEY":     "global_api_key_12345",
		"ENVIRONMENT": "production",
		"REGION":      "us-east-1",
	}

	dag := NewDependencyGraph()
	engine := NewTemplateEngine(clickhouseCfg, dag, globalEnv)

	t.Run("transformation with global env vars", func(t *testing.T) {
		mockTransform := &mockTransformationWithTemplate{
			config: transformation.Config{
				Database: "analytics",
				Table:    "api_calls",
			},
			handler: &mockTemplateHandler{
				dependencies: []string{},
			},
			value: `INSERT INTO {{ .self.database }}.{{ .self.table }}
SELECT '{{ .env.API_KEY }}' as api_key,
       '{{ .env.ENVIRONMENT }}' as environment,
       '{{ .env.REGION }}' as region`,
		}

		rendered, err := engine.RenderTransformation(mockTransform, 1000, 3600, time.Now())
		require.NoError(t, err)
		assert.Contains(t, rendered, "global_api_key_12345")
		assert.Contains(t, rendered, "production")
		assert.Contains(t, rendered, "us-east-1")
	})

	t.Run("transformation with model-specific env override", func(t *testing.T) {
		mockTransform := &mockTransformationWithTemplate{
			config: transformation.Config{
				Database: "analytics",
				Table:    "api_calls",
				Env: map[string]string{
					"API_KEY":   "model_override_key",
					"MODEL_VAR": "model_specific_value",
				},
			},
			handler: &mockTemplateHandler{
				dependencies: []string{},
			},
			value: `SELECT '{{ .env.API_KEY }}' as api_key,
       '{{ .env.ENVIRONMENT }}' as environment,
       '{{ .env.MODEL_VAR }}' as model_var`,
		}

		rendered, err := engine.RenderTransformation(mockTransform, 1000, 3600, time.Now())
		require.NoError(t, err)
		// Model-specific should override global
		assert.Contains(t, rendered, "model_override_key")
		assert.NotContains(t, rendered, "global_api_key_12345")
		// Global env should still be accessible
		assert.Contains(t, rendered, "production")
		// Model-specific var should be present
		assert.Contains(t, rendered, "model_specific_value")
	})

	t.Run("external model with env vars", func(t *testing.T) {
		mockExternal := &mockExternalWithTemplate{
			id: "ext.api_data",
			config: external.Config{
				Cluster:  "test_cluster",
				Database: "external",
				Table:    "api_data",
			},
			typ: external.ExternalTypeSQL,
			value: `SELECT * FROM {{ .self.database }}.{{ .self.table }}
WHERE api_key = '{{ .env.API_KEY }}'
  AND environment = '{{ .env.ENVIRONMENT }}'`,
		}

		rendered, err := engine.RenderExternal(mockExternal, nil)
		require.NoError(t, err)
		assert.Contains(t, rendered, "global_api_key_12345")
		assert.Contains(t, rendered, "production")
	})

	t.Run("no env vars configured", func(t *testing.T) {
		emptyEngine := NewTemplateEngine(clickhouseCfg, dag, nil)

		mockTransform := &mockTransformationWithTemplate{
			config: transformation.Config{
				Database: "analytics",
				Table:    "test_table",
			},
			handler: &mockTemplateHandler{
				dependencies: []string{},
			},
			value: `SELECT '{{ .env.API_KEY }}' as api_key`,
		}

		rendered, err := emptyEngine.RenderTransformation(mockTransform, 1000, 3600, time.Now())
		require.NoError(t, err)
		// When no env vars are configured, template should still work but render <no value>
		assert.Contains(t, rendered, "SELECT '<no value>' as api_key")
	})
}
