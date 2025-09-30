package models

import (
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock transformation with template value
type mockTransformationWithTemplate struct {
	id           string
	dependencies []string
	config       transformation.Config
	value        string
}

func (m *mockTransformationWithTemplate) GetID() string                     { return m.id }
func (m *mockTransformationWithTemplate) GetDependencies() []string         { return m.dependencies }
func (m *mockTransformationWithTemplate) GetConfig() *transformation.Config { return &m.config }
func (m *mockTransformationWithTemplate) GetSQL() string                    { return "" }
func (m *mockTransformationWithTemplate) GetType() string                   { return "transformation" }
func (m *mockTransformationWithTemplate) GetValue() string                  { return m.value }
func (m *mockTransformationWithTemplate) GetEnvironmentVariables() []string { return []string{} }
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
func (m *mockExternalWithTemplate) SetDefaultDatabase(defaultDB string) {
	m.config.SetDefaults(defaultDB)
}

func TestTemplateEngineDualKeyWithRealSQL(t *testing.T) {
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

	// Create transformation with SQL using placeholder syntax
	transformModel := &mockTransformationWithTemplate{
		id:           "processed.block_summary",
		dependencies: []string{"{{external}}.blocks"},
		config: transformation.Config{
			Database:             "processed",
			Table:                "block_summary",
			Dependencies:         []transformation.Dependency{{IsGroup: false, SingleDep: "{{external}}.blocks"}},
			OriginalDependencies: []transformation.Dependency{{IsGroup: false, SingleDep: "{{external}}.blocks"}},
		},
		value: `INSERT INTO {{ .self.database }}.{{ .self.table }}
SELECT 
    block_number,
    block_hash,
    count(*) as tx_count
FROM {{ index .dep "{{external}}" "blocks" "database" }}.{{ index .dep "{{external}}" "blocks" "table" }}
WHERE block_timestamp BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY block_number, block_hash`,
	}

	// Substitute placeholders
	transformModel.config.SubstituteDependencyPlaceholders("raw_data", "processed")

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
	engine := NewTemplateEngine(clickhouseCfg, dag)

	// Render the template
	rendered, err := engine.RenderTransformation(transformModel, 1000, 100, time.Now())
	require.NoError(t, err)

	// Verify the SQL was rendered correctly with the resolved database
	assert.Contains(t, rendered, "INSERT INTO processed.block_summary")
	assert.Contains(t, rendered, "FROM raw_data.blocks")
	assert.Contains(t, rendered, "WHERE block_timestamp BETWEEN 1000 AND 1100")
}

func TestTemplateEngineDualKeyDependencyAccess(t *testing.T) {
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
		id:           "analytics.hourly_stats",
		dependencies: []string{},
		config: transformation.Config{
			Database: "analytics",
			Table:    "hourly_stats",
		},
		value: "SELECT * FROM stats",
	}

	// Main transformation with placeholder dependencies
	mainTransformModel := &mockTransformationWithTemplate{
		id:           "analytics.test_transform",
		dependencies: []string{"{{external}}.beacon_blocks", "{{transformation}}.hourly_stats"},
		config: transformation.Config{
			Database: "analytics",
			Table:    "test_transform",
			Dependencies: []transformation.Dependency{
				{IsGroup: false, SingleDep: "{{external}}.beacon_blocks"},
				{IsGroup: false, SingleDep: "{{transformation}}.hourly_stats"},
			},
			OriginalDependencies: []transformation.Dependency{
				{IsGroup: false, SingleDep: "{{external}}.beacon_blocks"},
				{IsGroup: false, SingleDep: "{{transformation}}.hourly_stats"},
			},
		},
		value: `Placeholder access: {{ index .dep "{{external}}" "beacon_blocks" "database" }}.{{ index .dep "{{external}}" "beacon_blocks" "table" }}
Resolved access: {{ index .dep "ethereum" "beacon_blocks" "database" }}.{{ index .dep "ethereum" "beacon_blocks" "table" }}
Transformation placeholder: {{ index .dep "{{transformation}}" "hourly_stats" "database" }}.{{ index .dep "{{transformation}}" "hourly_stats" "table" }}
Transformation resolved: {{ index .dep "analytics" "hourly_stats" "database" }}.{{ index .dep "analytics" "hourly_stats" "table" }}`,
	}

	// Substitute placeholders
	mainTransformModel.config.SubstituteDependencyPlaceholders("ethereum", "analytics")

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
	engine := NewTemplateEngine(clickhouseCfg, dag)

	// Render the template
	rendered, err := engine.RenderTransformation(mainTransformModel, 1000, 100, time.Now())
	require.NoError(t, err)

	// Verify both placeholder and resolved forms work
	assert.Contains(t, rendered, "Placeholder access: ethereum.beacon_blocks")
	assert.Contains(t, rendered, "Resolved access: ethereum.beacon_blocks")
	assert.Contains(t, rendered, "Transformation placeholder: analytics.hourly_stats")
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

	engine := NewTemplateEngine(chConfig, dag)

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
		id: "dep.model1",
		config: transformation.Config{
			Database: "dep_db",
			Table:    "model1",
			Interval: &transformation.IntervalConfig{
				Max: 100,
				Min: 0,
			},
			Schedules: &transformation.SchedulesConfig{
				ForwardFill: "@every 1m",
			},
			Dependencies: []transformation.Dependency{},
		},
	}

	// Build DAG
	err := dag.BuildGraph([]Transformation{dep1}, []External{})
	require.NoError(t, err)

	engine := NewTemplateEngine(chConfig, dag)

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
				id: "test.model",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table",
					Interval: &transformation.IntervalConfig{
						Max: 100,
						Min: 0,
					},
					Schedules: &transformation.SchedulesConfig{
						ForwardFill: "@every 1m",
					},
					Dependencies: []transformation.Dependency{},
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
				id: "test.model2",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table2",
					Interval: &transformation.IntervalConfig{
						Max: 100,
						Min: 0,
					},
					Schedules: &transformation.SchedulesConfig{
						ForwardFill: "@every 1m",
					},
					Dependencies: []transformation.Dependency{
						{IsGroup: false, SingleDep: "dep.model1"},
					},
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
				id: "test.model3",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table3",
					Interval: &transformation.IntervalConfig{
						Max: 100,
						Min: 0,
					},
					Schedules: &transformation.SchedulesConfig{
						ForwardFill: "@every 1m",
					},
					Dependencies: []transformation.Dependency{},
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
				id: "test.model4",
				config: transformation.Config{
					Database: "test_db",
					Table:    "test_table4",
					Interval: &transformation.IntervalConfig{
						Max: 100,
						Min: 0,
					},
					Schedules: &transformation.SchedulesConfig{
						ForwardFill: "@every 1m",
					},
					Dependencies: []transformation.Dependency{},
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
	engine := NewTemplateEngine(chConfig, dag)

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
		id: "dep.model1",
		config: transformation.Config{
			Database: "dep_db",
			Table:    "model1",
			Interval: &transformation.IntervalConfig{
				Max: 100,
				Min: 0,
			},
			Schedules: &transformation.SchedulesConfig{
				ForwardFill: "@every 1m",
			},
			Dependencies: []transformation.Dependency{},
		},
	}

	ext1 := &mockExternalWithTemplate{
		id: "ext.source1",
		config: external.Config{
			Database: "ext_db",
			Table:    "source1",
		},
		typ: external.ExternalTypeSQL,
	}

	// Build DAG
	err := dag.BuildGraph([]Transformation{dep1}, []External{ext1})
	require.NoError(t, err)

	engine := NewTemplateEngine(chConfig, dag)

	model := &mockTransformationWithTemplate{
		id: "test.model",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
			Interval: &transformation.IntervalConfig{
				Max: 100,
				Min: 0,
			},
			Schedules: &transformation.SchedulesConfig{
				ForwardFill: "@every 1m",
			},
			Dependencies: []transformation.Dependency{
				{IsGroup: false, SingleDep: "dep.model1"},
				{IsGroup: false, SingleDep: "ext.source1"},
			},
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
		"DEP_DEP_MODEL1_DATABASE=dep_db",
		"DEP_DEP_MODEL1_TABLE=model1",
		"DEP_EXT_SOURCE1_DATABASE=ext_db",
		"DEP_EXT_SOURCE1_TABLE=source1",
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

// Test buildTransformationVariables with missing dependency
func TestBuildTransformationVariables_MissingDependency(t *testing.T) {
	chConfig := &clickhouse.Config{
		Cluster:     "test_cluster",
		LocalSuffix: "_local",
		URL:         "http://localhost:8123",
	}
	dag := NewDependencyGraph()
	engine := NewTemplateEngine(chConfig, dag)

	model := &mockTransformationWithTemplate{
		id: "test.model",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
			Interval: &transformation.IntervalConfig{
				Max: 100,
				Min: 0,
			},
			Schedules: &transformation.SchedulesConfig{
				ForwardFill: "@every 1m",
			},
			Dependencies: []transformation.Dependency{
				{IsGroup: false, SingleDep: "missing.dep"},
			},
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
	engine := NewTemplateEngine(chConfig, dag)

	model := &mockTransformationWithTemplate{
		id: "test.model",
		config: transformation.Config{
			Database: "test_db",
			Table:    "test_table",
			Interval: &transformation.IntervalConfig{
				Max: 100,
				Min: 0,
			},
			Schedules: &transformation.SchedulesConfig{
				ForwardFill: "@every 1m",
			},
			Dependencies: []transformation.Dependency{},
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
	engine := NewTemplateEngine(chConfig, dag)

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
	engine := NewTemplateEngine(clickhouseCfg, dag)

	// Create a mock transformation with hyphenated database
	mockTransform := &mockTransformationWithTemplate{
		config: transformation.Config{
			Database: "analytics-db",
			Table:    "hourly_stats",
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

// TestPlaceholderSubstitutionWithHyphenatedDatabases tests placeholder substitution with hyphenated database names
func TestPlaceholderSubstitutionWithHyphenatedDatabases(t *testing.T) {
	config := &transformation.Config{
		Database: "target-db",
		Table:    "processed",
		Dependencies: []transformation.Dependency{
			{SingleDep: "{{external}}.raw_data", IsGroup: false},
			{SingleDep: "{{transformation}}.hourly", IsGroup: false},
		},
	}

	// Apply substitution with hyphenated database names
	config.SubstituteDependencyPlaceholders("source-db", "analytics-db")

	// Check that placeholders were correctly replaced
	assert.Equal(t, "source-db.raw_data", config.Dependencies[0].SingleDep)
	assert.Equal(t, "analytics-db.hourly", config.Dependencies[1].SingleDep)

	// Check that original dependencies were preserved
	assert.Equal(t, "{{external}}.raw_data", config.OriginalDependencies[0].SingleDep)
	assert.Equal(t, "{{transformation}}.hourly", config.OriginalDependencies[1].SingleDep)
}
