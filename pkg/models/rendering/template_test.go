package rendering

import (
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplateEngine_Render(t *testing.T) {
	engine := NewTemplateEngine()

	tests := []struct {
		name      string
		template  string
		variables map[string]interface{}
		expected  string
		hasError  bool
	}{
		{
			name:     "simple variable substitution",
			template: "SELECT * FROM {{.database}}.{{.table}}",
			variables: map[string]interface{}{
				"database": "mydb",
				"table":    "mytable",
			},
			expected: "SELECT * FROM mydb.mytable",
			hasError: false,
		},
		{
			name:     "nested variable access",
			template: "SELECT * FROM {{.self.database}}.{{.self.table}} WHERE timestamp >= {{.range.start}}",
			variables: map[string]interface{}{
				"self": map[string]interface{}{
					"database": "testdb",
					"table":    "testtable",
				},
				"range": map[string]interface{}{
					"start": 1000,
					"end":   2000,
				},
			},
			expected: "SELECT * FROM testdb.testtable WHERE timestamp >= 1000",
			hasError: false,
		},
		{
			name:     "sprig function usage",
			template: `SELECT * FROM {{.table | upper}} WHERE id IN ({{range $i, $v := .ids}}{{if $i}}, {{end}}'{{$v}}'{{end}})`,
			variables: map[string]interface{}{
				"table": "users",
				"ids":   []string{"id1", "id2", "id3"},
			},
			expected: "SELECT * FROM USERS WHERE id IN ('id1', 'id2', 'id3')",
			hasError: false,
		},
		{
			name:     "arithmetic operations",
			template: "SELECT * FROM table WHERE slot >= {{.start}} AND slot < {{add .start .interval}}",
			variables: map[string]interface{}{
				"start":    1000,
				"interval": 500,
			},
			expected: "SELECT * FROM table WHERE slot >= 1000 AND slot < 1500",
			hasError: false,
		},
		{
			name:     "invalid template syntax",
			template: "SELECT * FROM {{.database",
			variables: map[string]interface{}{
				"database": "mydb",
			},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing variable",
			template: "SELECT * FROM {{.missing}}",
			variables: map[string]interface{}{
				"database": "mydb",
			},
			expected: "SELECT * FROM <no value>",
			hasError: false,
		},
		{
			name:     "conditional logic",
			template: `{{if .usePartition}}PARTITION BY {{.partition}}{{else}}NO PARTITION{{end}}`,
			variables: map[string]interface{}{
				"usePartition": true,
				"partition":    "timestamp",
			},
			expected: "PARTITION BY timestamp",
			hasError: false,
		},
		{
			name:     "string manipulation with sprig",
			template: `{{.prefix | trimPrefix "pre_"}} {{.suffix | trimSuffix "_post"}}`,
			variables: map[string]interface{}{
				"prefix": "pre_value",
				"suffix": "data_post",
			},
			expected: "value data",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Render(tt.template, tt.variables)

			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestTemplateEngine_BuildVariables(t *testing.T) {
	engine := NewTemplateEngine()

	clickhouseCfg := &clickhouse.Config{
		URL:         "http://localhost:8123",
		Cluster:     "cluster1",
		LocalSuffix: "_local",
	}

	modelConfig := &models.ModelConfig{
		Database:     "mydb",
		Table:        "mytable",
		Partition:    "timestamp",
		Interval:     3600,
		Dependencies: []string{"dep_db.dep_table1", "dep_db.dep_table2"},
	}

	startTime := time.Unix(1700000000, 0)
	position := uint64(1000)
	interval := uint64(3600)

	variables := engine.BuildVariables(clickhouseCfg, modelConfig, position, interval, startTime)

	// Check clickhouse variables
	ch, ok := variables["clickhouse"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "cluster1", ch["cluster"])
	assert.Equal(t, "_local", ch["local_suffix"])

	// Check self variables
	self, ok := variables["self"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "mydb", self["database"])
	assert.Equal(t, "mytable", self["table"])
	assert.Equal(t, "timestamp", self["partition"])
	assert.Equal(t, uint64(3600), self["interval"])

	// Check task variables
	task, ok := variables["task"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, int64(1700000000), task["start"])

	// Check range variables
	rng, ok := variables["range"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, uint64(1000), rng["start"])
	assert.Equal(t, uint64(4600), rng["end"])

	// Check dependency variables
	dep, ok := variables["dep"].(map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, dep)
}

func TestBuildEnvironmentVariables(t *testing.T) {
	clickhouseCfg := &clickhouse.Config{
		URL:         "http://localhost:8123",
		Cluster:     "cluster1",
		LocalSuffix: "_local",
	}

	modelConfig := &models.ModelConfig{
		Database:     "mydb",
		Table:        "mytable",
		Partition:    "timestamp",
		Dependencies: []string{"dep_db.dep_table", "other_db.other_table"},
	}

	startTime := time.Unix(1700000000, 0)
	position := uint64(1000)
	interval := uint64(3600)

	env := BuildEnvironmentVariables(clickhouseCfg, modelConfig, position, interval, startTime)

	// Check required environment variables are present
	expectedVars := map[string]string{
		"CLICKHOUSE_URL":          "http://localhost:8123",
		"SELF_DATABASE":           "mydb",
		"SELF_TABLE":              "mytable",
		"SELF_PARTITION":          "timestamp",
		"TASK_START":              "1700000000",
		"TASK_MODEL":              "mydb.mytable",
		"TASK_INTERVAL":           "3600",
		"RANGE_START":             "1000",
		"RANGE_END":               "4600",
		"CLICKHOUSE_CLUSTER":      "cluster1",
		"CLICKHOUSE_LOCAL_SUFFIX": "_local",
	}

	for key, expectedValue := range expectedVars {
		found := false
		for _, e := range env {
			if strings.HasPrefix(e, key+"=") {
				actualValue := strings.TrimPrefix(e, key+"=")
				assert.Equal(t, expectedValue, actualValue, "Mismatch for %s", key)
				found = true
				break
			}
		}
		assert.True(t, found, "Missing environment variable: %s", key)
	}

	// Check dependency environment variables
	depVars := []string{
		"DEP_DEP_DB_DEP_TABLE_DATABASE=dep_db",
		"DEP_DEP_DB_DEP_TABLE_TABLE=dep_table",
		"DEP_OTHER_DB_OTHER_TABLE_DATABASE=other_db",
		"DEP_OTHER_DB_OTHER_TABLE_TABLE=other_table",
	}

	for _, expectedVar := range depVars {
		assert.Contains(t, env, expectedVar)
	}
}

func TestBuildDependencyMap(t *testing.T) {
	dependencies := []string{
		"db1.table1",
		"db1.table2",
		"db2.table3",
	}

	depMap := buildDependencyMap(dependencies)

	// Check structure
	db1, ok := depMap["db1"].(map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, db1["table1"])
	assert.NotNil(t, db1["table2"])

	db2, ok := depMap["db2"].(map[string]interface{})
	require.True(t, ok)
	assert.NotNil(t, db2["table3"])

	// Check table details
	table1, ok := db1["table1"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "db1", table1["database"])
	assert.Equal(t, "table1", table1["table"])
	assert.Equal(t, "slot_start_date_time", table1["partition"])
}

func TestTemplateEngine_ComplexTemplate(t *testing.T) {
	engine := NewTemplateEngine()

	// Complex template similar to what might be used in production
	template := `
INSERT INTO {{.self.database}}.{{.self.table}}_{{.clickhouse.local_suffix}}
SELECT 
    toStartOfInterval(timestamp, INTERVAL {{.self.interval}} SECOND) as interval_start,
    count() as record_count,
    sum(value) as total_value
FROM (
    {{range $db, $tables := .dep}}
    {{range $table, $config := $tables}}
    SELECT * FROM {{$config.database}}.{{$config.table}}
    WHERE {{$config.partition}} >= {{$.range.start}}
      AND {{$config.partition}} < {{$.range.end}}
    {{end}}
    {{end}}
)
GROUP BY interval_start
SETTINGS max_threads = 8`

	variables := map[string]interface{}{
		"self": map[string]interface{}{
			"database": "analytics",
			"table":    "aggregated",
			"interval": 3600,
		},
		"clickhouse": map[string]interface{}{
			"local_suffix": "_local",
		},
		"range": map[string]interface{}{
			"start": 1000,
			"end":   4600,
		},
		"dep": map[string]interface{}{
			"source_db": map[string]interface{}{
				"events": map[string]interface{}{
					"database":  "source_db",
					"table":     "events",
					"partition": "event_time",
				},
			},
		},
	}

	result, err := engine.Render(template, variables)
	require.NoError(t, err)

	// Check key parts of the rendered template
	assert.Contains(t, result, "INSERT INTO analytics.aggregated__local")
	assert.Contains(t, result, "INTERVAL 3600 SECOND")
	assert.Contains(t, result, "SELECT * FROM source_db.events")
	assert.Contains(t, result, "WHERE event_time >= 1000")
	assert.Contains(t, result, "AND event_time < 4600")
	assert.Contains(t, result, "SETTINGS max_threads = 8")
}

func TestBuildEnvironmentVariables_NoCluster(t *testing.T) {
	clickhouseCfg := &clickhouse.Config{
		URL: "http://localhost:8123",
		// No cluster specified
	}

	modelConfig := &models.ModelConfig{
		Database:  "mydb",
		Table:     "mytable",
		Partition: "timestamp",
	}

	startTime := time.Unix(1700000000, 0)
	position := uint64(1000)
	interval := uint64(3600)

	env := BuildEnvironmentVariables(clickhouseCfg, modelConfig, position, interval, startTime)

	// Should not contain cluster variables
	for _, e := range env {
		assert.False(t, strings.HasPrefix(e, "CLICKHOUSE_CLUSTER="))
		assert.False(t, strings.HasPrefix(e, "CLICKHOUSE_LOCAL_SUFFIX="))
	}

	// Should still contain other variables
	assert.Contains(t, env, "CLICKHOUSE_URL=http://localhost:8123")
	assert.Contains(t, env, "SELF_DATABASE=mydb")
}

func TestBuildDependencyMap_InvalidDependency(t *testing.T) {
	dependencies := []string{
		"valid.table",
		"invalidformat",    // No dot separator
		"also_valid.table", // Use underscore instead of dot for valid format
	}

	depMap := buildDependencyMap(dependencies)

	// Should have valid entries
	assert.NotNil(t, depMap["valid"])
	assert.NotNil(t, depMap["also_valid"])

	// Invalid format should be skipped
	assert.Nil(t, depMap["invalidformat"])
}
