// Package rendering provides template rendering functionality for model SQL transformations
package rendering

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
)

// TemplateEngine provides template rendering with Sprig functions
type TemplateEngine struct {
	funcMap template.FuncMap
}

// NewTemplateEngine creates a new template engine with Sprig functions
func NewTemplateEngine() *TemplateEngine {
	return &TemplateEngine{
		funcMap: sprig.TxtFuncMap(),
	}
}

// Render renders a template with the given variables
func (t *TemplateEngine) Render(content string, variables map[string]interface{}) (string, error) {
	tmpl, err := template.New("model").Funcs(t.funcMap).Parse(content)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// buildDependencyMap builds a map structure for dependency references
func buildDependencyMap(dependencies []string) map[string]interface{} {
	dep := make(map[string]interface{})
	for _, depID := range dependencies {
		parts := strings.Split(depID, ".")
		if len(parts) != 2 {
			continue
		}

		db, tbl := parts[0], parts[1]
		if dep[db] == nil {
			dep[db] = make(map[string]interface{})
		}

		// Need to get actual dependency config here - will be injected by caller
		if depMap, ok := dep[db].(map[string]interface{}); ok {
			depMap[tbl] = map[string]interface{}{
				"database":  db,
				"table":     tbl,
				"partition": "slot_start_date_time", // This should come from actual dep config
			}
		}
	}
	return dep
}

// BuildVariables builds template variables from configuration and task context
func (t *TemplateEngine) BuildVariables(clickhouseCfg *clickhouse.Config, modelConfig *models.ModelConfig, position, interval uint64, startTime time.Time) map[string]interface{} {
	variables := map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"cluster":      clickhouseCfg.Cluster,
			"local_suffix": clickhouseCfg.LocalSuffix,
		},
		"self": map[string]interface{}{
			"database":  modelConfig.Database,
			"table":     modelConfig.Table,
			"partition": modelConfig.Partition,
			"interval":  modelConfig.Interval,
		},
		"task": map[string]interface{}{
			"start": startTime.Unix(),
		},
		"range": map[string]interface{}{
			"start": position,
			"end":   position + interval,
		},
	}

	// Add dependency references
	if len(modelConfig.Dependencies) > 0 {
		dep := buildDependencyMap(modelConfig.Dependencies)
		variables["dep"] = dep
	}

	return variables
}

// BuildEnvironmentVariables builds environment variables for exec models
func BuildEnvironmentVariables(clickhouseCfg *clickhouse.Config, modelConfig *models.ModelConfig, position, interval uint64, startTime time.Time) []string {
	env := []string{
		fmt.Sprintf("CLICKHOUSE_URL=%s", clickhouseCfg.URL),
		fmt.Sprintf("SELF_DATABASE=%s", modelConfig.Database),
		fmt.Sprintf("SELF_TABLE=%s", modelConfig.Table),
		fmt.Sprintf("SELF_PARTITION=%s", modelConfig.Partition),
		fmt.Sprintf("TASK_START=%d", startTime.Unix()),
		fmt.Sprintf("TASK_MODEL=%s.%s", modelConfig.Database, modelConfig.Table),
		fmt.Sprintf("TASK_INTERVAL=%d", interval),
		fmt.Sprintf("RANGE_START=%d", position),
		fmt.Sprintf("RANGE_END=%d", position+interval),
	}

	if clickhouseCfg.Cluster != "" {
		env = append(env,
			fmt.Sprintf("CLICKHOUSE_CLUSTER=%s", clickhouseCfg.Cluster),
			fmt.Sprintf("CLICKHOUSE_LOCAL_SUFFIX=%s", clickhouseCfg.LocalSuffix))
	}

	// Add dependency environment variables
	for _, depID := range modelConfig.Dependencies {
		parts := strings.Split(depID, ".")
		if len(parts) == 2 {
			db, tbl := parts[0], parts[1]
			envPrefix := fmt.Sprintf("DEP_%s_%s",
				strings.ToUpper(strings.ReplaceAll(db, ".", "_")),
				strings.ToUpper(strings.ReplaceAll(tbl, ".", "_")))
			env = append(env,
				fmt.Sprintf("%s_DATABASE=%s", envPrefix, db),
				fmt.Sprintf("%s_TABLE=%s", envPrefix, tbl),
				fmt.Sprintf("%s_PARTITION=%s", envPrefix, "slot_start_date_time"), // From actual dep config
			)
		}
	}

	return env
}
