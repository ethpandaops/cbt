// Package rendering provides template rendering functionality for model SQL transformations
package models

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
)

// TemplateEngine provides template rendering with Sprig functions
type TemplateEngine struct {
	funcMap       template.FuncMap
	dag           *DependencyGraph
	clickhouseCfg *clickhouse.Config
}

func NewTemplateEngine(clickhouseCfg *clickhouse.Config, dag *DependencyGraph) *TemplateEngine {
	return &TemplateEngine{
		funcMap:       sprig.TxtFuncMap(),
		dag:           dag,
		clickhouseCfg: clickhouseCfg,
	}
}

func (t *TemplateEngine) RenderTransformation(model Transformation, position, interval uint64, startTime time.Time) (string, error) {
	variables, err := t.buildTransformationVariables(model, position, interval, startTime)
	if err != nil {
		return "", fmt.Errorf("failed to build variables: %w", err)
	}

	tmpl, err := template.New("model").Funcs(t.funcMap).Parse(model.GetValue())
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

func (t *TemplateEngine) buildTransformationVariables(model Transformation, position, interval uint64, startTime time.Time) (map[string]interface{}, error) {
	config := model.GetConfig()

	variables := map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"cluster":      t.clickhouseCfg.Cluster,
			"local_suffix": t.clickhouseCfg.LocalSuffix,
		},
		"self": map[string]interface{}{
			"database":  config.Database,
			"table":     config.Table,
			"partition": config.Partition,
			"interval":  config.Interval,
		},
		"task": map[string]interface{}{
			"start": startTime.Unix(),
		},
		"bounds": map[string]interface{}{
			"start": position,
			"end":   position + interval,
		},
	}

	deps := map[string]interface{}{}

	for _, depID := range config.Dependencies {
		dep, err := t.dag.GetNode(depID)
		if err != nil {
			return nil, fmt.Errorf("failed to get dependency: %w", err)
		}

		if dep.NodeType == NodeTypeTransformation {
			transformation, ok := dep.Model.(Transformation)
			if !ok {
				return nil, fmt.Errorf("dependency %s is not a transformation model", depID)
			}

			tConfig := transformation.GetConfig()

			database := map[string]interface{}{}

			if db, ok := deps[tConfig.Database].(map[string]interface{}); ok {
				database = db
			}

			database[tConfig.Table] = map[string]interface{}{
				"database":  tConfig.Database,
				"table":     tConfig.Table,
				"partition": tConfig.Partition,
			}

			deps[tConfig.Database] = database
		}

		if dep.NodeType == NodeTypeExternal {
			external, ok := dep.Model.(External)
			if !ok {
				return nil, fmt.Errorf("dependency %s is not an external model", depID)
			}

			eConfig := external.GetConfig()

			database := map[string]interface{}{}

			if db, ok := deps[eConfig.Database].(map[string]interface{}); ok {
				database = db
			}

			database[eConfig.Table] = map[string]interface{}{
				"database":  eConfig.Database,
				"table":     eConfig.Table,
				"partition": eConfig.Partition,
			}

			deps[eConfig.Database] = database
		}
	}

	variables["dep"] = deps

	return variables, nil
}

func (t *TemplateEngine) GetTransformationEnvironmentVariables(model Transformation, position, interval uint64, startTime time.Time) (*[]string, error) {
	config := model.GetConfig()

	env := []string{
		fmt.Sprintf("CLICKHOUSE_URL=%s", t.clickhouseCfg.URL),
		fmt.Sprintf("SELF_DATABASE=%s", config.Database),
		fmt.Sprintf("SELF_TABLE=%s", config.Table),
		fmt.Sprintf("SELF_PARTITION=%s", config.Partition),
		fmt.Sprintf("TASK_START=%d", startTime.Unix()),
		fmt.Sprintf("TASK_MODEL=%s.%s", config.Database, config.Table),
		fmt.Sprintf("TASK_INTERVAL=%d", interval),
		fmt.Sprintf("BOUNDS_START=%d", position),
		fmt.Sprintf("BOUNDS_END=%d", position+interval),
	}

	if t.clickhouseCfg.Cluster != "" {
		env = append(env,
			fmt.Sprintf("CLICKHOUSE_CLUSTER=%s", t.clickhouseCfg.Cluster),
			fmt.Sprintf("CLICKHOUSE_LOCAL_SUFFIX=%s", t.clickhouseCfg.LocalSuffix))
	}

	for _, depID := range config.Dependencies {
		dep, err := t.dag.GetNode(depID)
		if err != nil {
			return nil, fmt.Errorf("failed to get dependency: %w", err)
		}

		uppercaseName := strings.ToUpper(strings.ReplaceAll(depID, ".", "_"))

		if dep.NodeType == NodeTypeTransformation {
			transformation, ok := dep.Model.(Transformation)
			if !ok {
				return nil, fmt.Errorf("dependency %s is not a transformation model", depID)
			}

			tConfig := transformation.GetConfig()

			env = append(env,
				fmt.Sprintf("DEP_%s_DATABASE=%s", uppercaseName, tConfig.Database),
				fmt.Sprintf("DEP_%s_TABLE=%s", uppercaseName, tConfig.Table),
				fmt.Sprintf("DEP_%s_PARTITION=%s", uppercaseName, tConfig.Partition),
			)
		}

		if dep.NodeType == NodeTypeExternal {
			external, ok := dep.Model.(External)
			if !ok {
				return nil, fmt.Errorf("dependency %s is not an external model", depID)
			}

			eConfig := external.GetConfig()

			env = append(env,
				fmt.Sprintf("DEP_%s_DATABASE=%s", uppercaseName, eConfig.Database),
				fmt.Sprintf("DEP_%s_TABLE=%s", uppercaseName, eConfig.Table),
				fmt.Sprintf("DEP_%s_PARTITION=%s", uppercaseName, eConfig.Partition),
			)
		}
	}

	return &env, nil
}

func (t *TemplateEngine) RenderExternal(model External) (string, error) {
	variables, err := t.buildExternalVariables(model)
	if err != nil {
		return "", fmt.Errorf("failed to build variables: %w", err)
	}

	tmpl, err := template.New("model").Funcs(t.funcMap).Parse(model.GetValue())
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, variables); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

func (t *TemplateEngine) buildExternalVariables(model External) (map[string]interface{}, error) {
	config := model.GetConfig()

	variables := map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"cluster":      t.clickhouseCfg.Cluster,
			"local_suffix": t.clickhouseCfg.LocalSuffix,
		},
		"self": map[string]interface{}{
			"database":  config.Database,
			"table":     config.Table,
			"partition": config.Partition,
		},
	}

	return variables, nil
}
