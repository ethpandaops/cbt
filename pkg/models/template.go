// Package models provides template rendering functionality for model SQL transformations
package models

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// TemplateEngine provides template rendering with Sprig functions
type TemplateEngine struct {
	funcMap       template.FuncMap
	dag           *DependencyGraph
	clickhouseCfg *clickhouse.Config
}

// NewTemplateEngine creates a new template engine for rendering models
func NewTemplateEngine(clickhouseCfg *clickhouse.Config, dag *DependencyGraph) *TemplateEngine {
	return &TemplateEngine{
		funcMap:       sprig.TxtFuncMap(),
		dag:           dag,
		clickhouseCfg: clickhouseCfg,
	}
}

// RenderTransformation renders a transformation model template with variables
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

	variables := t.buildBaseVariables(config, position, interval, startTime)

	deps, err := t.buildDependencyVariables(model)
	if err != nil {
		return nil, err
	}

	variables["dep"] = deps
	return variables, nil
}

// buildBaseVariables creates the base template variables
func (t *TemplateEngine) buildBaseVariables(config *transformation.Config, position, interval uint64, startTime time.Time) map[string]interface{} {
	return map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"cluster":      t.clickhouseCfg.Cluster,
			"local_suffix": t.clickhouseCfg.LocalSuffix,
		},
		"self": map[string]interface{}{
			"database": config.Database,
			"table":    config.Table,
			"interval": interval,
		},
		"task": map[string]interface{}{
			"start": startTime.Unix(),
		},
		"bounds": map[string]interface{}{
			"start": position,
			"end":   position + interval,
		},
	}
}

// buildDependencyVariables processes all dependencies and builds the dep variables
func (t *TemplateEngine) buildDependencyVariables(model Transformation) (map[string]interface{}, error) {
	deps := map[string]interface{}{}

	// Get dependencies from handler if it supports them
	handler := model.GetHandler()
	if handler == nil {
		return deps, nil
	}

	depProvider, ok := handler.(interface{ GetFlattenedDependencies() []string })
	if !ok {
		return deps, nil
	}

	// Helper function to add a dep entry
	addDepEntry := func(database, table string, data map[string]interface{}) {
		db := map[string]interface{}{}
		if existing, ok := deps[database].(map[string]interface{}); ok {
			db = existing
		}
		db[table] = data
		deps[database] = db
	}

	allDeps := depProvider.GetFlattenedDependencies()
	for _, depID := range allDeps {
		if err := t.processSingleDependencyID(depID, depID, addDepEntry); err != nil {
			return nil, err
		}
	}

	return deps, nil
}

// Deprecated: These functions are no longer needed with the new handler architecture
// Dependencies are now managed directly by type-specific handlers

// processSingleDependencyID processes a single dependency ID
func (t *TemplateEngine) processSingleDependencyID(depID, originalDep string, addDepEntry func(string, string, map[string]interface{})) error {
	dep, err := t.dag.GetNode(depID)
	if err != nil {
		return fmt.Errorf("failed to get dependency: %w", err)
	}

	switch dep.NodeType {
	case NodeTypeTransformation:
		return t.processTransformationDependency(dep, depID, originalDep, addDepEntry)
	case NodeTypeExternal:
		return t.processExternalDependency(dep, depID, originalDep, addDepEntry)
	default:
		return nil
	}
}

// processTransformationDependency processes a transformation dependency
func (t *TemplateEngine) processTransformationDependency(dep Node, depID, originalDep string, addDepEntry func(string, string, map[string]interface{})) error {
	transformModel, ok := dep.Model.(Transformation)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotTransformationModel, depID)
	}

	tConfig := transformModel.GetConfig()
	depData := map[string]interface{}{
		"database": tConfig.Database,
		"table":    tConfig.Table,
	}

	// Add entry with resolved database
	addDepEntry(tConfig.Database, tConfig.Table, depData)

	// If original had a placeholder, also add entry with placeholder key
	if originalDep != depID && strings.Contains(originalDep, ".") {
		placeholderDB := strings.SplitN(originalDep, ".", 2)[0]
		addDepEntry(placeholderDB, tConfig.Table, depData)
	}

	return nil
}

// processExternalDependency processes an external dependency
func (t *TemplateEngine) processExternalDependency(dep Node, depID, originalDep string, addDepEntry func(string, string, map[string]interface{})) error {
	external, ok := dep.Model.(External)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotExternalModel, depID)
	}

	eConfig := external.GetConfig()
	depData := map[string]interface{}{
		"database": eConfig.Database,
		"table":    eConfig.Table,
	}

	// Add entry with resolved database
	addDepEntry(eConfig.Database, eConfig.Table, depData)

	// If original had a placeholder, also add entry with placeholder key
	if originalDep != depID && strings.Contains(originalDep, ".") {
		placeholderDB := strings.SplitN(originalDep, ".", 2)[0]
		addDepEntry(placeholderDB, eConfig.Table, depData)
	}

	return nil
}

// GetTransformationEnvironmentVariables builds environment variables for transformation execution
func (t *TemplateEngine) GetTransformationEnvironmentVariables(model Transformation, position, interval uint64, startTime time.Time) (*[]string, error) {
	config := model.GetConfig()

	env := []string{
		fmt.Sprintf("CLICKHOUSE_URL=%s", t.clickhouseCfg.URL),
		fmt.Sprintf("SELF_DATABASE=%s", config.Database),
		fmt.Sprintf("SELF_TABLE=%s", config.Table),
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

	// Process all dependencies from handler
	var allDeps []string
	handler := model.GetHandler()
	if handler != nil {
		if depProvider, ok := handler.(interface{ GetFlattenedDependencies() []string }); ok {
			allDeps = depProvider.GetFlattenedDependencies()
		}
	}
	for _, depID := range allDeps {
		dep, err := t.dag.GetNode(depID)
		if err != nil {
			return nil, fmt.Errorf("failed to get dependency: %w", err)
		}

		uppercaseName := strings.ToUpper(strings.ReplaceAll(depID, ".", "_"))

		if dep.NodeType == NodeTypeTransformation {
			transformModel, ok := dep.Model.(Transformation)
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrNotTransformationModel, depID)
			}

			tConfig := transformModel.GetConfig()

			env = append(env,
				fmt.Sprintf("DEP_%s_DATABASE=%s", uppercaseName, tConfig.Database),
				fmt.Sprintf("DEP_%s_TABLE=%s", uppercaseName, tConfig.Table),
			)
		}

		if dep.NodeType == NodeTypeExternal {
			external, ok := dep.Model.(External)
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrNotExternalModel, depID)
			}

			eConfig := external.GetConfig()

			env = append(env,
				fmt.Sprintf("DEP_%s_DATABASE=%s", uppercaseName, eConfig.Database),
				fmt.Sprintf("DEP_%s_TABLE=%s", uppercaseName, eConfig.Table),
			)
		}
	}

	return &env, nil
}

// RenderExternal renders an external model template with variables
func (t *TemplateEngine) RenderExternal(model External, cacheState map[string]interface{}) (string, error) {
	variables := t.buildExternalVariables(model, cacheState)

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

func (t *TemplateEngine) buildExternalVariables(model External, cacheState map[string]interface{}) map[string]interface{} {
	config := model.GetConfig()

	variables := map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"cluster":      t.clickhouseCfg.Cluster,
			"local_suffix": t.clickhouseCfg.LocalSuffix,
		},
		"self": map[string]interface{}{
			"database": config.Database,
			"table":    config.Table,
		},
	}

	// Add cache state if provided
	if cacheState != nil {
		variables["cache"] = cacheState
	}

	return variables
}
