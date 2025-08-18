package manager

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// ListModels returns a list of all discovered models with their metadata
func (m *Manager) ListModels(ctx context.Context) ([]ModelInfo, error) {
	// Load all models using the consolidated loader
	modelConfigs, err := models.LoadAllModels(m.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load models: %w", err)
	}

	// Build dependency graph for consistency
	modelConfigList := make([]models.ModelConfig, 0, len(modelConfigs))
	for id := range modelConfigs {
		modelConfigList = append(modelConfigList, modelConfigs[id])
	}

	// Import dependencies package
	depGraph := dependencies.NewDependencyGraph()
	if err := depGraph.BuildGraph(modelConfigList); err != nil {
		return nil, fmt.Errorf("failed to build dependency graph: %w", err)
	}

	modelList := make([]ModelInfo, 0, len(modelConfigs))
	for modelID := range modelConfigs {
		// Get the enriched config from the dependency graph
		modelConfig, _ := depGraph.GetModelConfig(modelID)
		info := m.formatModelInfo(ctx, modelID, &modelConfig)
		modelList = append(modelList, info)
	}

	// Sort by model ID
	sort.Slice(modelList, func(i, j int) bool {
		return modelList[i].ID < modelList[j].ID
	})

	return modelList, nil
}

func (m *Manager) formatModelInfo(ctx context.Context, modelID string, modelConfig *models.ModelConfig) ModelInfo {
	// Get processing status
	lastPos, _ := m.adminManager.GetLastPosition(ctx, modelID)
	status := "ready"
	if lastPos > 0 {
		status = "active"
	}

	// Format dependencies
	deps := "-"
	if len(modelConfig.Dependencies) > 0 {
		deps = strings.Join(modelConfig.Dependencies, ", ")
	}

	// Format schedule
	schedule := "-"
	if modelConfig.Schedule != "" {
		schedule = modelConfig.Schedule
	}

	// Format interval
	interval := "-"
	if modelConfig.Interval > 0 {
		interval = fmt.Sprintf("%d", modelConfig.Interval)
	}

	// Format lag (external models only)
	lag := "-"
	if modelConfig.External && modelConfig.Lag > 0 {
		lag = fmt.Sprintf("%ds", modelConfig.Lag)
	}

	modelType := ModelTypeTransformation
	if modelConfig.External {
		modelType = ModelTypeExternal
	}

	return ModelInfo{
		ID:       modelID,
		Type:     modelType,
		Schedule: schedule,
		Interval: interval,
		Lag:      lag,
		Deps:     deps,
		Status:   status,
	}
}

// LoadModels loads and returns all model configurations
func LoadModels(log *logrus.Entry) (map[string]models.ModelConfig, error) {
	// Use the consolidated loader
	return models.LoadAllModels(log)
}
