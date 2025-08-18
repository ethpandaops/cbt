package manager

import (
	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
)

// GetDAGInfo returns DAG visualization information
func (m *Manager) GetDAGInfo() (*dependencies.DAGInfo, error) {
	// Load all models
	modelConfigs, err := LoadModels(m.logger)
	if err != nil {
		return nil, err
	}

	// Convert map to slice for BuildGraph
	configList := make([]models.ModelConfig, 0, len(modelConfigs))
	for id := range modelConfigs {
		configList = append(configList, modelConfigs[id])
	}

	// Create dependency graph
	depGraph := dependencies.NewDependencyGraph()
	if err := depGraph.BuildGraph(configList); err != nil {
		return nil, err
	}

	// Get DAG visualization info
	return depGraph.GetDAGInfo(), nil
}
