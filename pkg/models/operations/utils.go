package operations

import (
	"github.com/ethpandaops/cbt/pkg/models"
)

// FindDependentModels finds all models that depend on the given model
func FindDependentModels(modelID string, modelConfigs map[string]models.ModelConfig) []string {
	var dependents []string

	for id := range modelConfigs {
		modelCfg := modelConfigs[id]
		if id == modelID {
			continue
		}

		for _, dep := range modelCfg.Dependencies {
			if dep == modelID {
				dependents = append(dependents, id)
				// Recursively find dependents of this model
				subDependents := FindDependentModels(id, modelConfigs)
				dependents = append(dependents, subDependents...)
				break
			}
		}
	}

	// Deduplicate
	seen := make(map[string]bool)
	var unique []string
	for _, id := range dependents {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}

	return unique
}
