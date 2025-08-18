// Package dependencies manages the dependency graph for CBT models
package dependencies

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/heimdalr/dag"
)

var (
	// ErrNonExistentDependency is returned when a model depends on a non-existent model
	ErrNonExistentDependency = errors.New("model depends on non-existent model")
	// ErrInconsistentGraph is returned when the dependency graph is inconsistent
	ErrInconsistentGraph = errors.New("dependency graph is inconsistent: vertex count mismatch")
)

// DependencyGraph manages the dependency graph for models
type DependencyGraph struct {
	dag          *dag.DAG
	modelConfigs map[string]models.ModelConfig
	mutex        sync.RWMutex
}

// NewDependencyGraph creates a new dependency graph
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		dag:          dag.NewDAG(),
		modelConfigs: make(map[string]models.ModelConfig),
	}
}

// BuildGraph builds the dependency graph from model configurations
func (d *DependencyGraph) BuildGraph(modelList []models.ModelConfig) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Reset DAG
	d.dag = dag.NewDAG()
	d.modelConfigs = make(map[string]models.ModelConfig)

	// Add all models as vertices (use index to avoid copying)
	for i := range modelList {
		model := &modelList[i]
		modelID := fmt.Sprintf("%s.%s", model.Database, model.Table)
		d.modelConfigs[modelID] = *model

		// Store just the modelID as vertex data (ModelConfig isn't hashable due to slices)
		if err := d.dag.AddVertexByID(modelID, modelID); err != nil {
			return fmt.Errorf("failed to add vertex %s: %w", modelID, err)
		}
	}

	// Add edges (dependency → dependent)
	for i := range modelList {
		model := &modelList[i]
		modelID := fmt.Sprintf("%s.%s", model.Database, model.Table)

		for _, depID := range model.Dependencies {
			// Validate dependency exists
			if _, exists := d.modelConfigs[depID]; !exists {
				return fmt.Errorf("%w: %s depends on %s", ErrNonExistentDependency, modelID, depID)
			}

			// AddEdge returns error if it would create a cycle
			if err := d.dag.AddEdge(depID, modelID); err != nil {
				return fmt.Errorf("invalid dependency %s → %s: %w", depID, modelID, err)
			}
		}
	}

	return nil
}

// GetDependents returns the direct dependents of a model
func (d *DependencyGraph) GetDependents(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	children, err := d.dag.GetChildren(modelID)
	if err != nil {
		return nil
	}

	dependents := make([]string, 0, len(children))
	for id := range children {
		dependents = append(dependents, id)
	}

	return dependents
}

// GetDependencies returns the direct dependencies of a model
func (d *DependencyGraph) GetDependencies(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	parents, err := d.dag.GetParents(modelID)
	if err != nil {
		return nil
	}

	dependencies := make([]string, 0, len(parents))
	for id := range parents {
		dependencies = append(dependencies, id)
	}

	return dependencies
}

// GetAllDependents returns all dependents (recursive) of a model
func (d *DependencyGraph) GetAllDependents(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	descendants, err := d.dag.GetDescendants(modelID)
	if err != nil {
		return nil
	}

	allDependents := make([]string, 0, len(descendants))
	for id := range descendants {
		allDependents = append(allDependents, id)
	}

	return allDependents
}

// GetAllDependencies returns all dependencies (recursive) of a model
func (d *DependencyGraph) GetAllDependencies(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	ancestors, err := d.dag.GetAncestors(modelID)
	if err != nil {
		return nil
	}

	allDependencies := make([]string, 0, len(ancestors))
	for id := range ancestors {
		allDependencies = append(allDependencies, id)
	}

	return allDependencies
}

// IsPathBetween checks if there's a path from one model to another
func (d *DependencyGraph) IsPathBetween(fromModelID, toModelID string) bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Check if toModelID is in the descendants of fromModelID
	descendants, err := d.dag.GetDescendants(fromModelID)
	if err != nil {
		return false
	}

	_, exists := descendants[toModelID]
	return exists
}

// GetModelConfig returns the configuration for a model
func (d *DependencyGraph) GetModelConfig(modelID string) (models.ModelConfig, bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	config, exists := d.modelConfigs[modelID]
	return config, exists
}

// ValidateNoCycles validates that the dependency graph contains no cycles
func (d *DependencyGraph) ValidateNoCycles() error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// heimdalr/dag automatically prevents cycles during AddEdge
	// If the DAG has been successfully built, there are no cycles
	// This is a no-op but kept for API consistency
	return nil
}

// GetAllModelIDs returns all model IDs in the dependency graph
func (d *DependencyGraph) GetAllModelIDs() []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	ids := make([]string, 0, len(d.modelConfigs))
	for id := range d.modelConfigs {
		ids = append(ids, id)
	}

	sort.Strings(ids) // Ensure consistent ordering
	return ids
}

// GetTransformationModels returns all transformation (non-external) model IDs
func (d *DependencyGraph) GetTransformationModels() []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	modelList := make([]string, 0)
	for id := range d.modelConfigs {
		if !d.modelConfigs[id].External {
			modelList = append(modelList, id)
		}
	}

	sort.Strings(modelList) // Ensure consistent ordering
	return modelList
}

// GetExternalModels returns all external model IDs
func (d *DependencyGraph) GetExternalModels() []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	modelList := make([]string, 0)
	for id := range d.modelConfigs {
		if d.modelConfigs[id].External {
			modelList = append(modelList, id)
		}
	}

	sort.Strings(modelList) // Ensure consistent ordering
	return modelList
}
