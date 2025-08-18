package dependencies

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ethpandaops/cbt/pkg/models"
)

// DAGInfo contains DAG visualization information
type DAGInfo struct {
	Levels       map[int][]string
	MaxLevel     int
	RootNodes    []string
	TotalModels  int
	ModelConfigs map[string]models.ModelConfig
}

// GetDAGInfo returns DAG visualization information
func (d *DependencyGraph) GetDAGInfo() *DAGInfo {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Calculate levels
	levels := d.calculateModelLevels()

	// Group models by level
	levelGroups := make(map[int][]string)
	maxLevel := 0
	for modelID, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
		levelGroups[level] = append(levelGroups[level], modelID)
	}

	// Sort models within each level
	for level := range levelGroups {
		sort.Strings(levelGroups[level])
	}

	// Find root nodes
	rootNodes := d.findRootNodes()

	return &DAGInfo{
		Levels:       levelGroups,
		MaxLevel:     maxLevel,
		RootNodes:    rootNodes,
		TotalModels:  len(d.modelConfigs),
		ModelConfigs: d.modelConfigs,
	}
}

// calculateModelLevels calculates the dependency depth level for each model
func (d *DependencyGraph) calculateModelLevels() map[string]int {
	levels := make(map[string]int)

	// Initialize all models to level 0
	for modelID := range d.modelConfigs {
		levels[modelID] = 0
	}

	// Keep updating levels until stable
	changed := true
	for changed {
		changed = false
		for modelID := range d.modelConfigs {
			modelConfig := d.modelConfigs[modelID]
			currentLevel := levels[modelID]

			// Calculate max level of dependencies
			maxDepLevel := -1
			for _, dep := range modelConfig.Dependencies {
				if depLevel, exists := levels[dep]; exists && depLevel > maxDepLevel {
					maxDepLevel = depLevel
				}
			}

			// Update level if needed
			if maxDepLevel >= 0 && maxDepLevel+1 > currentLevel {
				levels[modelID] = maxDepLevel + 1
				changed = true
			}
		}
	}

	return levels
}

// findRootNodes finds all models with no dependencies
func (d *DependencyGraph) findRootNodes() []string {
	roots := []string{}
	for modelID := range d.modelConfigs {
		modelConfig := d.modelConfigs[modelID]
		if len(modelConfig.Dependencies) == 0 {
			roots = append(roots, modelID)
		}
	}
	sort.Strings(roots)
	return roots
}

// FindDependents finds all models that directly depend on the given model
func (d *DependencyGraph) FindDependents(modelID string) []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	var dependents []string
	for id := range d.modelConfigs {
		modelCfg := d.modelConfigs[id]
		for _, dep := range modelCfg.Dependencies {
			if dep == modelID {
				dependents = append(dependents, id)
				break
			}
		}
	}
	sort.Strings(dependents)
	return dependents
}

// CalculateDepth calculates the maximum depth from a model to its furthest dependent
func (d *DependencyGraph) CalculateDepth(modelID string, currentDepth int, memo map[string]int) int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if depth, exists := memo[modelID]; exists {
		return depth
	}

	maxDepth := currentDepth

	// Find models that depend on this one
	for id := range d.modelConfigs {
		modelCfg := d.modelConfigs[id]
		for _, dep := range modelCfg.Dependencies {
			if dep == modelID {
				depth := d.CalculateDepth(id, currentDepth+1, memo)
				if depth > maxDepth {
					maxDepth = depth
				}
				break
			}
		}
	}

	memo[modelID] = maxDepth
	return maxDepth
}

// GenerateDOTFormat generates a DOT format representation of the DAG
func (d *DependencyGraph) GenerateDOTFormat() string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	var sb strings.Builder
	sb.WriteString("digraph models {\n")
	sb.WriteString("  rankdir=LR;\n")

	for modelID := range d.modelConfigs {
		modelConfig := d.modelConfigs[modelID]
		if modelConfig.External {
			fmt.Fprintf(&sb, "  \"%s\" [shape=box, style=filled, fillcolor=lightblue];\n", modelID)
		} else {
			fmt.Fprintf(&sb, "  \"%s\";\n", modelID)
		}
		for _, dep := range modelConfig.Dependencies {
			fmt.Fprintf(&sb, "  \"%s\" -> \"%s\";\n", dep, modelID)
		}
	}

	sb.WriteString("}")
	return sb.String()
}
