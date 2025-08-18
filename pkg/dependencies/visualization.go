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
	Dependents   map[string][]string // Precomputed dependents for each model
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

	// Precompute dependents for all models using the efficient GetDependents
	// Note: We need to unlock before calling GetDependents to avoid nested locks
	d.mutex.RUnlock()
	dependents := make(map[string][]string)
	for modelID := range d.modelConfigs {
		dependents[modelID] = d.GetDependents(modelID)
	}
	d.mutex.RLock()

	return &DAGInfo{
		Levels:       levelGroups,
		MaxLevel:     maxLevel,
		RootNodes:    rootNodes,
		TotalModels:  len(d.modelConfigs),
		ModelConfigs: d.modelConfigs,
		Dependents:   dependents,
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
	return d.GenerateDOTFormatWithOptions(DOTOptions{})
}

// DOTOptions configures DOT format generation
type DOTOptions struct {
	IncludeSchedule bool
	IncludeInterval bool
	ColorByLevel    bool
	ShowBackfill    bool
	ShowTags        bool
}

// GenerateDOTFormatWithOptions generates an enhanced DOT format with styling options
func (d *DependencyGraph) GenerateDOTFormatWithOptions(opts DOTOptions) string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Calculate levels for coloring
	levels := d.calculateModelLevels()
	maxLevel := d.getMaxLevel(levels)

	var sb strings.Builder

	// Write graph header
	d.writeDOTHeader(&sb)

	// Add subgraphs for levels if coloring by level
	if opts.ColorByLevel && maxLevel > 0 {
		d.writeLevelSubgraphs(&sb, levels, maxLevel)
	}

	// Write nodes
	d.writeDOTNodes(&sb, levels, maxLevel, opts)

	// Write edges
	d.writeDOTEdges(&sb)

	sb.WriteString("}")
	return sb.String()
}

// getMaxLevel finds the maximum dependency level
func (d *DependencyGraph) getMaxLevel(levels map[string]int) int {
	maxLevel := 0
	for _, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
	}
	return maxLevel
}

// writeDOTHeader writes the graph header with styling
func (d *DependencyGraph) writeDOTHeader(sb *strings.Builder) {
	sb.WriteString("digraph models {\n")
	sb.WriteString("  // Graph properties\n")
	sb.WriteString("  rankdir=LR;\n")
	sb.WriteString("  bgcolor=\"#f7f7f7\";\n")
	sb.WriteString("  fontname=\"Helvetica,Arial,sans-serif\";\n")
	sb.WriteString("  node [fontname=\"Helvetica,Arial,sans-serif\", fontsize=10];\n")
	sb.WriteString("  edge [fontname=\"Helvetica,Arial,sans-serif\", fontsize=9];\n")
	sb.WriteString("  pad=0.5;\n")
	sb.WriteString("  nodesep=0.5;\n")
	sb.WriteString("  ranksep=1.0;\n")
	sb.WriteString("\n")
}

// writeLevelSubgraphs writes subgraphs for each dependency level
func (d *DependencyGraph) writeLevelSubgraphs(sb *strings.Builder, levels map[string]int, maxLevel int) {
	for level := 0; level <= maxLevel; level++ {
		fmt.Fprintf(sb, "  subgraph cluster_%d {\n", level)
		// Label level 0 as "External" since it contains external data sources
		if level == 0 {
			fmt.Fprintf(sb, "    label=\"External\";\n")
		} else {
			fmt.Fprintf(sb, "    label=\"Level %d\";\n", level)
		}
		sb.WriteString("    style=\"rounded,dashed\";\n")
		sb.WriteString("    color=\"#cccccc\";\n")
		sb.WriteString("    fontcolor=\"#666666\";\n")

		for modelID, modelLevel := range levels {
			if modelLevel == level {
				fmt.Fprintf(sb, "    %q;\n", modelID)
			}
		}
		sb.WriteString("  }\n\n")
	}
}

// writeDOTNodes writes node definitions with styling
func (d *DependencyGraph) writeDOTNodes(sb *strings.Builder, levels map[string]int, maxLevel int, opts DOTOptions) {
	sb.WriteString("  // Model nodes\n")
	for modelID := range d.modelConfigs {
		modelConfig := d.modelConfigs[modelID]
		level := levels[modelID]

		// Build label
		label := d.buildNodeLabel(modelID, &modelConfig, opts)

		// Write node with appropriate styling
		if modelConfig.External {
			d.writeExternalNode(sb, modelID, label)
		} else {
			d.writeTransformationNode(sb, modelID, label, level, maxLevel, &modelConfig)
		}
	}
	sb.WriteString("\n")
}

// buildNodeLabel builds the label for a node
func (d *DependencyGraph) buildNodeLabel(modelID string, modelConfig *models.ModelConfig, opts DOTOptions) string {
	if !opts.IncludeSchedule && !opts.IncludeInterval && !opts.ShowBackfill && !opts.ShowTags {
		return fmt.Sprintf("<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\" CELLPADDING=\"8\"><TR><TD ALIGN=\"CENTER\"><B>%s</B></TD></TR></TABLE>", modelID)
	}

	// Use HTML table with proper cellpadding for spacing
	label := "<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\" CELLPADDING=\"4\">"

	// Model name row with proper width to prevent overflow
	label += fmt.Sprintf("<TR><TD COLSPAN=\"2\" ALIGN=\"CENTER\" WIDTH=\"200\"><B>%s</B></TD></TR>", modelID)

	// Add model type indicator (SQL vs Script)
	if !modelConfig.External {
		if modelConfig.Exec != "" {
			label += "<TR><TD COLSPAN=\"2\" ALIGN=\"CENTER\"><FONT POINT-SIZE=\"9\" COLOR=\"#666666\"><I>Script</I></FONT></TD></TR>"
		} else {
			label += "<TR><TD COLSPAN=\"2\" ALIGN=\"CENTER\"><FONT POINT-SIZE=\"9\" COLOR=\"#666666\"><I>SQL</I></FONT></TD></TR>"
		}
	}

	label += d.addTagsLabel(modelConfig, opts)
	label += d.addScheduleLabel(modelConfig, opts)
	label += d.addIntervalLabel(modelConfig, opts)
	label += d.addBackfillLabel(modelConfig, opts)
	label += "</TABLE>"
	return label
}

// addScheduleLabel adds schedule information to the label if applicable
func (d *DependencyGraph) addScheduleLabel(modelConfig *models.ModelConfig, opts DOTOptions) string {
	if opts.IncludeSchedule && modelConfig.Schedule != "" {
		return fmt.Sprintf("<TR><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">schedule:</FONT></TD><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">%s</FONT></TD></TR>", modelConfig.Schedule)
	}
	return ""
}

// addIntervalLabel adds interval information to the label if applicable
func (d *DependencyGraph) addIntervalLabel(modelConfig *models.ModelConfig, opts DOTOptions) string {
	if !opts.IncludeInterval || modelConfig.Interval == 0 {
		return ""
	}

	// Show interval as raw seconds
	return fmt.Sprintf("<TR><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">interval:</FONT></TD><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">%ds</FONT></TD></TR>", modelConfig.Interval)
}

// addBackfillLabel adds backfill information to the label if applicable
func (d *DependencyGraph) addBackfillLabel(modelConfig *models.ModelConfig, opts DOTOptions) string {
	if opts.ShowBackfill && modelConfig.Backfill != nil && modelConfig.Backfill.Enabled {
		schedule := "enabled"
		if modelConfig.Backfill.Schedule != "" {
			schedule = modelConfig.Backfill.Schedule
		}
		return fmt.Sprintf("<TR><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">backfill:</FONT></TD><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">%s</FONT></TD></TR>", schedule)
	}
	return ""
}

// addTagsLabel adds tags to the label if applicable
func (d *DependencyGraph) addTagsLabel(modelConfig *models.ModelConfig, opts DOTOptions) string {
	if !opts.ShowTags || len(modelConfig.Tags) == 0 {
		return ""
	}

	tagStr := ""
	for i, tag := range modelConfig.Tags {
		if i > 0 {
			tagStr += ", "
		}
		tagStr += tag
	}
	return fmt.Sprintf("<TR><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">tags:</FONT></TD><TD ALIGN=\"LEFT\"><FONT POINT-SIZE=\"8\">%s</FONT></TD></TR>", tagStr)
}

// writeExternalNode writes an external model node
func (d *DependencyGraph) writeExternalNode(sb *strings.Builder, modelID, label string) {
	// External models - use dashed box with light blue fill to indicate external data source
	fmt.Fprintf(sb, "  \"%s\" [label=<%s>, shape=box, style=\"filled,rounded,dashed\", ", modelID, label)
	fmt.Fprintf(sb, "fillcolor=\"#e3f2fd\", color=\"#1976d2\", fontcolor=\"#0d47a1\", penwidth=2];\n")
}

// writeTransformationNode writes a transformation model node
func (d *DependencyGraph) writeTransformationNode(sb *strings.Builder, modelID, label string, _, _ int, _ *models.ModelConfig) {
	// Use consistent green fill for all transformation models
	fmt.Fprintf(sb, "  \"%s\" [label=<%s>, shape=box, style=\"filled,rounded\", ", modelID, label)
	fmt.Fprintf(sb, "fillcolor=\"#66bb6a\", color=\"#2e7d32\", fontcolor=\"white\", penwidth=1.5];\n")
}

// writeDOTEdges writes edge definitions with styling
func (d *DependencyGraph) writeDOTEdges(sb *strings.Builder) {
	sb.WriteString("  // Dependencies\n")
	for modelID := range d.modelConfigs {
		modelConfig := d.modelConfigs[modelID]
		for _, dep := range modelConfig.Dependencies {
			// Style edges based on dependency type
			depConfig, exists := d.modelConfigs[dep]
			if exists && depConfig.External {
				// Dependency on external model - dashed line
				fmt.Fprintf(sb, "  \"%s\" -> \"%s\" [style=dashed, color=\"#0288d1\", penwidth=2];\n", dep, modelID)
			} else {
				// Regular dependency - solid line
				fmt.Fprintf(sb, "  \"%s\" -> \"%s\" [color=\"#424242\", penwidth=1.5, arrowsize=0.8];\n", dep, modelID)
			}
		}
	}
}
