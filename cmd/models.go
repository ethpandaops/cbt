package cmd

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/manager"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// modelsCmd represents the models command group
//
//nolint:gochecknoglobals // Cobra commands are typically global
var modelsCmd = &cobra.Command{
	Use:   "models",
	Short: "Manage and inspect model configurations",
	Long:  `Commands for listing, validating, and visualizing model configurations.`,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		// Set default log level to error for models commands to suppress ClickHouse info logs
		// unless explicitly set via --log-level flag
		if !cmd.Flags().Changed("log-level") {
			logger.SetLevel(logrus.ErrorLevel)
		}
		return nil
	},
}

// listCmd lists all discovered models
//
//nolint:gochecknoglobals // Cobra commands are typically global
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all discovered models",
	Long:  `List all discovered models with their metadata including type, schedule, interval, and dependencies.`,
	RunE:  runModelsList,
}

// validateCmd validates model configurations
//
//nolint:gochecknoglobals // Cobra commands are typically global
var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate model configurations",
	Long:  `Validate model configurations including syntax, required fields, dependencies, and schema compatibility.`,
	RunE:  runModelsValidate,
}

// dagCmd visualizes the dependency DAG
//
//nolint:gochecknoglobals // Cobra commands are typically global
var dagCmd = &cobra.Command{
	Use:   "dag",
	Short: "Visualize model dependency DAG",
	Long:  `Visualize the dependency directed acyclic graph (DAG) showing model relationships and data flow.`,
	RunE:  runModelsDAG,
}

// statusCmd shows detailed model status including last run times
//
//nolint:gochecknoglobals // Cobra commands are typically global
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show detailed model status including last run times and backfill information",
	Long:  `Show detailed status for all models including when they last ran, current position, processing state, and backfill/gap information.`,
	RunE:  runModelsStatus,
}

func init() {
	rootCmd.AddCommand(modelsCmd)
	modelsCmd.AddCommand(listCmd)
	modelsCmd.AddCommand(validateCmd)
	modelsCmd.AddCommand(dagCmd)
	modelsCmd.AddCommand(statusCmd)

	// Add dot flag to dag command
	dagCmd.Flags().Bool("dot", false, "Output in DOT format for graphviz")
}

func runModelsList(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	cfg, err := LoadCLIConfig(cfgFile)
	if err != nil {
		return err
	}
	if validationErr := cfg.Validate(); validationErr != nil {
		return validationErr
	}

	// Create manager
	mgr, err := manager.NewManager(&cfg.ClickHouse, logger)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := mgr.Close(); closeErr != nil {
			logger.WithError(closeErr).Error("Failed to close manager")
		}
	}()

	ctx := context.Background()

	// Get model list
	modelList, err := mgr.ListModels(ctx)
	if err != nil {
		return err
	}

	// Print table
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "MODEL ID\tTYPE\tSCHEDULE\tINTERVAL\tLAG\tDEPS\tSTATUS")
	for _, m := range modelList {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			m.ID, m.Type, m.Schedule, m.Interval, m.Lag, m.Deps, m.Status)
	}
	_ = w.Flush()

	return nil
}

func runModelsValidate(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	cfg, err := LoadCLIConfig(cfgFile)
	if err != nil {
		return err
	}
	if validationErr := cfg.Validate(); validationErr != nil {
		return validationErr
	}

	// Create manager
	mgr, err := manager.NewManager(&cfg.ClickHouse, logger)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := mgr.Close(); closeErr != nil {
			logger.WithError(closeErr).Error("Failed to close manager")
		}
	}()

	ctx := context.Background()

	// Validate models
	result, err := mgr.ValidateModels(ctx)
	if err != nil {
		return err
	}

	// Print validation results
	for modelID := range result.ModelConfigs {
		fmt.Printf("✓ %s: valid\n", modelID)
	}

	// Summary
	fmt.Printf("\n%d valid, %d errors\n", result.ValidCount, result.ErrorCount)

	if result.ErrorCount > 0 {
		return fmt.Errorf("%w: %d errors", models.ErrValidationFailed, result.ErrorCount)
	}

	return nil
}

func runModelsDAG(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	cfg, err := LoadCLIConfig(cfgFile)
	if err != nil {
		return err
	}
	if validationErr := cfg.Validate(); validationErr != nil {
		return validationErr
	}

	// Create manager
	mgr, err := manager.NewManager(&cfg.ClickHouse, logger)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := mgr.Close(); closeErr != nil {
			logger.WithError(closeErr).Error("Failed to close manager")
		}
	}()

	// Get DAG info
	dagInfo, err := mgr.GetDAGInfo()
	if err != nil {
		return err
	}

	// Generate DOT format if requested
	if dotFlag, _ := cmd.Flags().GetBool("dot"); dotFlag {
		fmt.Println("DOT Format:")
		fmt.Println("===========")
		// Create dependency graph for DOT generation
		configList := make([]models.ModelConfig, 0, len(dagInfo.ModelConfigs))
		for id := range dagInfo.ModelConfigs {
			configList = append(configList, dagInfo.ModelConfigs[id])
		}
		depGraph := dependencies.NewDependencyGraph()
		_ = depGraph.BuildGraph(configList) // Error already handled in GetDAGInfo
		fmt.Println(depGraph.GenerateDOTFormat())
		return nil
	}

	// Print dependency graph
	fmt.Println("Dependency Graph:")
	fmt.Println("=================")
	printDAGLevels(dagInfo)

	// Print statistics
	fmt.Println("\nStatistics:")
	fmt.Println("===========")
	fmt.Printf("Independent chains: %d\n", len(dagInfo.RootNodes))
	fmt.Printf("Total models: %d\n", dagInfo.TotalModels)
	fmt.Printf("Max depth: %d\n", dagInfo.MaxLevel)

	return nil
}

func runModelsStatus(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	cfg, err := LoadCLIConfig(cfgFile)
	if err != nil {
		return err
	}
	if validationErr := cfg.Validate(); validationErr != nil {
		return validationErr
	}

	// Create manager
	mgr, err := manager.NewManager(&cfg.ClickHouse, logger)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := mgr.Close(); closeErr != nil {
			logger.WithError(closeErr).Error("Failed to close manager")
		}
	}()

	ctx := context.Background()

	// Get model statuses
	statusMap, allModels, err := mgr.GetModelStatuses(ctx)
	if err != nil {
		return err
	}

	// Get gap information
	gapMap := mgr.GetGapInformation(ctx, allModels)

	// Print detailed status table
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "MODEL ID\tTYPE\tLAST RUN\tNEXT RUN\tFIRST POS\tLAST POS\tNEXT POS\tTOTAL RUNS\tCOVERAGE\tGAPS")

	// Sort model IDs for consistent output
	modelIDs := make([]string, 0, len(allModels))
	for modelID := range allModels {
		modelIDs = append(modelIDs, modelID)
	}
	sort.Strings(modelIDs)

	for _, modelID := range modelIDs {
		modelConfig := allModels[modelID]
		formatModelStatusWithGaps(w, modelID, &modelConfig, statusMap, gapMap)
	}

	_ = w.Flush()

	return nil
}

// Helper functions

func printDAGLevels(dagInfo *dependencies.DAGInfo) {
	for level := 0; level <= dagInfo.MaxLevel; level++ {
		if modelList, exists := dagInfo.Levels[level]; exists {
			fmt.Printf("\nLevel %d:\n", level)
			for _, modelID := range modelList {
				printModelInfo(modelID, dagInfo)
			}
		}
	}
}

func printModelInfo(modelID string, dagInfo *dependencies.DAGInfo) {
	modelConfig := dagInfo.ModelConfigs[modelID]
	modelType := manager.ModelTypeTransformation
	if modelConfig.External {
		modelType = manager.ModelTypeExternal
	}

	fmt.Printf("  • %s (%s)", modelID, modelType)

	// Show lag for external models
	if modelConfig.External && modelConfig.Lag > 0 {
		fmt.Printf(" [lag: %ds]", modelConfig.Lag)
	}

	printDependencyInfo(&modelConfig)
	printDependentsInfo(modelID, dagInfo)
	fmt.Println()
}

func printDependencyInfo(modelConfig *models.ModelConfig) {
	if len(modelConfig.Dependencies) > 0 {
		fmt.Printf("\n    ← depends on: %s", strings.Join(modelConfig.Dependencies, ", "))
	}
}

func printDependentsInfo(modelID string, dagInfo *dependencies.DAGInfo) {
	// Use precomputed dependents from DAGInfo
	dependents := dagInfo.Dependents[modelID]
	if len(dependents) > 0 {
		fmt.Printf("\n    → used by: %s", strings.Join(dependents, ", "))
	}
}

func formatModelStatusWithGaps(w io.Writer, modelID string, modelConfig *models.ModelConfig, statusMap map[string]manager.ModelStatus, gapMap map[string]manager.GapInfo) {
	modelType := manager.ModelTypeTransformation
	if modelConfig.External {
		modelType = manager.ModelTypeExternal
	}

	// Check if model has run
	status, exists := statusMap[modelID]
	if !exists {
		// Model has never run
		nextRun := "-"
		if modelConfig.Schedule != "" && !modelConfig.External {
			nextRun = "ready"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			modelID, modelType, "never", nextRun, "-", "-", "-", 0, "-", "-")
		return
	}

	lastRun, nextRun := manager.FormatRunTimes(status.LastRun, modelConfig)
	firstPosStr, lastPosStr, nextPosStr := manager.FormatPositions(status.FirstPosition, status.LastPosition, status.NextPosition, modelConfig)
	coverage, gaps := formatGapInfo(modelID, modelConfig, gapMap)

	_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
		modelID, modelType, lastRun, nextRun, firstPosStr, lastPosStr, nextPosStr, status.TotalRuns, coverage, gaps)
}

func formatGapInfo(modelID string, modelConfig *models.ModelConfig, gapMap map[string]manager.GapInfo) (coverage, gaps string) {
	coverage = "-"
	gaps = "-"

	gap, hasGap := gapMap[modelID]
	if !hasGap {
		return coverage, gaps
	}

	if gap.TotalExpected > 0 {
		coverage = fmt.Sprintf("%.1f%%", gap.Coverage*100)
	}

	if gap.GapCount > 0 {
		gaps = formatGapCount(gap, modelConfig)
	} else {
		gaps = "0"
	}

	return coverage, gaps
}

func formatGapCount(gap manager.GapInfo, modelConfig *models.ModelConfig) string {
	gaps := fmt.Sprintf("%d", gap.GapCount)

	if gap.OldestGap == 0 || modelConfig.Partition == "" || gap.OldestGap <= 1000000000 {
		return gaps
	}

	// Show oldest gap as time for timestamp-based models
	if gap.OldestGap > math.MaxInt64 {
		return gaps
	}

	oldestTime := time.Unix(int64(gap.OldestGap), 0)
	ago := time.Since(oldestTime)

	switch {
	case ago < time.Hour:
		return fmt.Sprintf("%d (oldest: %dm ago)", gap.GapCount, int(ago.Minutes()))
	case ago < 24*time.Hour:
		return fmt.Sprintf("%d (oldest: %dh ago)", gap.GapCount, int(ago.Hours()))
	default:
		return fmt.Sprintf("%d (oldest: %dd ago)", gap.GapCount, int(ago.Hours()/24))
	}
}
