package cmd

import (
	"context"

	"github.com/ethpandaops/cbt/pkg/models/operations"
	"github.com/spf13/cobra"
)

//nolint:gochecknoglobals // Command flags need to be global for cobra
var (
	rerunModelID string
	rerunStart   uint64
	rerunEnd     uint64
	rerunCascade bool
	rerunForce   bool
)

// rerunCmd represents the rerun command
//
//nolint:gochecknoglobals // Cobra commands are typically global
var rerunCmd = &cobra.Command{
	Use:   "rerun",
	Short: "Rerun tasks for a specific model and time range",
	Long: `Rerun allows you to manually re-execute tasks for a specific model
and time range. This is useful for reprocessing data after fixing issues
or when upstream data has been corrected.

Examples:
  # Rerun a specific model for a time range
  cbt rerun --model analytics.block_propagation --start 1704067200 --end 1704070800

  # Rerun with cascade to also rerun dependent models
  cbt rerun --model ethereum.beacon_blocks --start 1704067200 --end 1704070800 --cascade

  # Force rerun even if already completed
  cbt rerun --model analytics.block_propagation --start 1704067200 --end 1704070800 --force`,
	RunE: runRerun,
}

func init() {
	rootCmd.AddCommand(rerunCmd)

	rerunCmd.Flags().StringVar(&rerunModelID, "model", "", "Model ID to rerun (e.g., analytics.block_propagation)")
	rerunCmd.Flags().Uint64Var(&rerunStart, "start", 0, "Start position (Unix timestamp)")
	rerunCmd.Flags().Uint64Var(&rerunEnd, "end", 0, "End position (Unix timestamp)")
	rerunCmd.Flags().BoolVar(&rerunCascade, "cascade", false, "Also rerun dependent models")
	rerunCmd.Flags().BoolVar(&rerunForce, "force", false, "Force rerun even if already completed")

	_ = rerunCmd.MarkFlagRequired("model")
	_ = rerunCmd.MarkFlagRequired("start")
	_ = rerunCmd.MarkFlagRequired("end")
}

func runRerun(cmd *cobra.Command, _ []string) error {
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

	// Create rerun manager
	manager, err := operations.NewManager(&cfg.ClickHouse, cfg.Redis.URL, logger)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := manager.Close(); closeErr != nil {
			logger.WithError(closeErr).Error("Failed to close rerun manager")
		}
	}()

	// Execute rerun
	ctx := context.Background()
	opts := &operations.Options{
		ModelID: rerunModelID,
		Start:   rerunStart,
		End:     rerunEnd,
		Cascade: rerunCascade,
		Force:   rerunForce,
	}

	return manager.Execute(ctx, opts)
}
