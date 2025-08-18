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
)

// rerunCmd represents the rerun command
//
//nolint:gochecknoglobals // Cobra commands are typically global
var rerunCmd = &cobra.Command{
	Use:   "rerun",
	Short: "Invalidate completion records for a model and all dependent models",
	Long: `Rerun invalidates completion records for a specific model and all dependent
models for the specified time range. This creates gaps that will be detected
and filled by the backfill processes on their scheduled intervals.

The rerun command will:
  - Clear completion records for the specified time range
  - Automatically include all dependent models to ensure consistency
  - Let the existing backfill processes detect and fill the gaps

Note: The actual reprocessing happens when the backfill schedule runs for each
model. Check your model's backfill schedule to know when processing will occur.

Examples:
  # Invalidate a model and all dependents for a time range
  cbt rerun --model analytics.block_propagation --start 1704067200 --end 1704070800

  # Invalidate an upstream model to cascade changes through the pipeline
  cbt rerun --model ethereum.beacon_blocks --start 1704067200 --end 1704070800`,
	RunE: runRerun,
}

func init() {
	rootCmd.AddCommand(rerunCmd)

	rerunCmd.Flags().StringVar(&rerunModelID, "model", "", "Model ID to rerun (e.g., analytics.block_propagation)")
	rerunCmd.Flags().Uint64Var(&rerunStart, "start", 0, "Start position (Unix timestamp)")
	rerunCmd.Flags().Uint64Var(&rerunEnd, "end", 0, "End position (Unix timestamp)")

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
	manager, err := operations.NewManager(&cfg.ClickHouse, &cfg.Models, logger)
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
	}

	return manager.Execute(ctx, opts)
}
