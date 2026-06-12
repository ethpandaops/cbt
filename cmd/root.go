// Package cmd contains the CLI commands for CBT
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/creasty/defaults"
	"github.com/ethpandaops/cbt/pkg/engine"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

//nolint:gochecknoglobals // Global vars needed for cobra CLI
var (
	cfgFile string
)

// engineRunner is the minimal lifecycle surface of the engine that runEngine
// depends on. It exists so the engine can be substituted in tests.
type engineRunner interface {
	Start(ctx context.Context) error
	Stop() error
}

// Seams overridable in tests.
//
//nolint:gochecknoglobals // Test seams for otherwise-untestable code paths.
var (
	osExit = os.Exit

	setDefaults = defaults.Set

	newEngine = func(log *logrus.Logger, cfg *engine.Config) (engineRunner, error) {
		return engine.NewService(log, cfg)
	}
)

// rootCmd represents the base command
//
//nolint:gochecknoglobals // Cobra commands are typically global
var rootCmd = &cobra.Command{
	Use:   "cbt",
	Short: "ClickHouse Build Tool - Manage data transformations in ClickHouse",
	Long: `CBT (ClickHouse Build Tool) is a simplified, ClickHouse-focused data
transformation tool that provides idempotent transformations, DAG-based
dependency management, and interval-based processing.`,
	RunE: runEngine,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		osExit(1)
	}
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
}

func loadConfigFromFile(file string) (*engine.Config, error) {
	if file == "" {
		file = "config.yaml"
	}

	config := &engine.Config{}

	if err := setDefaults(config); err != nil {
		return nil, err
	}

	yamlFile, err := os.ReadFile(file) //nolint:gosec // User-provided config file path
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(yamlFile, config); err != nil {
		return nil, err
	}

	return config, nil
}

func runEngine(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	config, err := loadConfigFromFile(cfgFile)
	if err != nil {
		return err
	}

	// Setup logger
	level, err := logrus.ParseLevel(config.Logging)
	if err != nil {
		return err
	}
	logger := logrus.New()
	logger.SetLevel(level)

	logger.Info("Configuration loaded")

	// Canceled on SIGINT/SIGTERM so services can shut down via context
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create and start config application
	app, err := newEngine(logger, config)
	if err != nil {
		return fmt.Errorf("failed to create engine: %w", err)
	}

	if err := app.Start(ctx); err != nil {
		return fmt.Errorf("failed to start engine: %w", err)
	}

	// Wait for interrupt signal
	<-ctx.Done()
	stop()

	// Graceful shutdown
	return app.Stop()
}
