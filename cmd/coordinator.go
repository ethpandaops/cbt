package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/creasty/defaults"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

//nolint:gochecknoglobals // Cobra flags are typically global
var (
	coordinatorCfgFile string
)

//nolint:gochecknoglobals // Cobra commands are typically global
var coordinatorCmd = &cobra.Command{
	Use:   "coordinator",
	Short: "Start the CBT coordinator service",
	Long:  `The coordinator service schedules tasks and manages dependencies.`,
	RunE:  runCoordinator,
}

func init() {
	rootCmd.AddCommand(coordinatorCmd)
	coordinatorCmd.Flags().StringVar(&coordinatorCfgFile, "config", "coordinator.yaml", "config file (default is coordinator.yaml)")
}

func loadCoordinatorConfigFromFile(file string) (*coordinator.Config, error) {
	if file == "" {
		file = "coordinator.yaml"
	}

	config := &coordinator.Config{}

	if err := defaults.Set(config); err != nil {
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

func runCoordinator(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	config, err := loadCoordinatorConfigFromFile(coordinatorCfgFile)
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

	// Create and start coordinator application
	app := coordinator.NewApplication(config, logger)
	if err := app.Start(); err != nil {
		return err
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	return app.Stop()
}
