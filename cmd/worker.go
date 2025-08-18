package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/creasty/defaults"
	"github.com/ethpandaops/cbt/pkg/worker"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

//nolint:gochecknoglobals // Cobra flags are typically global
var (
	workerCfgFile string
)

//nolint:gochecknoglobals // Cobra commands are typically global
var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start the CBT worker service",
	Long:  `The worker service processes transformation tasks.`,
	RunE:  runWorker,
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringVar(&workerCfgFile, "config", "worker.yaml", "config file (default is worker.yaml)")
}

func loadWorkerConfigFromFile(file string) (*worker.Config, error) {
	if file == "" {
		file = "worker.yaml"
	}

	config := &worker.Config{}

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

func runWorker(cmd *cobra.Command, _ []string) error {
	// Silence usage on error
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	// Load configuration
	config, err := loadWorkerConfigFromFile(workerCfgFile)
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

	// Create and start worker application
	app := worker.NewApplication(config, logger)
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
