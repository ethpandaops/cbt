// Package cmd provides CLI commands
package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/creasty/defaults"
	"github.com/ethpandaops/cbt/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	log              = logrus.New()
	serverConfigFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cbt",
	Short: "Runs Clickhouse Build Tool.",
	Long:  `Runs Clickhouse Build Tool.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		initCommon()

		return runServer(cmd.Context())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&serverConfigFile, "config", "", "config file (default is ./config.yaml)")
}

func initCommon() {

}

func runServer(ctx context.Context) error {
	config, err := loadServerConfigFromFile(serverConfigFile)
	if err != nil {
		return fmt.Errorf("failed to load server config: %w", err)
	}

	level, err := logrus.ParseLevel(config.LoggingLevel)
	if err != nil {
		log.WithError(err).Warn("Invalid logging level, using info")

		level = logrus.InfoLevel
	}

	log.SetLevel(level)

	srv, err := server.NewServer(ctx, log, "cbt", config)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("server failed to start: %w", err)
	}

	log.Info("cbt server exited - cya!")

	return nil
}

func loadServerConfigFromFile(file string) (*server.Config, error) {
	if file == "" {
		file = "config.yaml"
	}

	config := &server.Config{}

	if err := defaults.Set(config); err != nil {
		return nil, err
	}

	yamlFile, err := os.ReadFile(file) // #nosec G304 -- file path is from config flag
	if err != nil {
		return nil, err
	}

	type plain server.Config

	if err := yaml.Unmarshal(yamlFile, (*plain)(config)); err != nil {
		return nil, err
	}

	return config, nil
}
