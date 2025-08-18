// Package cmd contains the CLI commands for CBT
package cmd

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

//nolint:gochecknoglobals // Global vars needed for cobra CLI
var (
	cfgFile string
	logger  *logrus.Logger
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
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error, fatal, panic)")

	// Initialize logger
	logger = logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
}

func initConfig() {
	if cfgFile == "" {
		cfgFile = "./config.yaml"
	}

	// Set log level
	logLevel, err := rootCmd.PersistentFlags().GetString("log-level")
	if err != nil {
		logLevel = "info" // Default to info if error
	}
	level, parseErr := logrus.ParseLevel(logLevel)
	if parseErr != nil {
		logger.WithError(parseErr).Warn("Invalid log level, defaulting to info")
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
}
