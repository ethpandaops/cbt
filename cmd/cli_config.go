package cmd

import (
	"errors"
	"os"
	"time"

	"github.com/creasty/defaults"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"gopkg.in/yaml.v3"
)

var (
	// ErrClickHouseURLRequired is returned when ClickHouse URL is not provided
	ErrClickHouseURLRequired = errors.New("clickhouse URL is required")
)

// CLIConfig represents minimal configuration for CLI commands
type CLIConfig struct {
	// Logging level
	Logging string `yaml:"logging" default:"error" validate:"oneof=panic fatal warn info debug trace"`

	// ClickHouse configuration
	ClickHouse clickhouse.Config `yaml:"clickhouse"`

	// Redis configuration (optional, only needed for rerun)
	Redis struct {
		URL string `yaml:"url"`
	} `yaml:"redis,omitempty"`
}

// Validate validates the CLI configuration
func (c *CLIConfig) Validate() error {
	if c.ClickHouse.URL == "" {
		return ErrClickHouseURLRequired
	}

	// Set defaults for ClickHouse if not specified
	if c.ClickHouse.QueryTimeout == 0 {
		c.ClickHouse.QueryTimeout = 30 * time.Second
	}
	if c.ClickHouse.InsertTimeout == 0 {
		c.ClickHouse.InsertTimeout = 60 * time.Second
	}
	if c.ClickHouse.KeepAlive == 0 {
		c.ClickHouse.KeepAlive = 30 * time.Second
	}

	return nil
}

// LoadCLIConfig loads CLI configuration from a YAML file
func LoadCLIConfig(path string) (*CLIConfig, error) {
	if path == "" {
		path = "cli.yaml"
	}

	config := &CLIConfig{}

	if err := defaults.Set(config); err != nil {
		return nil, err
	}

	// Try to read the file, but allow it to not exist
	yamlFile, err := os.ReadFile(path) //nolint:gosec // User-provided config file path
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, use defaults or environment variables
			return config, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(yamlFile, config); err != nil {
		return nil, err
	}

	return config, nil
}
