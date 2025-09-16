// Package clickhouse provides a ClickHouse client implementation
package clickhouse

import (
	"errors"
	"time"
)

// Static errors for configuration validation
var (
	ErrURLRequired = errors.New("URL is required")
)

// AdminTableConfig represents configuration for a specific admin table type
type AdminTableConfig struct {
	Database string `yaml:"database"`
	Table    string `yaml:"table"`
}

// AdminConfig contains configuration for type-specific admin tables
type AdminConfig struct {
	Incremental AdminTableConfig `yaml:"incremental"`
	Scheduled   AdminTableConfig `yaml:"scheduled"`
}

// SetDefaults sets default values for admin configuration
func (c *AdminConfig) SetDefaults() {
	// Incremental table defaults
	if c.Incremental.Database == "" {
		c.Incremental.Database = "admin"
	}
	if c.Incremental.Table == "" {
		c.Incremental.Table = "cbt_incremental"
	}

	// Scheduled table defaults
	if c.Scheduled.Database == "" {
		c.Scheduled.Database = "admin"
	}
	if c.Scheduled.Table == "" {
		c.Scheduled.Table = "cbt_scheduled"
	}
}

// Config contains ClickHouse connection and cluster settings
type Config struct {
	URL           string        `yaml:"url" validate:"required,url"`
	Cluster       string        `yaml:"cluster"`
	LocalSuffix   string        `yaml:"localSuffix"`
	QueryTimeout  time.Duration `yaml:"queryTimeout"`
	InsertTimeout time.Duration `yaml:"insertTimeout"`
	Debug         bool          `yaml:"debug"`
	KeepAlive     time.Duration `yaml:"keepAlive"`
	Admin         AdminConfig   `yaml:"admin"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.URL == "" {
		return ErrURLRequired
	}

	return nil
}

// SetDefaults sets default values for the configuration
func (c *Config) SetDefaults() {
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 30 * time.Second
	}

	if c.InsertTimeout == 0 {
		c.InsertTimeout = 5 * time.Minute
	}

	if c.KeepAlive == 0 {
		c.KeepAlive = 30 * time.Second
	}

	c.Admin.SetDefaults()
}
