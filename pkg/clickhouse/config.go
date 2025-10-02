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

// Default values
const (
	defaultAdminDatabase = "admin"
)

// AdminTableConfig represents the configuration for a type-specific admin table
type AdminTableConfig struct {
	Database string `yaml:"database"`
	Table    string `yaml:"table"`
}

// AdminConfig contains configurations for different transformation type admin tables
type AdminConfig struct {
	Incremental AdminTableConfig `yaml:"incremental"`
	Scheduled   AdminTableConfig `yaml:"scheduled"`
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

	// Set defaults for admin config
	if c.Admin.Incremental.Database == "" {
		c.Admin.Incremental.Database = defaultAdminDatabase
	}

	if c.Admin.Incremental.Table == "" {
		c.Admin.Incremental.Table = "cbt_incremental"
	}

	if c.Admin.Scheduled.Database == "" {
		c.Admin.Scheduled.Database = defaultAdminDatabase
	}

	if c.Admin.Scheduled.Table == "" {
		c.Admin.Scheduled.Table = "cbt_scheduled"
	}
}
