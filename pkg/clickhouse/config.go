// Package clickhouse provides a ClickHouse client implementation
package clickhouse

import (
	"errors"
	"os"
	"time"
)

// Static errors for configuration validation
var (
	ErrURLRequired = errors.New("URL is required")
)

// Config contains ClickHouse connection and cluster settings
type Config struct {
	URL           string        `yaml:"url" validate:"required,url"`
	Cluster       string        `yaml:"cluster"`
	LocalSuffix   string        `yaml:"localSuffix"`
	QueryTimeout  time.Duration `yaml:"queryTimeout"`
	InsertTimeout time.Duration `yaml:"insertTimeout"`
	Debug         bool          `yaml:"debug"`
	KeepAlive     time.Duration `yaml:"keepAlive"`
	AdminDatabase string        `yaml:"adminDatabase"`
	AdminTable    string        `yaml:"adminTable"`
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

	if c.AdminDatabase == "" {
		c.AdminDatabase = "admin"
	}

	if c.AdminTable == "" {
		c.AdminTable = "cbt"
	}
}

// MapDatabase maps a logical database name to a physical database name
// If CBT_DATABASE_PREFIX env var is set, prepends it to the database name
// Otherwise returns the original name unchanged
func (c *Config) MapDatabase(logicalName string) string {
	if prefix := os.Getenv("CBT_DATABASE_PREFIX"); prefix != "" {
		return prefix + logicalName
	}
	return logicalName
}
