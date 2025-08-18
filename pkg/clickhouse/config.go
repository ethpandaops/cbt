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

// Config contains ClickHouse connection and cluster settings
type Config struct {
	URL           string        `yaml:"url" validate:"required,url"`
	Cluster       string        `yaml:"cluster"`
	LocalSuffix   string        `yaml:"local_suffix"`
	QueryTimeout  time.Duration `yaml:"query_timeout"`
	InsertTimeout time.Duration `yaml:"insert_timeout"`
	Debug         bool          `yaml:"debug"`
	KeepAlive     time.Duration `yaml:"keep_alive"`
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
}
