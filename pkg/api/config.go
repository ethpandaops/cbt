// Package api provides a REST API layer for exposing model metadata and transformation state.
package api

import "errors"

// ErrAPIAddrRequired is returned when API is enabled but no address is configured
var (
	ErrAPIAddrRequired = errors.New("api address is required when API is enabled")
)

// Config represents API service configuration
type Config struct {
	Enabled bool   `yaml:"enabled" default:"false"`
	Addr    string `yaml:"addr" default:":8080" validate:"hostname_port"`
}

// Validate validates the API configuration
func (c *Config) Validate() error {
	if c.Enabled && c.Addr == "" {
		return ErrAPIAddrRequired
	}
	return nil
}
