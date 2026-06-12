// Package api provides a REST API layer for exposing model metadata and transformation state.
package api //nolint:revive // package name follows existing project layout.

// Config represents API service configuration
type Config struct {
	Enabled bool   `yaml:"enabled" default:"false"`
	Addr    string `yaml:"addr" default:":8080" validate:"hostname_port"`
}
