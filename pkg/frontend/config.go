package frontend

import "errors"

// ErrFrontendAddrRequired is returned when frontend is enabled but no address is configured
var (
	ErrFrontendAddrRequired = errors.New("frontend address is required when frontend is enabled")
)

// Config represents frontend service configuration
type Config struct {
	Enabled bool   `yaml:"enabled" default:"false"`
	Addr    string `yaml:"addr" default:":8080" validate:"hostname_port"`
}

// Validate validates the frontend configuration
func (c *Config) Validate() error {
	if c.Enabled && c.Addr == "" {
		return ErrFrontendAddrRequired
	}
	return nil
}
