// Package server provides server configuration and management
package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/redis"
)

// Define static errors
var (
	ErrRedisConfigRequired = errors.New("redis configuration is required")
)

// Config holds server configuration
type Config struct { // MetricsAddr is the address to listen on for metrics.
	MetricsAddr string `yaml:"metricsAddr" default:":9090"`
	// HealthCheckAddr is the address to listen on for healthcheck.
	HealthCheckAddr *string `yaml:"healthCheckAddr"`
	// PProfAddr is the address to listen on for pprof.
	PProfAddr *string `yaml:"pprofAddr"`
	// LoggingLevel is the logging level to use.
	LoggingLevel string `yaml:"logging" default:"info"`
	// Redis is the redis configuration.
	Redis *redis.Config `yaml:"redis"`
	// ShutdownTimeout is the timeout for shutting down the server.
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout" default:"10s"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Redis == nil {
		return ErrRedisConfigRequired
	}

	if err := c.Redis.Validate(); err != nil {
		return fmt.Errorf("invalid redis configuration: %w", err)
	}

	return nil
}
