package worker

import (
	"errors"
)

const (
	// DefaultConcurrency is the default number of concurrent workers.
	DefaultConcurrency = 10
	// DefaultShutdownTimeoutSeconds is the default shutdown timeout in seconds.
	DefaultShutdownTimeoutSeconds = 30
)

var (
	// ErrNoQueuesConfigured is returned when no queues are configured
	ErrNoQueuesConfigured = errors.New("at least one queue must be configured")
	// ErrInvalidConcurrency is returned when concurrency is not positive
	ErrInvalidConcurrency = errors.New("concurrency must be positive")
	// ErrWorkerShutdownTimeout is returned when worker shutdown times out
	ErrWorkerShutdownTimeout = errors.New("worker shutdown timed out")
)

// Config contains worker-specific settings
type Config struct {
	Concurrency     int      `yaml:"concurrency" default:"10"`
	Tags            []string `yaml:"tags,omitempty"` // Optional tag-based model filtering
	ShutdownTimeout int      `yaml:"shutdownTimeout" default:"30"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Concurrency <= 0 {
		return ErrInvalidConcurrency
	}

	return nil
}
