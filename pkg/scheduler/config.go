// Package scheduler provides task scheduling services
package scheduler

import (
	"errors"
	"time"
)

var (
	// ErrInvalidConcurrency is returned when concurrency is not positive
	ErrInvalidConcurrency = errors.New("concurrency must be positive")
)

// Config defines scheduler configuration
type Config struct {
	Concurrency     int           `yaml:"concurrency" default:"10"`
	Consolidation   string        `yaml:"consolidation" default:"@every 10m"`
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout" default:"10s"`
	TaskTimeout     time.Duration `yaml:"taskTimeout" default:"30m"`
}

// Validate checks if the scheduler configuration is valid
func (c *Config) Validate() error {
	if c.Concurrency <= 0 {
		return ErrInvalidConcurrency
	}

	return nil
}
