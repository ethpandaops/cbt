package scheduler

import (
	"errors"
	"time"
)

var (
	// ErrInvalidConcurrency is returned when concurrency is not positive
	ErrInvalidConcurrency = errors.New("concurrency must be positive")
)

type Config struct {
	Interval    time.Duration `yaml:"interval" default:"1m"`
	Concurrency int           `yaml:"concurrency" default:"10"`
}

func (c *Config) Validate() error {
	if c.Concurrency <= 0 {
		return ErrInvalidConcurrency
	}

	return nil
}
