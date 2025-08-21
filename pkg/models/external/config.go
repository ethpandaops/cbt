// Package external provides external model configuration and validation
package external

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrDatabaseRequired is returned when database is not specified
	ErrDatabaseRequired = errors.New("database is required")
	// ErrTableRequired is returned when table is not specified
	ErrTableRequired = errors.New("table is required")
	// ErrTTLRequired is returned when TTL is not specified
	ErrTTLRequired = errors.New("ttl is required")
)

// Config defines configuration for external models
type Config struct {
	Database string         `yaml:"database" validate:"required"`
	Table    string         `yaml:"table" validate:"required"`
	TTL      *time.Duration `yaml:"ttl"`
	Lag      uint64         `yaml:"lag"`
}

// Validate checks if the external configuration is valid
func (c *Config) Validate() error {
	if c.Database == "" {
		return ErrDatabaseRequired
	}

	if c.Table == "" {
		return ErrTableRequired
	}

	if c.TTL == nil {
		return ErrTTLRequired
	}

	return nil
}

// GetID returns the unique identifier for the external model
func (c *Config) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}
