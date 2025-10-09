package transformation

import (
	"errors"
	"fmt"
)

// Type represents the transformation execution pattern
type Type string

const (
	// TypeIncremental represents incremental transformations that track position
	TypeIncremental Type = "incremental"
	// TypeScheduled represents scheduled transformations without position tracking
	TypeScheduled Type = "scheduled"
)

var (
	// ErrDatabaseRequired is returned when database is not specified
	ErrDatabaseRequired = errors.New("database is required")
	// ErrTableRequired is returned when table is not specified
	ErrTableRequired = errors.New("table is required")
	// ErrUnknownTransformationType is returned for unknown transformation types
	ErrUnknownTransformationType = errors.New("unknown transformation type")
)

// Config is the minimal configuration used to determine the transformation type
// Each type has its own complete configuration structure
type Config struct {
	Type     Type              `yaml:"type"`     // Required: "incremental" or "scheduled"
	Database string            `yaml:"database"` // Optional, can fall back to default
	Table    string            `yaml:"table"`    // Required
	Env      map[string]string `yaml:"env,omitempty"`
}

// Validate checks if the base configuration is valid
func (c *Config) Validate() error {
	if c.Database == "" {
		return ErrDatabaseRequired
	}

	if c.Table == "" {
		return ErrTableRequired
	}

	// Type validation
	if c.Type != TypeIncremental && c.Type != TypeScheduled {
		return fmt.Errorf("%w: %s", ErrUnknownTransformationType, c.Type)
	}

	return nil
}

// SetDefaults applies default values to the configuration
func (c *Config) SetDefaults(defaultDatabase string) {
	if c.Database == "" && defaultDatabase != "" {
		c.Database = defaultDatabase
	}
}

// GetID returns the unique identifier for the transformation model
func (c *Config) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}

// IsScheduledType returns true if this is a scheduled transformation
func (c *Config) IsScheduledType() bool {
	return c.Type == TypeScheduled
}

// IsIncrementalType returns true if this is an incremental transformation
func (c *Config) IsIncrementalType() bool {
	return c.Type == TypeIncremental
}
