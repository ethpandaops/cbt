package transformation

import (
	"errors"
	"fmt"

	"github.com/robfig/cron/v3"
)

var (
	// ErrDatabaseRequired is returned when database is not specified
	ErrDatabaseRequired = errors.New("database is required")
	// ErrTableRequired is returned when table is not specified
	ErrTableRequired = errors.New("table is required")
	// ErrIntervalRequired is returned when interval is not specified
	ErrIntervalRequired = errors.New("interval is required")
	// ErrBackfillRequired is returned when backfill is not specified
	ErrBackfillRequired = errors.New("backfill is required")
	// ErrDependenciesRequired is returned when dependencies are not specified
	ErrDependenciesRequired = errors.New("dependencies is required")
	// ErrScheduleRequiredForBackfill is returned when schedule is not specified for backfill
	ErrScheduleRequiredForBackfill = errors.New("schedule is required when backfill is enabled")
)

// Config defines the configuration for transformation models
type Config struct {
	Database     string          `yaml:"database"`
	Table        string          `yaml:"table"`
	Interval     uint64          `yaml:"interval"`
	Schedule     string          `yaml:"schedule"`
	Backfill     *BackfillConfig `yaml:"backfill,omitempty"`
	Dependencies []string        `yaml:"dependencies"`
	Tags         []string        `yaml:"tags"`
}

// BackfillConfig defines backfill configuration for transformations
type BackfillConfig struct {
	Enabled  bool   `yaml:"enabled,omitempty"`
	Schedule string `yaml:"schedule,omitempty"`
	Minimum  uint64 `yaml:"minimum,omitempty"`
}

// Validate checks if the transformation configuration is valid
func (c *Config) Validate() error {
	if c.Database == "" {
		return ErrDatabaseRequired
	}

	if c.Table == "" {
		return ErrTableRequired
	}

	if c.Interval == 0 {
		return ErrIntervalRequired
	}

	if err := ValidateScheduleFormat(c.Schedule); err != nil {
		return err
	}

	if c.Backfill == nil {
		return ErrBackfillRequired
	}

	if len(c.Dependencies) == 0 {
		return ErrDependenciesRequired
	}

	return nil
}

// GetID returns the unique identifier for the transformation model
func (c *Config) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}

// Validate checks if the backfill configuration is valid
func (c *BackfillConfig) Validate() error {
	if c.Enabled && c.Schedule == "" {
		return ErrScheduleRequiredForBackfill
	}

	return ValidateScheduleFormat(c.Schedule)
}

// ValidateScheduleFormat validates a cron schedule expression
func ValidateScheduleFormat(schedule string) error {
	_, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	return nil
}
