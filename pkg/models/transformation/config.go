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
	// ErrNoProcessingConfig is returned when neither forwardfill nor backfill is specified
	ErrNoProcessingConfig = errors.New("at least one of forwardfill or backfill must be configured")
	// ErrIntervalRequired is returned when interval is not specified
	ErrIntervalRequired = errors.New("interval is required")
	// ErrScheduleRequired is returned when schedule is not specified
	ErrScheduleRequired = errors.New("schedule is required")
	// ErrIncompleteConfig is returned when config has interval but no schedule or vice versa
	ErrIncompleteConfig = errors.New("both interval and schedule must be specified")
	// ErrDependenciesRequired is returned when dependencies are not specified
	ErrDependenciesRequired = errors.New("dependencies is required")
	// ErrInvalidLimits is returned when min limit is greater than max limit
	ErrInvalidLimits = errors.New("min limit cannot be greater than max limit")
)

// Config defines the configuration for transformation models
type Config struct {
	Database     string             `yaml:"database"`
	Table        string             `yaml:"table"`
	Limits       *LimitsConfig      `yaml:"limits,omitempty"`
	ForwardFill  *ForwardFillConfig `yaml:"forwardfill"`
	Backfill     *BackfillConfig    `yaml:"backfill,omitempty"`
	Dependencies []string           `yaml:"dependencies"`
	Tags         []string           `yaml:"tags"`
}

// ForwardFillConfig defines forward fill configuration for transformations
type ForwardFillConfig struct {
	Interval uint64 `yaml:"interval"`
	Schedule string `yaml:"schedule"`
}

// BackfillConfig defines backfill configuration for transformations
type BackfillConfig struct {
	Interval uint64 `yaml:"interval"`
	Schedule string `yaml:"schedule"`
}

// LimitsConfig defines position limits for transformations
type LimitsConfig struct {
	Min uint64 `yaml:"min,omitempty"`
	Max uint64 `yaml:"max,omitempty"`
}

// Validate checks if the transformation configuration is valid
func (c *Config) Validate() error {
	if c.Database == "" {
		return ErrDatabaseRequired
	}

	if c.Table == "" {
		return ErrTableRequired
	}

	// At least one of forwardfill or backfill must be configured
	if c.ForwardFill == nil && c.Backfill == nil {
		return ErrNoProcessingConfig
	}

	// ForwardFill is optional, but if specified must be valid
	if c.ForwardFill != nil {
		if err := c.ForwardFill.Validate(); err != nil {
			return fmt.Errorf("forwardfill validation failed: %w", err)
		}
	}

	// Backfill is optional, but if specified must be valid
	if c.Backfill != nil {
		if err := c.Backfill.Validate(); err != nil {
			return fmt.Errorf("backfill validation failed: %w", err)
		}
	}

	// Limits are optional, but if specified must be valid
	if c.Limits != nil {
		if err := c.Limits.Validate(); err != nil {
			return fmt.Errorf("limits validation failed: %w", err)
		}
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

// GetForwardInterval returns the interval for forward fill operations
func (c *Config) GetForwardInterval() uint64 {
	if c.ForwardFill != nil {
		return c.ForwardFill.Interval
	}
	return 0
}

// GetForwardSchedule returns the schedule for forward fill operations
func (c *Config) GetForwardSchedule() string {
	if c.ForwardFill != nil {
		return c.ForwardFill.Schedule
	}
	return ""
}

// GetBackfillInterval returns the interval for backfill operations
func (c *Config) GetBackfillInterval() uint64 {
	if c.Backfill != nil {
		return c.Backfill.Interval
	}
	return 0
}

// IsBackfillEnabled returns true if backfill is configured
func (c *Config) IsBackfillEnabled() bool {
	return c.Backfill != nil
}

// GetMinLimit returns the minimum position limit
func (c *Config) GetMinLimit() uint64 {
	if c.Limits != nil {
		return c.Limits.Min
	}
	return 0
}

// GetMaxLimit returns the maximum position limit (0 means no limit)
func (c *Config) GetMaxLimit() uint64 {
	if c.Limits != nil {
		return c.Limits.Max
	}
	return 0
}

// HasLimits returns true if limits are configured
func (c *Config) HasLimits() bool {
	return c.Limits != nil && (c.Limits.Min > 0 || c.Limits.Max > 0)
}

// Validate checks if the forward fill configuration is valid
func (c *ForwardFillConfig) Validate() error {
	if c.Interval == 0 {
		return ErrIntervalRequired
	}

	if c.Schedule == "" {
		return ErrScheduleRequired
	}

	return ValidateScheduleFormat(c.Schedule)
}

// Validate checks if the backfill configuration is valid
func (c *BackfillConfig) Validate() error {
	if c.Interval == 0 {
		return ErrIntervalRequired
	}

	if c.Schedule == "" {
		return ErrScheduleRequired
	}

	return ValidateScheduleFormat(c.Schedule)
}

// Validate checks if the limits configuration is valid
func (c *LimitsConfig) Validate() error {
	// Check that min is not greater than max if both are set
	if c.Min > 0 && c.Max > 0 && c.Min > c.Max {
		return ErrInvalidLimits
	}
	return nil
}

// ValidateScheduleFormat validates a cron schedule expression
func ValidateScheduleFormat(schedule string) error {
	_, err := cron.ParseStandard(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	return nil
}
