package transformation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/robfig/cron/v3"
)

var (
	// ErrDatabaseRequired is returned when database is not specified
	ErrDatabaseRequired = errors.New("database is required")
	// ErrTableRequired is returned when table is not specified
	ErrTableRequired = errors.New("table is required")
	// ErrNoSchedulesConfig is returned when no schedules are configured
	ErrNoSchedulesConfig = errors.New("at least one schedule must be configured")
	// ErrIntervalRequired is returned when interval is not specified
	ErrIntervalRequired = errors.New("interval configuration is required")
	// ErrIntervalMaxRequired is returned when interval.max is not specified
	ErrIntervalMaxRequired = errors.New("interval.max is required")
	// ErrInvalidInterval is returned when interval.min exceeds interval.max
	ErrInvalidInterval = errors.New("interval.min cannot exceed interval.max")
	// ErrDependenciesRequired is returned when dependencies are not specified
	ErrDependenciesRequired = errors.New("dependencies is required")
	// ErrInvalidLimits is returned when min limit is greater than max limit
	ErrInvalidLimits = errors.New("min limit cannot be greater than max limit")
)

// Config defines the configuration for transformation models
type Config struct {
	Database     string           `yaml:"database"` // Optional, can fall back to default
	Table        string           `yaml:"table"`
	Limits       *LimitsConfig    `yaml:"limits,omitempty"`
	Interval     *IntervalConfig  `yaml:"interval"`
	Schedules    *SchedulesConfig `yaml:"schedules"`
	Dependencies []string         `yaml:"dependencies"`
	Tags         []string         `yaml:"tags"`
}

// IntervalConfig defines interval configuration for transformations
type IntervalConfig struct {
	Max uint64 `yaml:"max"` // Maximum interval size for processing
	Min uint64 `yaml:"min"` // Minimum interval size (0 = allow any partial size)
}

// SchedulesConfig defines scheduling configuration for transformations
type SchedulesConfig struct {
	ForwardFill string `yaml:"forwardfill,omitempty"` // Forward fill schedule (optional)
	Backfill    string `yaml:"backfill,omitempty"`    // Backfill schedule (optional)
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

	// Interval must be configured
	if c.Interval == nil {
		return ErrIntervalRequired
	}

	if err := c.Interval.Validate(); err != nil {
		return fmt.Errorf("interval validation failed: %w", err)
	}

	// At least one schedule must be configured
	if c.Schedules == nil || (c.Schedules.ForwardFill == "" && c.Schedules.Backfill == "") {
		return ErrNoSchedulesConfig
	}

	if err := c.Schedules.Validate(); err != nil {
		return fmt.Errorf("schedules validation failed: %w", err)
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

// SetDefaults applies default values to the configuration
func (c *Config) SetDefaults(defaultDatabase string) {
	if c.Database == "" && defaultDatabase != "" {
		c.Database = defaultDatabase
	}
}

// SubstituteDependencyPlaceholders replaces {{external}} and {{transformation}} placeholders
// with the actual default database names
func (c *Config) SubstituteDependencyPlaceholders(externalDefaultDB, transformationDefaultDB string) {
	for i, dep := range c.Dependencies {
		if externalDefaultDB != "" {
			c.Dependencies[i] = strings.ReplaceAll(dep, "{{external}}", externalDefaultDB)
		}
		if transformationDefaultDB != "" {
			c.Dependencies[i] = strings.ReplaceAll(c.Dependencies[i], "{{transformation}}", transformationDefaultDB)
		}
	}
}

// GetID returns the unique identifier for the transformation model
func (c *Config) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}

// GetMaxInterval returns the maximum interval size
func (c *Config) GetMaxInterval() uint64 {
	if c.Interval != nil {
		return c.Interval.Max
	}
	return 0
}

// GetMinInterval returns the minimum interval size
func (c *Config) GetMinInterval() uint64 {
	if c.Interval != nil {
		return c.Interval.Min
	}
	return 0
}

// AllowsPartialIntervals returns true if partial intervals are allowed
func (c *Config) AllowsPartialIntervals() bool {
	return c.Interval != nil && c.Interval.Min < c.Interval.Max
}

// GetForwardSchedule returns the schedule for forward fill operations
func (c *Config) GetForwardSchedule() string {
	if c.Schedules != nil {
		return c.Schedules.ForwardFill
	}
	return ""
}

// GetBackfillSchedule returns the schedule for backfill operations
func (c *Config) GetBackfillSchedule() string {
	if c.Schedules != nil {
		return c.Schedules.Backfill
	}
	return ""
}

// IsForwardFillEnabled returns true if forward fill is configured
func (c *Config) IsForwardFillEnabled() bool {
	return c.Schedules != nil && c.Schedules.ForwardFill != ""
}

// IsBackfillEnabled returns true if backfill is configured
func (c *Config) IsBackfillEnabled() bool {
	return c.Schedules != nil && c.Schedules.Backfill != ""
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

// Validate checks if the interval configuration is valid
func (c *IntervalConfig) Validate() error {
	if c.Max == 0 {
		return ErrIntervalMaxRequired
	}

	if c.Min > c.Max {
		return ErrInvalidInterval
	}

	return nil
}

// Validate checks if the schedules configuration is valid
func (c *SchedulesConfig) Validate() error {
	if c.ForwardFill != "" {
		if err := ValidateScheduleFormat(c.ForwardFill); err != nil {
			return fmt.Errorf("invalid forwardfill schedule: %w", err)
		}
	}

	if c.Backfill != "" {
		if err := ValidateScheduleFormat(c.Backfill); err != nil {
			return fmt.Errorf("invalid backfill schedule: %w", err)
		}
	}

	return nil
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
