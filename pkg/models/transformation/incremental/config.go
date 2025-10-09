// Package incremental provides the incremental transformation type handler
package incremental

import (
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/robfig/cron/v3"
)

var (
	// ErrIntervalRequired is returned when interval configuration is missing
	ErrIntervalRequired = errors.New("interval configuration is required")
	// ErrNoSchedulesConfig is returned when no schedules are configured
	ErrNoSchedulesConfig = errors.New("at least one schedule must be configured")
	// ErrDependenciesRequired is returned when dependencies are not specified
	ErrDependenciesRequired = errors.New("dependencies are required for incremental transformations")
	// ErrAdminServiceInvalid is returned when admin service doesn't implement required interface
	ErrAdminServiceInvalid = errors.New("admin service does not implement RecordCompletion")
	// ErrIntervalMaxRequired is returned when interval.max is not specified
	ErrIntervalMaxRequired = errors.New("interval.max is required")
	// ErrInvalidInterval is returned when interval.min exceeds interval.max
	ErrInvalidInterval = errors.New("interval.min cannot exceed interval.max")
	// ErrInvalidLimits is returned when min limit is greater than max limit
	ErrInvalidLimits = errors.New("min limit cannot be greater than max limit")
	// ErrIntervalTypeRequired is returned when interval.type is not specified
	ErrIntervalTypeRequired = errors.New("interval.type is required")
)

// Config defines the configuration for incremental transformation models
type Config struct {
	Type         transformation.Type         `yaml:"type"`
	Database     string                      `yaml:"database"`
	Table        string                      `yaml:"table"`
	Limits       *LimitsConfig               `yaml:"limits,omitempty"`
	Interval     *IntervalConfig             `yaml:"interval"`
	Schedules    *SchedulesConfig            `yaml:"schedules"`
	Dependencies []transformation.Dependency `yaml:"dependencies"`
	Tags         []string                    `yaml:"tags,omitempty"`
	Exec         string                      `yaml:"exec,omitempty"`
	Env          map[string]string           `yaml:"env,omitempty"`
	SQL          string                      `yaml:"-"` // SQL content from separate file

	// OriginalDependencies stores the dependencies before placeholder substitution
	OriginalDependencies []transformation.Dependency `yaml:"-"`
}

// IntervalConfig defines interval configuration for transformations
type IntervalConfig struct {
	Max  uint64 `yaml:"max"`              // Maximum interval size for processing
	Min  uint64 `yaml:"min"`              // Minimum interval size (0 = allow any partial size)
	Type string `yaml:"type" json:"type"` // Required: examples: "second", "slot", "epoch", "block"
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

// Validate checks if the interval configuration is valid
func (c *IntervalConfig) Validate() error {
	if c.Max == 0 {
		return ErrIntervalMaxRequired
	}

	if c.Min > c.Max {
		return ErrInvalidInterval
	}

	if c.Type == "" {
		return ErrIntervalTypeRequired
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
