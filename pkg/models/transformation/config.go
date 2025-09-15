package transformation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
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
	// ErrInvalidDependencyType is returned when dependency has invalid YAML type
	ErrInvalidDependencyType = errors.New("dependency must be a string or array of strings")
	// ErrInvalidDependencyArrayItem is returned when dependency array contains non-string
	ErrInvalidDependencyArrayItem = errors.New("expected string in dependency array")
	// ErrEmptyDependencyGroup is returned when dependency group is empty
	ErrEmptyDependencyGroup = errors.New("dependency group cannot be empty")
)

// Dependency represents a dependency that can be either a string (AND) or an array of strings (OR)
type Dependency struct {
	// IsGroup indicates if this is an OR group (array) or a single dependency (string)
	IsGroup bool
	// SingleDep holds the dependency ID for single dependencies
	SingleDep string
	// GroupDeps holds multiple dependency IDs for OR groups
	GroupDeps []string
}

// UnmarshalYAML implements custom YAML unmarshaling for mixed dependency types
func (d *Dependency) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		// Single string dependency - must be a string, not a number
		if node.Tag != "!!str" && node.Tag != "" && node.Tag != "!" {
			return fmt.Errorf("%w: expected string but got %s", ErrInvalidDependencyType, node.Tag)
		}
		d.IsGroup = false
		d.SingleDep = node.Value
		return nil
	case yaml.SequenceNode:
		// Array of dependencies (OR group)
		d.IsGroup = true
		d.GroupDeps = make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			if item.Kind != yaml.ScalarNode {
				return fmt.Errorf("%w: got %v", ErrInvalidDependencyArrayItem, item.Kind)
			}
			// Ensure it's a string, not a number or other type
			if item.Tag != "!!str" && item.Tag != "" && item.Tag != "!" {
				return fmt.Errorf("%w: expected string but got %s", ErrInvalidDependencyArrayItem, item.Tag)
			}
			d.GroupDeps = append(d.GroupDeps, item.Value)
		}
		if len(d.GroupDeps) == 0 {
			return ErrEmptyDependencyGroup
		}
		return nil
	case yaml.DocumentNode, yaml.MappingNode, yaml.AliasNode:
		return fmt.Errorf("%w: got %v", ErrInvalidDependencyType, node.Kind)
	default:
		return fmt.Errorf("%w: got %v", ErrInvalidDependencyType, node.Kind)
	}
}

// MarshalYAML implements custom YAML marshaling for mixed dependency types
func (d Dependency) MarshalYAML() (interface{}, error) {
	if d.IsGroup {
		return d.GroupDeps, nil
	}
	return d.SingleDep, nil
}

// GetAllDependencies returns all dependency IDs from this dependency (flattened)
func (d *Dependency) GetAllDependencies() []string {
	if d.IsGroup {
		return d.GroupDeps
	}
	return []string{d.SingleDep}
}

// Config defines the configuration for transformation models
type Config struct {
	Database     string           `yaml:"database"` // Optional, can fall back to default
	Table        string           `yaml:"table"`
	Limits       *LimitsConfig    `yaml:"limits,omitempty"`
	Interval     *IntervalConfig  `yaml:"interval"`
	Schedules    *SchedulesConfig `yaml:"schedules"`
	Dependencies []Dependency     `yaml:"dependencies"`
	Tags         []string         `yaml:"tags"`

	// OriginalDependencies stores the dependencies before placeholder substitution
	// This is used by the template engine to create dual entries
	OriginalDependencies []Dependency `yaml:"-"`
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

	// At least one schedule must be configured
	if c.Schedules == nil || (c.Schedules.ForwardFill == "" && c.Schedules.Backfill == "") {
		return ErrNoSchedulesConfig
	}

	if err := c.Schedules.Validate(); err != nil {
		return fmt.Errorf("schedules validation failed: %w", err)
	}

	// Validate based on whether this is standalone or not
	if c.IsStandalone() {
		return c.validateStandalone()
	}

	return c.validateWithDependencies()
}

// validateStandalone validates configuration for standalone transformations
func (c *Config) validateStandalone() error {
	// Interval is optional for standalone transformations
	if c.Interval != nil {
		if err := c.Interval.Validate(); err != nil {
			return fmt.Errorf("interval validation failed: %w", err)
		}
	}

	// Limits are optional
	if c.Limits != nil {
		if err := c.Limits.Validate(); err != nil {
			return fmt.Errorf("limits validation failed: %w", err)
		}
	}

	return nil
}

// validateWithDependencies validates configuration for transformations with dependencies
func (c *Config) validateWithDependencies() error {
	// Interval required when dependencies exist
	if c.Interval == nil {
		return ErrIntervalRequired
	}

	if err := c.Interval.Validate(); err != nil {
		return fmt.Errorf("interval validation failed: %w", err)
	}

	// Limits are optional
	if c.Limits != nil {
		if err := c.Limits.Validate(); err != nil {
			return fmt.Errorf("limits validation failed: %w", err)
		}
	}

	return nil
}

// GetFlattenedDependencies returns all dependencies as a flat string array
// This includes all dependencies from both single and OR groups
func (c *Config) GetFlattenedDependencies() []string {
	var result []string
	for _, dep := range c.Dependencies {
		result = append(result, dep.GetAllDependencies()...)
	}
	return result
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
	// Deep copy original dependencies before substitution
	c.OriginalDependencies = make([]Dependency, len(c.Dependencies))
	for i := range c.Dependencies {
		origDep := Dependency{
			IsGroup:   c.Dependencies[i].IsGroup,
			SingleDep: c.Dependencies[i].SingleDep,
		}
		if c.Dependencies[i].IsGroup {
			// Deep copy the group deps slice
			origDep.GroupDeps = make([]string, len(c.Dependencies[i].GroupDeps))
			copy(origDep.GroupDeps, c.Dependencies[i].GroupDeps)
		}
		c.OriginalDependencies[i] = origDep
	}

	for i := range c.Dependencies {
		c.Dependencies[i] = c.substituteDependency(c.Dependencies[i], externalDefaultDB, transformationDefaultDB)
	}
}

// substituteDependency replaces placeholders in a single dependency
func (c *Config) substituteDependency(dep Dependency, externalDB, transformationDB string) Dependency {
	if dep.IsGroup {
		// Substitute in group dependencies
		for j := range dep.GroupDeps {
			dep.GroupDeps[j] = c.substitutePlaceholders(dep.GroupDeps[j], externalDB, transformationDB)
		}
	} else {
		// Substitute in single dependency
		dep.SingleDep = c.substitutePlaceholders(dep.SingleDep, externalDB, transformationDB)
	}
	return dep
}

// substitutePlaceholders replaces placeholders in a string
func (c *Config) substitutePlaceholders(s, externalDB, transformationDB string) string {
	if externalDB != "" {
		s = strings.ReplaceAll(s, "{{external}}", externalDB)
	}
	if transformationDB != "" {
		s = strings.ReplaceAll(s, "{{transformation}}", transformationDB)
	}
	return s
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

// IsStandalone returns true if this is a standalone transformation (no dependencies)
func (c *Config) IsStandalone() bool {
	return len(c.Dependencies) == 0
}
