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
	// ErrTypeRequired is returned when type is not specified
	ErrTypeRequired = errors.New("type is required")
	// ErrInvalidType is returned when type is not valid
	ErrInvalidType = errors.New("type must be 'incremental' or 'scheduled'")
	// ErrScheduleRequired is returned when schedule is not specified for scheduled type
	ErrScheduleRequired = errors.New("schedule is required for scheduled transformations")
	// ErrSchedulesRequired is returned when schedules are not specified for incremental type
	ErrSchedulesRequired = errors.New("schedules configuration is required for incremental transformations")
	// ErrIntervalRequired is returned when interval is not specified
	ErrIntervalRequired = errors.New("interval configuration is required for incremental transformations")
	// ErrDependenciesRequired is returned when dependencies are not specified for incremental type
	ErrDependenciesRequired = errors.New("dependencies are required for incremental transformations")
	// ErrIntervalMaxRequired is returned when interval.max is not specified
	ErrIntervalMaxRequired = errors.New("interval.max is required")
	// ErrInvalidInterval is returned when interval.min exceeds interval.max
	ErrInvalidInterval = errors.New("interval.min cannot exceed interval.max")
	// ErrInvalidLimits is returned when min limit is greater than max limit
	ErrInvalidLimits = errors.New("min limit cannot be greater than max limit")
	// ErrInvalidDependencyType is returned when dependency has invalid YAML type
	ErrInvalidDependencyType = errors.New("dependency must be a string or array of strings")
	// ErrInvalidDependencyArrayItem is returned when dependency array contains non-string
	ErrInvalidDependencyArrayItem = errors.New("expected string in dependency array")
	// ErrEmptyDependencyGroup is returned when dependency group is empty
	ErrEmptyDependencyGroup = errors.New("dependency group cannot be empty")
	// ErrScheduledNoDependencies is returned when scheduled type has dependencies
	ErrScheduledNoDependencies = errors.New("scheduled transformations cannot have dependencies")
	// ErrScheduledNoInterval is returned when scheduled type has interval
	ErrScheduledNoInterval = errors.New("scheduled transformations cannot have interval configuration")
	// ErrScheduledNoSchedules is returned when scheduled type has schedules
	ErrScheduledNoSchedules = errors.New("scheduled transformations should use 'schedule' not 'schedules'")
	// ErrIncrementalNoSchedule is returned when incremental type has schedule
	ErrIncrementalNoSchedule = errors.New("incremental transformations should use 'schedules' not 'schedule'")
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

// Type represents the type of transformation
type Type string

const (
	// TypeIncremental represents incremental transformations with position tracking
	TypeIncremental Type = "incremental"
	// TypeScheduled represents scheduled transformations without position tracking
	TypeScheduled Type = "scheduled"
)

// Config defines the configuration for transformation models
type Config struct {
	Type         Type             `yaml:"type"`
	Database     string           `yaml:"database"` // Optional, can fall back to default
	Table        string           `yaml:"table"`
	Limits       *LimitsConfig    `yaml:"limits,omitempty"`
	Interval     *IntervalConfig  `yaml:"interval,omitempty"`
	Schedules    *SchedulesConfig `yaml:"schedules,omitempty"`
	Schedule     string           `yaml:"schedule,omitempty"` // For scheduled type
	Dependencies []Dependency     `yaml:"dependencies,omitempty"`
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

	if c.Type == "" {
		return ErrTypeRequired
	}

	if c.Type != TypeIncremental && c.Type != TypeScheduled {
		return ErrInvalidType
	}

	// Validate based on type
	switch c.Type {
	case TypeIncremental:
		return c.validateIncremental()
	case TypeScheduled:
		return c.validateScheduled()
	default:
		return ErrInvalidType
	}
}

// validateScheduled validates configuration for scheduled transformations
func (c *Config) validateScheduled() error {
	// Schedule is required
	if c.Schedule == "" {
		return ErrScheduleRequired
	}

	if err := ValidateScheduleFormat(c.Schedule); err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}

	// Scheduled transformations cannot have dependencies
	if len(c.Dependencies) > 0 {
		return ErrScheduledNoDependencies
	}

	// Scheduled transformations cannot have interval
	if c.Interval != nil {
		return ErrScheduledNoInterval
	}

	// Scheduled transformations cannot have schedules (use schedule instead)
	if c.Schedules != nil {
		return ErrScheduledNoSchedules
	}

	// Limits are optional
	if c.Limits != nil {
		if err := c.Limits.Validate(); err != nil {
			return fmt.Errorf("limits validation failed: %w", err)
		}
	}

	return nil
}

// validateIncremental validates configuration for incremental transformations
func (c *Config) validateIncremental() error {
	// Dependencies are required
	if len(c.Dependencies) == 0 {
		return ErrDependenciesRequired
	}

	// Interval required for incremental transformations
	if c.Interval == nil {
		return ErrIntervalRequired
	}

	if err := c.Interval.Validate(); err != nil {
		return fmt.Errorf("interval validation failed: %w", err)
	}

	// Schedules are required
	if c.Schedules == nil || (c.Schedules.ForwardFill == "" && c.Schedules.Backfill == "") {
		return ErrSchedulesRequired
	}

	if err := c.Schedules.Validate(); err != nil {
		return fmt.Errorf("schedules validation failed: %w", err)
	}

	// Schedule field should not be used for incremental
	if c.Schedule != "" {
		return ErrIncrementalNoSchedule
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

// GetType returns the transformation type
func (c *Config) GetType() Type {
	return c.Type
}

// IsScheduled returns true if this is a scheduled transformation
func (c *Config) IsScheduled() bool {
	return c.Type == TypeScheduled
}

// IsIncremental returns true if this is an incremental transformation
func (c *Config) IsIncremental() bool {
	return c.Type == TypeIncremental
}

// GetScheduleForType returns the appropriate schedule based on transformation type
func (c *Config) GetScheduleForType() string {
	if c.IsScheduled() {
		return c.Schedule
	}
	return "" // Incremental uses Schedules, not Schedule
}
