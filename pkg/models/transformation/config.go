package transformation

import (
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
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
	// ErrUnknownType is returned for unknown transformation types
	ErrUnknownType = errors.New("unknown transformation type")
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
		return fmt.Errorf("%w: %s", ErrUnknownType, c.Type)
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
	return modelid.Format(c.Database, c.Table)
}

// IsScheduledType returns true if this is a scheduled transformation
func (c *Config) IsScheduledType() bool {
	return c.Type == TypeScheduled
}

// IsIncrementalType returns true if this is an incremental transformation
func (c *Config) IsIncrementalType() bool {
	return c.Type == TypeIncremental
}

// ApplyTagsOverride appends override tags to the existing tags.
// This is shared between incremental and scheduled transformation handlers.
func ApplyTagsOverride(existingTags, overrideTags []string) []string {
	if len(overrideTags) == 0 {
		return existingTags
	}

	result := existingTags
	result = append(result, overrideTags...)

	return result
}

// ApplyScheduleOverride returns the override schedule if present, otherwise the
// existing schedule. This is shared between transformation handlers that
// support schedule overrides.
func ApplyScheduleOverride(existingSchedule string, override *string) string {
	if override == nil {
		return existingSchedule
	}

	return *override
}

// ApplyMinMaxOverride applies an optional Min/Max uint64 override.
// Returns the updated min and max values, and whether any override was found.
// Used for Interval and Limits overrides in incremental transformations.
func ApplyMinMaxOverride(override *LimitsOverride, existingMin, existingMax uint64) (minVal, maxVal uint64, found bool) {
	if override == nil {
		return existingMin, existingMax, false
	}

	minVal, maxVal = existingMin, existingMax

	if override.Min != nil {
		minVal = *override.Min
		found = true
	}

	if override.Max != nil {
		maxVal = *override.Max
		found = true
	}

	return minVal, maxVal, found
}

// ApplyIntervalOverride applies an optional Min/Max uint64 interval override.
// Returns the updated min and max values, and whether any override was found.
func ApplyIntervalOverride(override *IntervalOverride, existingMin, existingMax uint64) (minVal, maxVal uint64, found bool) {
	if override == nil {
		return existingMin, existingMax, false
	}

	return ApplyMinMaxOverride(
		&LimitsOverride{Min: override.Min, Max: override.Max},
		existingMin, existingMax,
	)
}

// ApplySchedulesOverride applies optional ForwardFill/Backfill schedule overrides.
// Returns the updated forwardFill and backfill values.
// Used for incremental transformation schedule overrides.
func ApplySchedulesOverride(existingForwardFill, existingBackfill string, override *SchedulesOverride) (forwardFill, backfill string) {
	if override == nil {
		return existingForwardFill, existingBackfill
	}

	forwardFill, backfill = existingForwardFill, existingBackfill

	if override.ForwardFill != nil {
		forwardFill = *override.ForwardFill
	}

	if override.Backfill != nil {
		backfill = *override.Backfill
	}

	return forwardFill, backfill
}
