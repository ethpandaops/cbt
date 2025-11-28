package transformation

import (
	"errors"
	"fmt"
	"reflect"

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

// ApplyTagsOverride appends override tags to existing tags using reflection.
// This is shared between incremental and scheduled transformation handlers.
func ApplyTagsOverride(existingTags []string, v reflect.Value) []string {
	tagsField := v.FieldByName("Tags")
	if !tagsField.IsValid() || tagsField.Len() == 0 {
		return existingTags
	}

	result := existingTags
	for i := 0; i < tagsField.Len(); i++ {
		tag := tagsField.Index(i).String()
		result = append(result, tag)
	}

	return result
}

// ApplyScheduleOverride extracts a schedule override from a reflect.Value and returns
// the new schedule if present, or the existing schedule if not.
// This is shared between transformation handlers that support schedule overrides.
func ApplyScheduleOverride(existingSchedule string, v reflect.Value) string {
	scheduleField := v.FieldByName("Schedule")
	if !scheduleField.IsValid() || scheduleField.IsNil() {
		return existingSchedule
	}

	return scheduleField.Elem().String()
}

// ApplyMinMaxOverride extracts Min/Max uint64 overrides from a named nested struct field.
// Returns the updated min and max values, and whether any override was found.
// Used for Interval and Limits overrides in incremental transformations.
func ApplyMinMaxOverride(fieldName string, existingMin, existingMax uint64, v reflect.Value) (minVal, maxVal uint64, found bool) {
	field := v.FieldByName(fieldName)
	if !field.IsValid() || field.IsNil() {
		return existingMin, existingMax, false
	}

	minVal, maxVal = existingMin, existingMax
	fieldVal := field.Elem()

	if minField := fieldVal.FieldByName("Min"); minField.IsValid() && !minField.IsNil() {
		minVal = minField.Elem().Uint()
		found = true
	}

	if maxField := fieldVal.FieldByName("Max"); maxField.IsValid() && !maxField.IsNil() {
		maxVal = maxField.Elem().Uint()
		found = true
	}

	return minVal, maxVal, found
}

// ApplySchedulesOverride extracts ForwardFill/Backfill schedule overrides from a reflect.Value.
// Returns the updated forwardFill and backfill values.
// Used for incremental transformation schedule overrides.
func ApplySchedulesOverride(existingForwardFill, existingBackfill string, v reflect.Value) (forwardFill, backfill string) {
	schedulesField := v.FieldByName("Schedules")
	if !schedulesField.IsValid() || schedulesField.IsNil() {
		return existingForwardFill, existingBackfill
	}

	forwardFill, backfill = existingForwardFill, existingBackfill
	schedulesVal := schedulesField.Elem()

	if ffField := schedulesVal.FieldByName("ForwardFill"); ffField.IsValid() && !ffField.IsNil() {
		forwardFill = ffField.Elem().String()
	}

	if bfField := schedulesVal.FieldByName("Backfill"); bfField.IsValid() && !bfField.IsNil() {
		backfill = bfField.Elem().String()
	}

	return forwardFill, backfill
}
