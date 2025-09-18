package models

import (
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// ModelOverride represents configuration overrides for a specific model
type ModelOverride struct {
	Enabled *bool                   `yaml:"enabled,omitempty"`
	Config  *TransformationOverride `yaml:"config,omitempty"`
}

// TransformationOverride contains override values for transformation model configurations
// All fields are optional - nil means keep existing value
type TransformationOverride struct {
	Interval  *IntervalOverride  `yaml:"interval,omitempty"`
	Schedules *SchedulesOverride `yaml:"schedules,omitempty"`
	Limits    *LimitsOverride    `yaml:"limits,omitempty"`
	Tags      []string           `yaml:"tags,omitempty"`
}

// IntervalOverride allows overriding interval configuration
type IntervalOverride struct {
	Max *uint64 `yaml:"max,omitempty"`
	Min *uint64 `yaml:"min,omitempty"`
}

// SchedulesOverride allows overriding schedule configuration
// nil = keep existing, empty string = disable, non-empty = new schedule
type SchedulesOverride struct {
	ForwardFill *string `yaml:"forwardfill,omitempty"`
	Backfill    *string `yaml:"backfill,omitempty"`
}

// LimitsOverride allows overriding position limits
type LimitsOverride struct {
	Min *uint64 `yaml:"min,omitempty"`
	Max *uint64 `yaml:"max,omitempty"`
}

// ApplyToTransformation applies override configuration to a transformation model
// TODO: This needs to be refactored to work with the new incremental/scheduled split
func (o *ModelOverride) ApplyToTransformation(_ *transformation.Config) {
	// Overrides temporarily disabled during refactor
}

// TODO: These override methods need to be refactored to work with incremental/scheduled configs
// For now, they're commented out to avoid compilation errors

/*
func (o *ModelOverride) applyIntervalOverrides(config *transformation.Config) {
	// Needs refactoring for new config types
}

func (o *ModelOverride) applyScheduleOverrides(config *transformation.Config) {
	// Needs refactoring for new config types
}

func (o *ModelOverride) applyLimitOverrides(config *transformation.Config) {
	// Needs refactoring for new config types
}

func (o *ModelOverride) applyTagOverrides(config *transformation.Config) {
	// Needs refactoring for new config types
}
*/

// IsDisabled returns true if the model is explicitly disabled
func (o *ModelOverride) IsDisabled() bool {
	return o != nil && o.Enabled != nil && !*o.Enabled
}
