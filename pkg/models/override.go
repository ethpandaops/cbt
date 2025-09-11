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
func (o *ModelOverride) ApplyToTransformation(config *transformation.Config) {
	if o == nil || o.Config == nil {
		return
	}

	o.applyIntervalOverrides(config)
	o.applyScheduleOverrides(config)
	o.applyLimitOverrides(config)
	o.applyTagOverrides(config)
}

// applyIntervalOverrides applies interval configuration overrides
func (o *ModelOverride) applyIntervalOverrides(config *transformation.Config) {
	if o.Config.Interval == nil || config.Interval == nil {
		return
	}

	if o.Config.Interval.Max != nil {
		config.Interval.Max = *o.Config.Interval.Max
	}
	if o.Config.Interval.Min != nil {
		config.Interval.Min = *o.Config.Interval.Min
	}
}

// applyScheduleOverrides applies schedule configuration overrides
func (o *ModelOverride) applyScheduleOverrides(config *transformation.Config) {
	if o.Config.Schedules == nil || config.Schedules == nil {
		return
	}

	if o.Config.Schedules.ForwardFill != nil {
		config.Schedules.ForwardFill = *o.Config.Schedules.ForwardFill
	}
	if o.Config.Schedules.Backfill != nil {
		config.Schedules.Backfill = *o.Config.Schedules.Backfill
	}
}

// applyLimitOverrides applies limit configuration overrides
func (o *ModelOverride) applyLimitOverrides(config *transformation.Config) {
	if o.Config.Limits == nil {
		return
	}

	// Initialize limits if not already present
	if config.Limits == nil {
		config.Limits = &transformation.LimitsConfig{}
	}

	if o.Config.Limits.Min != nil {
		config.Limits.Min = *o.Config.Limits.Min
	}
	if o.Config.Limits.Max != nil {
		config.Limits.Max = *o.Config.Limits.Max
	}
}

// applyTagOverrides appends additional tags to the configuration
func (o *ModelOverride) applyTagOverrides(config *transformation.Config) {
	if len(o.Config.Tags) > 0 {
		config.Tags = append(config.Tags, o.Config.Tags...)
	}
}

// IsDisabled returns true if the model is explicitly disabled
func (o *ModelOverride) IsDisabled() bool {
	return o != nil && o.Enabled != nil && !*o.Enabled
}
