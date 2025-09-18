package models

import (
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/ethpandaops/cbt/pkg/models/transformation/scheduled"
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
func (o *ModelOverride) ApplyToTransformation(model Transformation) {
	if o == nil || o.Config == nil {
		return
	}

	handler := model.GetHandler()
	if handler == nil {
		return
	}

	// Handle incremental type
	if handler.Type() == transformation.TypeIncremental {
		if cfg, ok := handler.Config().(*incremental.Config); ok {
			o.applyIncrementalOverrides(cfg)
		}
	}

	// Handle scheduled type
	if handler.Type() == transformation.TypeScheduled {
		if cfg, ok := handler.Config().(*scheduled.Config); ok {
			o.applyScheduledOverrides(cfg)
		}
	}
}

// applyIncrementalOverrides applies overrides to incremental transformation config
func (o *ModelOverride) applyIncrementalOverrides(config *incremental.Config) {
	if o.Config.Interval != nil && config.Interval != nil {
		if o.Config.Interval.Max != nil {
			config.Interval.Max = *o.Config.Interval.Max
		}
		if o.Config.Interval.Min != nil {
			config.Interval.Min = *o.Config.Interval.Min
		}
	}

	if o.Config.Schedules != nil && config.Schedules != nil {
		if o.Config.Schedules.ForwardFill != nil {
			config.Schedules.ForwardFill = *o.Config.Schedules.ForwardFill
		}
		if o.Config.Schedules.Backfill != nil {
			config.Schedules.Backfill = *o.Config.Schedules.Backfill
		}
	}

	if o.Config.Limits != nil {
		if config.Limits == nil {
			config.Limits = &incremental.LimitsConfig{}
		}
		if o.Config.Limits.Min != nil {
			config.Limits.Min = *o.Config.Limits.Min
		}
		if o.Config.Limits.Max != nil {
			config.Limits.Max = *o.Config.Limits.Max
		}
	}

	if len(o.Config.Tags) > 0 {
		config.Tags = append(config.Tags, o.Config.Tags...)
	}
}

// applyScheduledOverrides applies overrides to scheduled transformation config
func (o *ModelOverride) applyScheduledOverrides(config *scheduled.Config) {
	// Scheduled transformations only support tags override
	if len(o.Config.Tags) > 0 {
		config.Tags = append(config.Tags, o.Config.Tags...)
	}
}

// IsDisabled returns true if the model is explicitly disabled
func (o *ModelOverride) IsDisabled() bool {
	return o != nil && o.Enabled != nil && !*o.Enabled
}
