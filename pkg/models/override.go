package models

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"gopkg.in/yaml.v3"
)

var (
	// ErrUnknownModelType is returned when an unknown model type is encountered
	ErrUnknownModelType = errors.New("unknown model type")
)

// ModelType identifies whether a model is external or transformation
type ModelType string

const (
	// ModelTypeExternal identifies external models
	ModelTypeExternal ModelType = "external"
	// ModelTypeTransformation identifies transformation models
	ModelTypeTransformation ModelType = "transformation"
)

// OverrideConfig defines the interface for override configurations
type OverrideConfig interface {
	applyToTransformation(config *transformation.Config)
	applyToExternal(config *external.Config)
}

// ModelOverride represents configuration overrides for a specific model
// The actual config is stored as raw YAML and unmarshaled based on model type
type ModelOverride struct {
	Enabled   *bool
	rawConfig *yaml.Node
	Config    OverrideConfig
}

// UnmarshalYAML implements custom unmarshaling for ModelOverride
func (m *ModelOverride) UnmarshalYAML(node *yaml.Node) error {
	// First unmarshal enabled field
	var temp struct {
		Enabled *bool `yaml:"enabled,omitempty"`
	}

	if err := node.Decode(&temp); err != nil {
		return fmt.Errorf("failed to unmarshal override: %w", err)
	}

	m.Enabled = temp.Enabled

	// Iterate through node content to find config field
	// node.Content is a flat list of key-value pairs for maps
	for i := 0; i < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]

		if keyNode.Value == "config" {
			m.rawConfig = valueNode
			break
		}
	}

	return nil
}

// ResolveConfig unmarshals the raw config based on the actual model type
func (m *ModelOverride) ResolveConfig(actualType ModelType) error {
	if m.rawConfig == nil {
		return nil
	}

	switch actualType {
	case ModelTypeTransformation:
		var config TransformationOverride
		if err := m.rawConfig.Decode(&config); err != nil {
			return fmt.Errorf("failed to unmarshal transformation config: %w", err)
		}
		m.Config = &config

	case ModelTypeExternal:
		var config ExternalOverride
		if err := m.rawConfig.Decode(&config); err != nil {
			return fmt.Errorf("failed to unmarshal external config: %w", err)
		}
		m.Config = &config

	default:
		return fmt.Errorf("%w: %s", ErrUnknownModelType, actualType)
	}

	return nil
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
	ForwardFill *string `yaml:"forwardfill"`
	Backfill    *string `yaml:"backfill"`
}

// LimitsOverride allows overriding position limits
type LimitsOverride struct {
	Min *uint64 `yaml:"min,omitempty"`
	Max *uint64 `yaml:"max,omitempty"`
}

// ApplyToTransformation applies override configuration to a transformation model
func (m *ModelOverride) ApplyToTransformation(config *transformation.Config) {
	if m == nil || m.Config == nil {
		return
	}

	m.Config.applyToTransformation(config)
}

// IsDisabled returns true if the model is explicitly disabled
func (m *ModelOverride) IsDisabled() bool {
	return m != nil && m.Enabled != nil && !*m.Enabled
}

// TransformationOverride implements OverrideConfig for transformation models
func (t *TransformationOverride) applyToTransformation(config *transformation.Config) {
	t.applyIntervalOverrides(config)
	t.applyScheduleOverrides(config)
	t.applyLimitOverrides(config)
	t.applyTagOverrides(config)
}

func (t *TransformationOverride) applyToExternal(_ *external.Config) {
	// No-op: transformation overrides don't apply to external models
}

func (t *TransformationOverride) applyIntervalOverrides(config *transformation.Config) {
	if t.Interval == nil || config.Interval == nil {
		return
	}

	if t.Interval.Max != nil {
		config.Interval.Max = *t.Interval.Max
	}
	if t.Interval.Min != nil {
		config.Interval.Min = *t.Interval.Min
	}
}

func (t *TransformationOverride) applyScheduleOverrides(config *transformation.Config) {
	if t.Schedules == nil || config.Schedules == nil {
		return
	}

	if t.Schedules.ForwardFill != nil {
		config.Schedules.ForwardFill = *t.Schedules.ForwardFill
	}
	if t.Schedules.Backfill != nil {
		config.Schedules.Backfill = *t.Schedules.Backfill
	}
}

func (t *TransformationOverride) applyLimitOverrides(config *transformation.Config) {
	if t.Limits == nil {
		return
	}

	// Initialize limits if not already present
	if config.Limits == nil {
		config.Limits = &transformation.LimitsConfig{}
	}

	if t.Limits.Min != nil {
		config.Limits.Min = *t.Limits.Min
	}
	if t.Limits.Max != nil {
		config.Limits.Max = *t.Limits.Max
	}
}

func (t *TransformationOverride) applyTagOverrides(config *transformation.Config) {
	if len(t.Tags) > 0 {
		config.Tags = append(config.Tags, t.Tags...)
	}
}

// ExternalOverride contains override values for external model configurations
// All fields are optional - nil means keep existing value
type ExternalOverride struct {
	Lag   *uint64        `yaml:"lag,omitempty"`
	Cache *CacheOverride `yaml:"cache,omitempty"`
}

// CacheOverride allows overriding cache configuration
type CacheOverride struct {
	IncrementalScanInterval *time.Duration `yaml:"incremental_scan_interval,omitempty"`
	FullScanInterval        *time.Duration `yaml:"full_scan_interval,omitempty"`
}

// ExternalOverride implements OverrideConfig for external models
func (e *ExternalOverride) applyToTransformation(_ *transformation.Config) {
	// No-op: external overrides don't apply to transformation models
}

func (e *ExternalOverride) applyToExternal(config *external.Config) {
	e.applyLagOverride(config)
	e.applyCacheOverride(config)
}

func (e *ExternalOverride) applyLagOverride(config *external.Config) {
	if e.Lag != nil {
		config.Lag = *e.Lag
	}
}

func (e *ExternalOverride) applyCacheOverride(config *external.Config) {
	if e.Cache == nil || config.Cache == nil {
		return
	}

	if e.Cache.IncrementalScanInterval != nil {
		config.Cache.IncrementalScanInterval = *e.Cache.IncrementalScanInterval
	}

	if e.Cache.FullScanInterval != nil {
		config.Cache.FullScanInterval = *e.Cache.FullScanInterval
	}
}

// ApplyToExternal applies override configuration to an external model
func (m *ModelOverride) ApplyToExternal(config *external.Config) {
	if m == nil || m.Config == nil {
		return
	}

	m.Config.applyToExternal(config)
}
