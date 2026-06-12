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
	// ErrUnknownType is returned when an unknown model type is encountered
	ErrUnknownType = errors.New("unknown model type")
)

// Type identifies whether a model is external or transformation
type Type string

const (
	// TypeExternal identifies external models
	TypeExternal Type = "external"
	// TypeTransformation identifies transformation models
	TypeTransformation Type = "transformation"
)

// Override represents configuration overrides for a specific model
// The actual config is stored as raw YAML and unmarshaled based on model type.
// Config holds either a *TransformationOverride or a *ExternalOverride once resolved.
type Override struct {
	Enabled   *bool
	rawConfig *yaml.Node
	Config    any
}

// UnmarshalYAML implements custom unmarshaling for Override
func (m *Override) UnmarshalYAML(node *yaml.Node) error {
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
func (m *Override) ResolveConfig(actualType Type) error {
	if m.rawConfig == nil {
		return nil
	}

	switch actualType {
	case TypeTransformation:
		var config TransformationOverride
		if err := m.rawConfig.Decode(&config); err != nil {
			return fmt.Errorf("failed to unmarshal transformation config: %w", err)
		}
		m.Config = &config

	case TypeExternal:
		var config ExternalOverride
		if err := m.rawConfig.Decode(&config); err != nil {
			return fmt.Errorf("failed to unmarshal external config: %w", err)
		}
		m.Config = &config

	default:
		return fmt.Errorf("%w: %s", ErrUnknownType, actualType)
	}

	return nil
}

// TransformationOverride is an alias for the canonical transformation override
// type, which lives in the leaf transformation package so handlers can apply it
// directly without reflection or an import cycle.
type TransformationOverride = transformation.Override

// IntervalOverride is an alias for transformation.IntervalOverride.
type IntervalOverride = transformation.IntervalOverride

// SchedulesOverride is an alias for transformation.SchedulesOverride.
type SchedulesOverride = transformation.SchedulesOverride

// LimitsOverride is an alias for transformation.LimitsOverride.
type LimitsOverride = transformation.LimitsOverride

// transformationOverrideApplier is implemented by transformation handlers that
// accept a typed transformation override.
type transformationOverrideApplier interface {
	ApplyOverrides(override *TransformationOverride)
}

// ApplyToTransformation applies override configuration to a transformation model
func (m *Override) ApplyToTransformation(model Transformation) {
	if m == nil || m.Config == nil {
		return
	}

	tOv, ok := m.Config.(*TransformationOverride)
	if !ok {
		return
	}

	handler := model.GetHandler()
	if handler == nil {
		return
	}

	if applier, ok := handler.(transformationOverrideApplier); ok {
		applier.ApplyOverrides(tOv)
	}
}

// IsDisabled returns true if the model is explicitly disabled
func (m *Override) IsDisabled() bool {
	return m != nil && m.Enabled != nil && !*m.Enabled
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

// ApplyToExternalConfig applies override configuration to an external model config.
func (e *ExternalOverride) ApplyToExternalConfig(config *external.Config) {
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
func (m *Override) ApplyToExternal(config *external.Config) {
	if m == nil || m.Config == nil {
		return
	}

	if eOv, ok := m.Config.(*ExternalOverride); ok {
		eOv.ApplyToExternalConfig(config)
	}
}
