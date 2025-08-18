package models

import "time"

// ModelTags defines tag-based filtering for model selection
type ModelTags struct {
	Include []string `yaml:"include,omitempty"` // Tags to include (OR logic)
	Exclude []string `yaml:"exclude,omitempty"` // Tags to exclude (AND logic)
	Require []string `yaml:"require,omitempty"` // Tags that must ALL be present (AND logic)
}

// ModelConfig defines the configuration for a data model
type ModelConfig struct {
	Database     string        `yaml:"database" validate:"required"`
	Table        string        `yaml:"table" validate:"required"`
	Partition    string        `yaml:"partition" validate:"required"`
	External     bool          `yaml:"external"`
	TTL          time.Duration `yaml:"ttl"`
	Interval     uint64        `yaml:"interval"`
	Schedule     string        `yaml:"schedule"`
	Backfill     bool          `yaml:"backfill"`
	Lookback     uint64        `yaml:"lookback"` // Number of previous intervals to reprocess
	Dependencies []string      `yaml:"dependencies"`
	Tags         []string      `yaml:"tags"` // Tags for worker filtering
	Exec         string        `yaml:"exec"`
	Content      string        `yaml:"-"` // SQL content or file path for exec

	// Inherited lookback fields (calculated at runtime)
	InheritedLookback uint64            `yaml:"-"` // Calculated lookback from dependencies
	LookbackSources   map[string]uint64 `yaml:"-"` // External model -> lookback value
	LookbackReason    string            `yaml:"-"` // Human-readable explanation
}

// GetEffectiveLookback returns the lookback value to use for this model
func (m *ModelConfig) GetEffectiveLookback() uint64 {
	if m.External {
		return m.Lookback // External models use configured value
	}
	return m.InheritedLookback // Transformation models use inherited value
}

// HasLookback returns true if model has any lookback (configured or inherited)
func (m *ModelConfig) HasLookback() bool {
	return m.GetEffectiveLookback() > 0
}
