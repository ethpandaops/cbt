package models

import "time"

// ModelTags defines tag-based filtering for model selection
type ModelTags struct {
	Include []string `yaml:"include,omitempty"` // Tags to include (OR logic)
	Exclude []string `yaml:"exclude,omitempty"` // Tags to exclude (AND logic)
	Require []string `yaml:"require,omitempty"` // Tags that must ALL be present (AND logic)
}

// BackfillConfig defines the backfill configuration for a model
type BackfillConfig struct {
	Enabled  bool   `yaml:"enabled,omitempty"`  // defaults to false if omitted
	Schedule string `yaml:"schedule,omitempty"` // required if enabled=true (e.g., "@every 5m")
	Minimum  uint64 `yaml:"minimum,omitempty"`  // minimum position to backfill from, defaults to 0
}

// ModelConfig defines the configuration for a data model
type ModelConfig struct {
	Database     string          `yaml:"database" validate:"required"`
	Table        string          `yaml:"table" validate:"required"`
	Partition    string          `yaml:"partition" validate:"required"`
	External     bool            `yaml:"external"`
	TTL          time.Duration   `yaml:"ttl"`
	Lag          uint64          `yaml:"lag"` // Seconds to subtract from max (external models only)
	Interval     uint64          `yaml:"interval"`
	Schedule     string          `yaml:"schedule"`
	Backfill     *BackfillConfig `yaml:"backfill,omitempty"`
	Dependencies []string        `yaml:"dependencies"`
	Tags         []string        `yaml:"tags"` // Tags for worker filtering
	Exec         string          `yaml:"exec"`
	Content      string          `yaml:"-"` // SQL content or file path for exec
}
