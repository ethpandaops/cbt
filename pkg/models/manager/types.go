// Package manager provides functionality for managing and inspecting models
package manager

import (
	"encoding/json"
)

// Constants for model types
const (
	ModelTypeTransformation = "transformation"
	ModelTypeExternal       = "external"
)

// ModelInfo represents basic model information for listing
type ModelInfo struct {
	ID       string
	Type     string
	Schedule string
	Interval string
	Lag      string
	Deps     string
	Status   string
}

// ModelStatus represents detailed model status from admin table
type ModelStatus struct {
	Database      string `json:"database"`
	Table         string `json:"table"`
	FirstPosition uint64 `json:"first_position,string"`
	LastPosition  uint64 `json:"last_position,string"`
	NextPosition  uint64 `json:"next_position,string"`
	LastRun       string `json:"last_run"`
	TotalRuns     uint64 `json:"total_runs,string"`
}

// GapInfo represents gap information for a model
type GapInfo struct {
	GapCount      int
	OldestGap     uint64
	Coverage      float64
	TotalExpected int
}

// IntervalResult represents query results for interval analysis
type IntervalResult struct {
	TotalIntervals uint64        `json:"total_intervals,string"`
	MinPos         uint64        `json:"min_pos,string"`
	MaxPos         uint64        `json:"max_pos,string"`
	Positions      []json.Number `json:"positions"`
	IntervalSize   uint64        `json:"interval_size,string"`
}

// ValidationResult contains validation results
type ValidationResult struct {
	ValidCount   int
	ErrorCount   int
	ModelConfigs map[string]interface{}
}
