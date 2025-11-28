// Package external provides external model configuration and validation
package external

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
)

var (
	// ErrIntervalRequired is returned when interval configuration is missing
	ErrIntervalRequired = errors.New("interval configuration is required")
	// ErrDatabaseRequired is returned when database is not specified
	ErrDatabaseRequired = errors.New("database is required")
	// ErrTableRequired is returned when table is not specified
	ErrTableRequired = errors.New("table is required")
	// ErrCacheConfigRequired is returned when cache configuration is not specified
	ErrCacheConfigRequired = errors.New("cache configuration is required")
	// ErrInvalidIncrementalInterval is returned when incremental scan interval is invalid
	ErrInvalidIncrementalInterval = errors.New("incremental_scan_interval must be positive")
	// ErrInvalidFullInterval is returned when full scan interval is invalid
	ErrInvalidFullInterval = errors.New("full_scan_interval must be positive")
	// ErrInvalidIntervalOrder is returned when incremental interval is not less than full interval
	ErrInvalidIntervalOrder = errors.New("incremental_scan_interval must be less than full_scan_interval")
	// ErrIntervalTypeRequired is returned when interval.type is not specified
	ErrIntervalTypeRequired = errors.New("interval.type is required")
)

// CacheConfig defines caching configuration for external models
type CacheConfig struct {
	IncrementalScanInterval time.Duration `yaml:"incremental_scan_interval"`
	FullScanInterval        time.Duration `yaml:"full_scan_interval"`
}

// IntervalConfig defines interval configuration for external models
type IntervalConfig struct {
	Type string `yaml:"type" json:"type"` // Required: examples: "second", "slot", "epoch", "block"
}

// Config defines configuration for external models
type Config struct {
	Cluster  string          `yaml:"cluster"`  // Optional, can fall back to default
	Database string          `yaml:"database"` // Optional, can fall back to default
	Table    string          `yaml:"table" validate:"required"`
	Cache    *CacheConfig    `yaml:"cache"`
	Lag      uint64          `yaml:"lag"`
	Interval *IntervalConfig `yaml:"interval" json:"interval"`
}

// Validate checks if the external configuration is valid
func (c *Config) Validate() error {
	if c.Database == "" {
		return ErrDatabaseRequired
	}

	if c.Table == "" {
		return ErrTableRequired
	}

	if c.Interval == nil {
		return ErrIntervalRequired
	}

	if err := c.Interval.Validate(); err != nil {
		return fmt.Errorf("interval validation failed: %w", err)
	}

	if c.Cache == nil {
		return ErrCacheConfigRequired
	}

	// Validate cache intervals
	if c.Cache.IncrementalScanInterval <= 0 {
		return ErrInvalidIncrementalInterval
	}

	if c.Cache.FullScanInterval <= 0 {
		return ErrInvalidFullInterval
	}

	if c.Cache.IncrementalScanInterval >= c.Cache.FullScanInterval {
		return ErrInvalidIntervalOrder
	}

	return nil
}

// Validate checks if the interval configuration is valid
func (c IntervalConfig) Validate() error {
	if c.Type == "" {
		return ErrIntervalTypeRequired
	}

	return nil
}

// SetDefaults applies default values to the configuration
func (c *Config) SetDefaults(defaultCluster, defaultDatabase string) {
	if c.Cluster == "" && defaultCluster != "" {
		c.Cluster = defaultCluster
	}
	if c.Database == "" && defaultDatabase != "" {
		c.Database = defaultDatabase
	}
}

// GetID returns the unique identifier for the external model
func (c *Config) GetID() string {
	return modelid.Format(c.Database, c.Table)
}
