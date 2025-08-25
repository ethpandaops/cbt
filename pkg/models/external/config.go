// Package external provides external model configuration and validation
package external

import (
	"errors"
	"fmt"
	"time"
)

var (
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
)

// CacheConfig defines caching configuration for external models
type CacheConfig struct {
	IncrementalScanInterval time.Duration `yaml:"incremental_scan_interval"`
	FullScanInterval        time.Duration `yaml:"full_scan_interval"`
}

// Config defines configuration for external models
type Config struct {
	Database string       `yaml:"database" validate:"required"`
	Table    string       `yaml:"table" validate:"required"`
	Cache    *CacheConfig `yaml:"cache"`
	Lag      uint64       `yaml:"lag"`
}

// Validate checks if the external configuration is valid
func (c *Config) Validate() error {
	if c.Database == "" {
		return ErrDatabaseRequired
	}

	if c.Table == "" {
		return ErrTableRequired
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

// GetID returns the unique identifier for the external model
func (c *Config) GetID() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}
