// Package engine provides the core CBT engine service
package engine

import (
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/api"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/frontend"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/scheduler"
	"github.com/ethpandaops/cbt/pkg/worker"
)

var (
	// ErrRedisURLRequired is returned when Redis URL is not provided
	ErrRedisURLRequired = errors.New("redis URL is required")
	// ErrIntervalTypeEmpty is returned when an interval type has no transformations
	ErrIntervalTypeEmpty = errors.New("interval type must have at least one transformation")
	// ErrTransformationNameRequired is returned when a transformation has no name
	ErrTransformationNameRequired = errors.New("transformation name is required")
	// ErrInvalidTransformationFormat is returned when a transformation has an invalid format
	ErrInvalidTransformationFormat = errors.New("invalid transformation format")
)

// Config represents the complete engine configuration
type Config struct {
	// Core settings
	Logging         string `yaml:"logging" default:"info" validate:"oneof=panic fatal warn info debug trace"`
	MetricsAddr     string `yaml:"metricsAddr" default:":9091"`
	HealthCheckAddr string `yaml:"healthCheckAddr"`
	PProfAddr       string `yaml:"pprofAddr"`

	// Dependencies
	ClickHouse clickhouse.Config `yaml:"clickhouse"`
	Redis      RedisConfig       `yaml:"redis"`

	// Coordinator specific
	Scheduler scheduler.Config `yaml:"scheduler"`

	// Worker specific settings
	Worker worker.Config `yaml:"worker"`

	// Models configuration
	Models models.Config `yaml:"models"`

	// API service configuration
	API api.Config `yaml:"api"`

	// Frontend service configuration
	Frontend frontend.Config `yaml:"frontend"`

	// Interval type transformations for API exposure
	IntervalTypes IntervalTypesConfig `yaml:"interval_types"`
}

// RedisConfig represents Redis connection configuration
type RedisConfig struct {
	URL string `yaml:"url" validate:"required,url"`
}

// IntervalTypeTransformation represents a single transformation step for an interval type
type IntervalTypeTransformation struct {
	Name       string `yaml:"name" json:"name"`                                 // Display name (e.g., "timestamp", "epoch")
	Expression string `yaml:"expression,omitempty" json:"expression,omitempty"` // Optional CEL expression (e.g., "math.floor(value / 12)")
	Format     string `yaml:"format,omitempty" json:"format,omitempty"`         // Optional display format hint (e.g., "datetime", "date", "time")
}

// IntervalTypesConfig maps interval type names to their transformation pipelines
type IntervalTypesConfig map[string][]IntervalTypeTransformation

// Validate validates the interval types configuration
func (c IntervalTypesConfig) Validate() error {
	validFormats := map[string]bool{
		"datetime": true,
		"date":     true,
		"time":     true,
		"duration": true,
		"relative": true,
	}

	for typeName, transformations := range c {
		if len(transformations) == 0 {
			return fmt.Errorf("%w: %q", ErrIntervalTypeEmpty, typeName)
		}

		for i, t := range transformations {
			if t.Name == "" {
				return fmt.Errorf("%w for interval type %q transformation %d", ErrTransformationNameRequired, typeName, i)
			}

			// Validate format if provided
			if t.Format != "" && !validFormats[t.Format] {
				return fmt.Errorf("%w: interval type %q transformation %q has format %q, must be one of: datetime, date, time, duration, relative", ErrInvalidTransformationFormat, typeName, t.Name, t.Format)
			}
		}
	}
	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Redis.URL == "" {
		return ErrRedisURLRequired
	}

	if err := c.ClickHouse.Validate(); err != nil {
		return err
	}

	if err := c.Scheduler.Validate(); err != nil {
		return err
	}

	if err := c.Worker.Validate(); err != nil {
		return err
	}

	if err := c.Models.Validate(); err != nil {
		return err
	}

	if err := c.API.Validate(); err != nil {
		return err
	}

	if err := c.Frontend.Validate(); err != nil {
		return err
	}

	return c.IntervalTypes.Validate()
}
