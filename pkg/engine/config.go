package engine

import (
	"errors"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/scheduler"
	"github.com/ethpandaops/cbt/pkg/worker"
)

var (
	// ErrRedisURLRequired is returned when Redis URL is not provided
	ErrRedisURLRequired = errors.New("redis URL is required")
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
}

// RedisConfig represents Redis connection configuration
type RedisConfig struct {
	URL string `yaml:"url" validate:"required,url"`
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

	return nil
}
