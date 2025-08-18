package worker

import (
	"errors"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
)

var (
	// ErrRedisURLRequired is returned when Redis URL is not provided
	ErrRedisURLRequired = errors.New("redis URL is required")
	// ErrClickHouseURLRequired is returned when ClickHouse URL is not provided
	ErrClickHouseURLRequired = errors.New("clickhouse URL is required")
	// ErrNoQueuesConfigured is returned when no queues are configured
	ErrNoQueuesConfigured = errors.New("at least one queue must be configured")
	// ErrInvalidConcurrency is returned when concurrency is not positive
	ErrInvalidConcurrency = errors.New("concurrency must be positive")
)

// Config represents the complete worker configuration
type Config struct {
	// Core settings
	Logging         string `yaml:"logging" default:"info" validate:"oneof=panic fatal warn info debug trace"`
	MetricsAddr     string `yaml:"metricsAddr" default:":9091"`
	HealthCheckAddr string `yaml:"healthCheckAddr"`
	PProfAddr       string `yaml:"pprofAddr"`

	// Dependencies
	ClickHouse clickhouse.Config `yaml:"clickhouse"`
	Redis      RedisConfig       `yaml:"redis"`

	// Worker specific settings
	Worker Settings `yaml:"worker"`

	// Models configuration
	Models models.PathConfig `yaml:"models"`
}

// RedisConfig represents Redis connection configuration
type RedisConfig struct {
	URL string `yaml:"url" validate:"required,url"`
}

// Settings contains worker-specific settings
type Settings struct {
	Queues          []string      `yaml:"queues" default:"[\"default\"]"`
	Concurrency     int           `yaml:"concurrency" default:"10"`
	ModelTags       *ModelTags    `yaml:"modelTags,omitempty"`          // Optional tag-based model filtering
	Advanced        []QueueConfig `yaml:"advanced,omitempty"`           // Advanced queue configuration
	ShutdownTimeout int           `yaml:"shutdownTimeout" default:"30"` // Seconds to wait for graceful shutdown
}

// ModelTags defines tag-based filtering for worker model selection
type ModelTags struct {
	Include []string `yaml:"include,omitempty"` // Tags to include (OR logic)
	Exclude []string `yaml:"exclude,omitempty"` // Tags to exclude (AND logic)
	Require []string `yaml:"require,omitempty"` // Tags that must ALL be present (AND logic)
}

// QueueConfig defines advanced queue configuration with pattern matching
type QueueConfig struct {
	Pattern     string `yaml:"pattern" validate:"required"`
	Concurrency int    `yaml:"concurrency" validate:"min=1"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Redis.URL == "" {
		return ErrRedisURLRequired
	}

	if c.ClickHouse.URL == "" {
		return ErrClickHouseURLRequired
	}

	if len(c.Worker.Queues) == 0 {
		return ErrNoQueuesConfigured
	}

	if c.Worker.Concurrency <= 0 {
		return ErrInvalidConcurrency
	}

	// Validate advanced queue configs
	for _, adv := range c.Worker.Advanced {
		if adv.Concurrency <= 0 {
			return ErrInvalidConcurrency
		}
	}

	// Set model path defaults
	c.Models.SetDefaults()

	return nil
}
