package coordinator

import (
	"errors"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
)

var (
	// ErrRedisURLRequired is returned when Redis URL is not provided
	ErrRedisURLRequired = errors.New("redis URL is required")
	// ErrClickHouseURLRequired is returned when ClickHouse URL is not provided
	ErrClickHouseURLRequired = errors.New("clickhouse URL is required")
	// ErrInvalidSchedulingInterval is returned when scheduling interval is not positive
	ErrInvalidSchedulingInterval = errors.New("scheduling interval must be positive")
	// ErrInvalidMaxConcurrentSchedules is returned when max concurrent schedules is not positive
	ErrInvalidMaxConcurrentSchedules = errors.New("max concurrent schedules must be positive")
)

// Config represents the complete coordinator configuration
type Config struct {
	// Core settings
	Logging         string `yaml:"logging" default:"info" validate:"oneof=panic fatal warn info debug trace"`
	MetricsAddr     string `yaml:"metricsAddr" default:":9090"`
	HealthCheckAddr string `yaml:"healthCheckAddr"`
	PProfAddr       string `yaml:"pprofAddr"`

	// Dependencies
	ClickHouse clickhouse.Config `yaml:"clickhouse"`
	Redis      RedisConfig       `yaml:"redis"`

	// Coordinator specific
	Scheduling SchedulingConfig `yaml:"scheduling"`
}

// RedisConfig represents Redis connection configuration
type RedisConfig struct {
	URL string `yaml:"url" validate:"required,url"`
}

// SchedulingConfig represents scheduling configuration
type SchedulingConfig struct {
	Interval               time.Duration `yaml:"interval" default:"1m"`
	MaxConcurrentSchedules int           `yaml:"maxConcurrentSchedules" default:"10"`
	BatchSize              int           `yaml:"batchSize" default:"100"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Redis.URL == "" {
		return ErrRedisURLRequired
	}

	if c.ClickHouse.URL == "" {
		return ErrClickHouseURLRequired
	}

	if c.Scheduling.Interval <= 0 {
		return ErrInvalidSchedulingInterval
	}

	if c.Scheduling.MaxConcurrentSchedules <= 0 {
		return ErrInvalidMaxConcurrentSchedules
	}

	return nil
}
