package redis

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// New creates a new Redis client from configuration
func New(config *Config) (*redis.Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid redis config: %w", err)
	}

	options, err := redis.ParseURL(config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}

	return redis.NewClient(options), nil
}
