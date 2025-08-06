// Package redis provides Redis client configuration
package redis

import (
	"errors"
	"fmt"
)

// Define static errors
var (
	ErrAddressRequired = errors.New("redis address is required")
)

// Config holds Redis client configuration
type Config struct {
	Address string `yaml:"address"`
	Prefix  string `yaml:"prefix"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Address == "" {
		return ErrAddressRequired
	}

	if c.Prefix == "" {
		c.Prefix = "cbt"
	}

	return nil
}

// PrefixKey adds the configured prefix to a Redis key
func (c *Config) PrefixKey(key string) string {
	if c.Prefix == "" {
		return key
	}

	return fmt.Sprintf("%s:%s", c.Prefix, key)
}

// PrefixQueue adds the configured prefix to an Asynq queue name
func (c *Config) PrefixQueue(queue string) string {
	if c.Prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", c.Prefix, queue)
}
