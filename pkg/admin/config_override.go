package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// configOverrideKeyPrefix is the Redis key prefix for config overrides.
	configOverrideKeyPrefix = "cbt:config-override:"
	// configOverrideVersionKey tracks the version counter for change detection.
	configOverrideVersionKey = "cbt:config-override:version"
)

// ConfigOverride represents a live configuration override stored in Redis.
type ConfigOverride struct {
	ModelID   string          `json:"model_id"`
	ModelType string          `json:"model_type"` // "transformation" or "external"
	Enabled   *bool           `json:"enabled,omitempty"`
	Override  json.RawMessage `json:"override"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// GetConfigOverride retrieves a config override for a specific model.
func (c *CacheManager) GetConfigOverride(ctx context.Context, modelID string) (*ConfigOverride, error) {
	key := configOverrideKeyPrefix + modelID

	data, err := c.redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get config override: %w", err)
	}

	var override ConfigOverride
	if err := json.Unmarshal([]byte(data), &override); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config override: %w", err)
	}

	return &override, nil
}

// GetAllConfigOverrides retrieves all config overrides using SCAN.
func (c *CacheManager) GetAllConfigOverrides(ctx context.Context) ([]ConfigOverride, error) {
	pattern := configOverrideKeyPrefix + "*"
	overrides := make([]ConfigOverride, 0, 16)

	var cursor uint64

	for {
		keys, nextCursor, err := c.redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan config overrides: %w", err)
		}

		for _, key := range keys {
			// Skip the version key
			if key == configOverrideVersionKey {
				continue
			}

			data, err := c.redisClient.Get(ctx, key).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}

				return nil, fmt.Errorf("failed to get config override %s: %w", key, err)
			}

			var override ConfigOverride
			if err := json.Unmarshal([]byte(data), &override); err != nil {
				return nil, fmt.Errorf("failed to unmarshal config override %s: %w", key, err)
			}

			overrides = append(overrides, override)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return overrides, nil
}

// SetConfigOverride stores a config override and increments the version counter.
func (c *CacheManager) SetConfigOverride(ctx context.Context, override *ConfigOverride) error {
	key := configOverrideKeyPrefix + override.ModelID

	data, err := json.Marshal(override)
	if err != nil {
		return fmt.Errorf("failed to marshal config override: %w", err)
	}

	pipe := c.redisClient.Pipeline()
	pipe.Set(ctx, key, data, 0)
	pipe.Incr(ctx, configOverrideVersionKey)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to set config override: %w", err)
	}

	return nil
}

// DeleteConfigOverride removes a config override and increments the version counter.
func (c *CacheManager) DeleteConfigOverride(ctx context.Context, modelID string) error {
	key := configOverrideKeyPrefix + modelID

	pipe := c.redisClient.Pipeline()
	pipe.Del(ctx, key)
	pipe.Incr(ctx, configOverrideVersionKey)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to delete config override: %w", err)
	}

	return nil
}

// DeleteAllConfigOverrides removes all config overrides and increments the version counter.
func (c *CacheManager) DeleteAllConfigOverrides(ctx context.Context) error {
	pattern := configOverrideKeyPrefix + "*"
	var cursor uint64

	keysToDelete := make([]string, 0, 16)

	for {
		keys, nextCursor, err := c.redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("failed to scan config overrides for deletion: %w", err)
		}

		for _, key := range keys {
			if key == configOverrideVersionKey {
				continue
			}

			keysToDelete = append(keysToDelete, key)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if len(keysToDelete) == 0 {
		return nil
	}

	pipe := c.redisClient.Pipeline()
	pipe.Del(ctx, keysToDelete...)
	pipe.Incr(ctx, configOverrideVersionKey)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to delete all config overrides: %w", err)
	}

	return nil
}

// GetConfigOverrideVersion returns the current version counter for change detection.
func (c *CacheManager) GetConfigOverrideVersion(ctx context.Context) (int64, error) {
	val, err := c.redisClient.Get(ctx, configOverrideVersionKey).Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to get config override version: %w", err)
	}

	return val, nil
}
