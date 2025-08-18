package models

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
)

// ExternalCacheManager manages cached external model min/max values
type ExternalCacheManager struct {
	redisClient *redis.Client
	keyPrefix   string
}

// NewExternalCacheManager creates a new external cache manager
func NewExternalCacheManager(redisClient *redis.Client) *ExternalCacheManager {
	return &ExternalCacheManager{
		redisClient: redisClient,
		keyPrefix:   "cbt:external:",
	}
}

// Get retrieves cached external model bounds
func (c *ExternalCacheManager) Get(ctx context.Context, modelID string) (*clickhouse.ExternalModelCache, error) {
	key := c.keyPrefix + modelID

	data, err := c.redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // Cache miss
		}
		return nil, err
	}

	var cache clickhouse.ExternalModelCache
	if err := json.Unmarshal([]byte(data), &cache); err != nil {
		return nil, err
	}

	// Check if expired
	if time.Since(cache.UpdatedAt) > cache.TTL {
		_ = c.redisClient.Del(ctx, key) // Async cleanup
		return nil, nil
	}

	return &cache, nil
}

// Set stores external model bounds in cache
func (c *ExternalCacheManager) Set(ctx context.Context, cache clickhouse.ExternalModelCache) error {
	key := c.keyPrefix + cache.ModelID

	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	return c.redisClient.Set(ctx, key, data, cache.TTL).Err()
}

// Invalidate removes a model from cache
func (c *ExternalCacheManager) Invalidate(ctx context.Context, modelID string) error {
	key := c.keyPrefix + modelID
	return c.redisClient.Del(ctx, key).Err()
}
