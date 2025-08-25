// Package admin provides administration and caching services for CBT
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// BoundsCache represents cached external model bounds
type BoundsCache struct {
	ModelID string `json:"model_id"`
	Min     uint64 `json:"min"`
	Max     uint64 `json:"max"`

	// Track scan times
	LastIncrementalScan time.Time `json:"last_incremental_scan"`
	LastFullScan        time.Time `json:"last_full_scan"`

	// For optimization hints
	PreviousMin uint64 `json:"previous_min"`
	PreviousMax uint64 `json:"previous_max"`

	// Metadata
	UpdatedAt time.Time `json:"updated_at"`
}

// CacheManager manages Redis-based caching for external models
type CacheManager struct {
	redisClient *redis.Client
	keyPrefix   string
}

// NewCacheManager creates a new cache manager instance
func NewCacheManager(redisClient *redis.Client) *CacheManager {
	return &CacheManager{
		redisClient: redisClient,
		keyPrefix:   "cbt:external:",
	}
}

// GetBounds retrieves cached external model bounds from Redis
func (c *CacheManager) GetBounds(ctx context.Context, modelID string) (*BoundsCache, error) {
	key := c.keyPrefix + modelID

	data, err := c.redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // Cache miss
		}
		return nil, err
	}

	var cache BoundsCache
	if err := json.Unmarshal([]byte(data), &cache); err != nil {
		return nil, err
	}

	// No expiration - cache entries persist indefinitely
	return &cache, nil
}

// SetBounds stores external model bounds in Redis cache (no TTL)
func (c *CacheManager) SetBounds(ctx context.Context, cache *BoundsCache) error {
	key := c.keyPrefix + cache.ModelID

	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	// Store without TTL - persistent cache
	return c.redisClient.Set(ctx, key, data, 0).Err()
}
