package admin

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type CacheExternal struct {
	ModelID   string        `json:"model_id"`
	Min       uint64        `json:"min"`
	Max       uint64        `json:"max"`
	UpdatedAt time.Time     `json:"updated_at"`
	TTL       time.Duration `json:"ttl"`
}

type CacheManager struct {
	redisClient *redis.Client
	keyPrefix   string
}

func NewCacheManager(redisClient *redis.Client) *CacheManager {
	return &CacheManager{
		redisClient: redisClient,
		keyPrefix:   "cbt:external:",
	}
}

func (c *CacheManager) GetExternal(ctx context.Context, modelID string) (*CacheExternal, error) {
	key := c.keyPrefix + modelID

	data, err := c.redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // Cache miss
		}
		return nil, err
	}

	var cache CacheExternal
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

func (c *CacheManager) SetExternal(ctx context.Context, cache CacheExternal) error {
	key := c.keyPrefix + cache.ModelID

	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}

	return c.redisClient.Set(ctx, key, data, cache.TTL).Err()
}

func (c *CacheManager) InvalidateExternal(ctx context.Context, modelID string) error {
	key := c.keyPrefix + modelID
	return c.redisClient.Del(ctx, key).Err()
}
