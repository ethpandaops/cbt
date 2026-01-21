// Package admin provides administration and caching services for CBT
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// Lock configuration constants
const (
	// boundsLockTTL is the TTL for the distributed lock (short to recover from crashes)
	boundsLockTTL = 10 * time.Second
	// boundsLockMaxRetries is the maximum number of retries for lock acquisition (~30s with backoff)
	boundsLockMaxRetries = 300
)

// ErrLockTimeout is returned when lock acquisition times out
var ErrLockTimeout = errors.New("timeout acquiring bounds lock")

// BoundsLock represents a distributed lock for bounds updates
type BoundsLock interface {
	Unlock(ctx context.Context) error
}

// boundsLock wraps redsync.Mutex to implement the BoundsLock interface
type boundsLock struct {
	mutex *redsync.Mutex
}

// Unlock releases the distributed lock.
func (l *boundsLock) Unlock(ctx context.Context) error {
	_, err := l.mutex.UnlockContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	return nil
}

// exponentialBackoff returns a delay based on the number of attempts.
// Starts at 50ms and doubles up to a maximum of 500ms.
func exponentialBackoff(tries int) time.Duration {
	const (
		baseDelay = 50 * time.Millisecond
		maxDelay  = 500 * time.Millisecond
		maxShift  = 3 // Cap shift to prevent overflow (50ms * 2^3 = 400ms)
	)

	shift := tries
	if shift > maxShift {
		shift = maxShift
	}

	delay := baseDelay << shift
	if delay > maxDelay {
		return maxDelay
	}

	return delay
}

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

	// Initial scan tracking
	InitialScanComplete bool       `json:"initial_scan_complete"`
	InitialScanStarted  *time.Time `json:"initial_scan_started,omitempty"`

	// Metadata
	UpdatedAt time.Time `json:"updated_at"`
}

// CacheManager manages Redis-based caching for external models
type CacheManager struct {
	redisClient *redis.Client
	keyPrefix   string
	rs          *redsync.Redsync
}

// NewCacheManager creates a new cache manager instance
func NewCacheManager(redisClient *redis.Client) *CacheManager {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)

	return &CacheManager{
		redisClient: redisClient,
		keyPrefix:   "cbt:external:",
		rs:          rs,
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

// AcquireLock acquires a distributed lock for bounds updates on a specific model.
// The lock uses redsync (Redlock algorithm) with exponential backoff for retries.
// Returns a BoundsLock that must be released when done.
func (c *CacheManager) AcquireLock(ctx context.Context, modelID string) (BoundsLock, error) {
	lockKey := c.keyPrefix + modelID + ":lock"
	mutex := c.rs.NewMutex(lockKey,
		redsync.WithExpiry(boundsLockTTL),
		redsync.WithTries(boundsLockMaxRetries),
		redsync.WithRetryDelayFunc(exponentialBackoff),
	)

	if err := mutex.LockContext(ctx); err != nil {
		// Check if context was canceled or timed out
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if errors.Is(err, redsync.ErrFailed) {
			return nil, fmt.Errorf("%w for model %s", ErrLockTimeout, modelID)
		}

		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	return &boundsLock{mutex: mutex}, nil
}
