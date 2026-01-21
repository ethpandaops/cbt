package admin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func TestCacheManager_AcquireLock(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*miniredis.Miniredis)
		wantErr bool
	}{
		{
			name:    "acquires lock when none exists",
			setup:   func(_ *miniredis.Miniredis) {},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, client := setupTestRedis(t)
			defer client.Close()

			tt.setup(mr)

			cm := NewCacheManager(client)
			ctx := context.Background()

			lock, err := cm.AcquireLock(ctx, "test-model")

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, lock)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, lock)

				// Verify lock exists in Redis
				val, err := client.Get(ctx, "cbt:external:test-model:lock").Result()
				require.NoError(t, err)
				assert.NotEmpty(t, val)

				// Clean up
				err = lock.Unlock(ctx)
				require.NoError(t, err)
			}
		})
	}
}

func TestCacheManager_AcquireLock_BlocksWhenHeld(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	// Acquire the first lock
	lock1, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)
	require.NotNil(t, lock1)

	// Try to acquire a second lock with a short timeout context
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	lock2, err := cm.AcquireLock(shortCtx, "test-model")

	// Should fail because lock is held and context times out
	require.Error(t, err)
	assert.Nil(t, lock2)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Release first lock
	err = lock1.Unlock(ctx)
	require.NoError(t, err)

	// Fast forward time in miniredis to ensure lock TTL doesn't interfere
	mr.FastForward(boundsLockTTL + time.Second)
}

func TestCacheManager_AcquireLock_SucceedsAfterRelease(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	// Acquire first lock
	lock1, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)

	// Release it
	err = lock1.Unlock(ctx)
	require.NoError(t, err)

	// Acquire second lock - should succeed immediately
	lock2, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)
	require.NotNil(t, lock2)

	// Clean up
	err = lock2.Unlock(ctx)
	require.NoError(t, err)
}

func TestBoundsLock_Unlock_ReleasesLock(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	// Acquire a lock
	lock, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)

	// Verify lock exists in Redis (redsync uses a different key pattern internally)
	val, err := client.Get(ctx, "cbt:external:test-model:lock").Result()
	require.NoError(t, err)
	assert.NotEmpty(t, val)

	// Unlock should release the lock
	err = lock.Unlock(ctx)
	require.NoError(t, err)

	// Now key should be gone
	_, err = client.Get(ctx, "cbt:external:test-model:lock").Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func TestCacheManager_AcquireLock_ExpiresAfterTTL(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	// Acquire a lock
	lock, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)
	require.NotNil(t, lock)

	// Verify lock exists
	_, err = client.Get(ctx, "cbt:external:test-model:lock").Result()
	require.NoError(t, err)

	// Fast forward past TTL
	mr.FastForward(boundsLockTTL + time.Second)

	// Lock should have expired - another acquire should succeed
	lock2, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)
	require.NotNil(t, lock2)

	// Clean up
	err = lock2.Unlock(ctx)
	require.NoError(t, err)
}

func TestCacheManager_AcquireLock_DifferentModels(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	// Acquire locks for different models - should all succeed
	lock1, err := cm.AcquireLock(ctx, "model-1")
	require.NoError(t, err)

	lock2, err := cm.AcquireLock(ctx, "model-2")
	require.NoError(t, err)

	lock3, err := cm.AcquireLock(ctx, "model-3")
	require.NoError(t, err)

	// Clean up
	require.NoError(t, lock1.Unlock(ctx))
	require.NoError(t, lock2.Unlock(ctx))
	require.NoError(t, lock3.Unlock(ctx))
}

func TestCacheManager_AcquireLock_ConcurrentAccess(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	const numGoroutines = 10
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// All goroutines try to acquire the same lock at once
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Use a short timeout so test doesn't hang
			shortCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			lock, err := cm.AcquireLock(shortCtx, "contested-model")
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()

				// Hold for a bit then release
				time.Sleep(10 * time.Millisecond)
				_ = lock.Unlock(ctx)
			}
		}()
	}

	wg.Wait()

	// At least some should have succeeded (not all due to timeout)
	assert.Greater(t, successCount, 0, "At least one goroutine should acquire the lock")
}

func TestCacheManager_GetSetBounds(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	now := time.Now().UTC()

	// Test SetBounds
	cache := &BoundsCache{
		ModelID:             "test-model",
		Min:                 100,
		Max:                 200,
		LastIncrementalScan: now,
		LastFullScan:        now,
		PreviousMin:         50,
		PreviousMax:         150,
		InitialScanComplete: true,
		UpdatedAt:           now,
	}

	err := cm.SetBounds(ctx, cache)
	require.NoError(t, err)

	// Test GetBounds
	retrieved, err := cm.GetBounds(ctx, "test-model")
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	assert.Equal(t, "test-model", retrieved.ModelID)
	assert.Equal(t, uint64(100), retrieved.Min)
	assert.Equal(t, uint64(200), retrieved.Max)
	assert.Equal(t, uint64(50), retrieved.PreviousMin)
	assert.Equal(t, uint64(150), retrieved.PreviousMax)
	assert.True(t, retrieved.InitialScanComplete)
}

func TestCacheManager_GetBounds_CacheMiss(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cm := NewCacheManager(client)
	ctx := context.Background()

	// Get non-existent cache entry
	cache, err := cm.GetBounds(ctx, "non-existent-model")
	require.NoError(t, err)
	assert.Nil(t, cache)
}
