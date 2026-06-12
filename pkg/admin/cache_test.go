package admin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/cbt/internal/testutil"
)

// setupTestRedis returns a miniredis server and connected client, both
// closed automatically when the test completes.
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	return testutil.NewMiniredisClient(t)
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
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Release first lock
	err = lock1.Unlock(ctx)
	require.NoError(t, err)

	// Fast forward time in miniredis to ensure lock TTL doesn't interfere
	mr.FastForward(boundsLockTTL + time.Second)
}

func TestCacheManager_AcquireLock_SucceedsAfterRelease(t *testing.T) {
	_, client := setupTestRedis(t)
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
	cm := NewCacheManager(client)
	ctx := context.Background()

	const numGoroutines = 10
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// All goroutines try to acquire the same lock at once
	for range numGoroutines {
		wg.Go(func() {
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
		})
	}

	wg.Wait()

	// At least some should have succeeded (not all due to timeout)
	assert.Positive(t, successCount, "At least one goroutine should acquire the lock")
}

func TestCacheManager_GetSetBounds(t *testing.T) {
	_, client := setupTestRedis(t)
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
	cm := NewCacheManager(client)
	ctx := context.Background()

	// Get non-existent cache entry
	cache, err := cm.GetBounds(ctx, "non-existent-model")
	require.NoError(t, err)
	assert.Nil(t, cache)
}

func TestCacheManager_GetBounds_UnmarshalError(t *testing.T) {
	mr, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	require.NoError(t, mr.Set("cbt:external:bad-model", "not-json"))

	cache, err := cm.GetBounds(ctx, "bad-model")
	require.Error(t, err)
	assert.Nil(t, cache)
}

func TestCacheManager_GetBounds_RedisError(t *testing.T) {
	cm := NewCacheManager(closedRedisClient(t))

	cache, err := cm.GetBounds(context.Background(), "test-model")
	require.Error(t, err)
	assert.Nil(t, cache)
}

func TestCacheManager_SetBounds_RedisError(t *testing.T) {
	cm := NewCacheManager(closedRedisClient(t))

	err := cm.SetBounds(context.Background(), &BoundsCache{ModelID: "test-model"})
	require.Error(t, err)
}

func TestCacheManager_DeleteBounds(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	// Seed a bounds entry then delete it.
	require.NoError(t, cm.SetBounds(ctx, &BoundsCache{ModelID: "del-model", Min: 1, Max: 2}))
	require.NoError(t, cm.DeleteBounds(ctx, "del-model"))

	got, err := cm.GetBounds(ctx, "del-model")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCacheManager_DeleteBounds_RedisError(t *testing.T) {
	cm := NewCacheManager(closedRedisClient(t))

	err := cm.DeleteBounds(context.Background(), "test-model")
	require.Error(t, err)
}

func TestBoundsLock_Unlock_Error(t *testing.T) {
	mr, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	lock, err := cm.AcquireLock(ctx, "test-model")
	require.NoError(t, err)

	// Expire the lock so the subsequent unlock fails (the value no longer matches).
	mr.FastForward(boundsLockTTL + time.Second)

	err = lock.Unlock(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to release lock")
}

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		name  string
		tries int
		want  time.Duration
	}{
		{name: "first try", tries: 0, want: 50 * time.Millisecond},
		{name: "second try", tries: 1, want: 100 * time.Millisecond},
		{name: "third try", tries: 2, want: 200 * time.Millisecond},
		{name: "capped at max shift", tries: 3, want: 400 * time.Millisecond},
		{name: "beyond max shift stays capped", tries: 100, want: 400 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, exponentialBackoff(tt.tries))
		})
	}
}

func TestCacheManager_AcquireLock_FailsWhenHeld(t *testing.T) {
	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	// Hold the lock.
	lock1, err := cm.AcquireLock(ctx, "held-model")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock1.Unlock(context.Background()) })

	// Manually claim every redsync retry quickly by exhausting retries with a
	// background contender that keeps the key alive. Easier: a fresh manager with
	// a 1-try mutex would fail, but AcquireLock hardcodes tries. Instead acquire a
	// second lock with a context that is already canceled so the redsync loop
	// observes ctx.Err() and returns it.
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	lock2, err := cm.AcquireLock(canceledCtx, "held-model")
	require.Error(t, err)
	assert.Nil(t, lock2)
	require.ErrorIs(t, err, context.Canceled)
}

// withMaxRetries temporarily lowers the lock retry count so retry-exhaustion paths
// run fast, restoring the original on cleanup.
func withMaxRetries(t *testing.T, n int) {
	t.Helper()

	orig := boundsLockMaxRetries
	boundsLockMaxRetries = n
	t.Cleanup(func() { boundsLockMaxRetries = orig })
}

func TestCacheManager_AcquireLock_RetriesExhausted(t *testing.T) {
	withMaxRetries(t, 2)

	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)
	ctx := context.Background()

	// Hold the lock so contenders never acquire quorum.
	lock1, err := cm.AcquireLock(ctx, "held-model")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock1.Unlock(context.Background()) })

	// A live, non-canceled context: redsync exhausts its tries. With a single node
	// the held key surfaces as a "taken" error on the final attempt, so AcquireLock
	// reports it through the generic acquisition-failure branch.
	lock2, err := cm.AcquireLock(ctx, "held-model")
	require.Error(t, err)
	assert.Nil(t, lock2)
	assert.Contains(t, err.Error(), "failed to acquire lock")
}

func TestCacheManager_AcquireLock_AcquireCommandError(t *testing.T) {
	withMaxRetries(t, 1)

	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)

	// Fault the SET so every acquire attempt errors; on the final try redsync
	// surfaces the command error, hitting the generic failure branch.
	client.AddHook(faultHook{cmdName: "set", err: errInjected})

	lock, err := cm.AcquireLock(context.Background(), "err-model")
	require.Error(t, err)
	assert.Nil(t, lock)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to acquire lock")
}

func TestClassifyLockError(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name      string
		ctx       context.Context
		err       error
		assertErr func(t *testing.T, err error)
	}{
		{
			name: "context error takes precedence",
			ctx:  canceledCtx,
			err:  redsync.ErrFailed,
			assertErr: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, context.Canceled)
			},
		},
		{
			name: "redsync quorum failure maps to lock timeout",
			ctx:  context.Background(),
			err:  redsync.ErrFailed,
			assertErr: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, ErrLockTimeout)
				assert.Contains(t, err.Error(), "for model test-model")
			},
		},
		{
			name: "other errors are wrapped generically",
			ctx:  context.Background(),
			err:  errInjected,
			assertErr: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, errInjected)
				assert.Contains(t, err.Error(), "failed to acquire lock")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyLockError(tt.ctx, tt.err, "test-model")
			require.Error(t, err)
			tt.assertErr(t, err)
		})
	}
}

func TestCacheManager_SetBounds_MarshalError(t *testing.T) {
	orig := marshalBounds
	marshalBounds = func(any) ([]byte, error) { return nil, errInjected }
	t.Cleanup(func() { marshalBounds = orig })

	_, client := setupTestRedis(t)
	cm := NewCacheManager(client)

	err := cm.SetBounds(context.Background(), &BoundsCache{ModelID: "test-model"})
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
}
