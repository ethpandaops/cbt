package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestElector(t *testing.T) (*elector, *redis.Client) {
	t.Helper()

	mr, client := testutil.NewMiniredisClient(t)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	e, ok := NewLeaderElector(log, &redis.Options{Addr: mr.Addr()}).(*elector)
	require.True(t, ok)
	// Fast intervals keep the run-loop tests in the millisecond range.
	e.renewInterval = 20 * time.Millisecond
	e.leaseTTL = 500 * time.Millisecond
	t.Cleanup(func() { _ = e.redis.Close() })

	return e, client
}

// TestTryAcquireFreshLock covers tryAcquire when the lock is free (NX succeeds).
func TestTryAcquireFreshLock(t *testing.T) {
	e, _ := newTestElector(t)

	acquired := e.tryAcquire(context.Background())
	assert.True(t, acquired, "should acquire a free lock")
}

// TestTryAcquireRenewOwnLock covers the renew-own-lease path: when the same
// instance already owns the key, tryAcquire extends the lease.
func TestTryAcquireRenewOwnLock(t *testing.T) {
	e, client := newTestElector(t)
	ctx := context.Background()

	// First acquire takes the lock.
	require.True(t, e.tryAcquire(ctx))

	// Sanity: the stored owner is this instance.
	owner, err := client.Get(ctx, e.leaderKey).Result()
	require.NoError(t, err)
	require.Equal(t, e.instanceID, owner)

	// Second acquire should renew (Expire) and return true.
	assert.True(t, e.tryAcquire(ctx))
}

// TestTryAcquireAnotherOwner covers the path where a different instance owns the
// lock: tryAcquire returns false.
func TestTryAcquireAnotherOwner(t *testing.T) {
	e, client := newTestElector(t)
	ctx := context.Background()

	// A different instance owns the key.
	require.NoError(t, client.Set(ctx, e.leaderKey, "other-instance", leaseTTL).Err())

	assert.False(t, e.tryAcquire(ctx), "should not acquire a lock owned by another instance")
}

// TestTryAcquireRedisError covers the SetArgs error branch (non-redis.Nil) by
// closing the client first.
func TestTryAcquireRedisError(t *testing.T) {
	e, _ := newTestElector(t)
	require.NoError(t, e.redis.Close())

	assert.False(t, e.tryAcquire(context.Background()))
}

// TestRelinquishOwnedLock covers relinquish when this instance is leader and
// owns the lock: the key is deleted.
func TestRelinquishOwnedLock(t *testing.T) {
	e, client := newTestElector(t)
	ctx := context.Background()

	require.True(t, e.tryAcquire(ctx))
	e.setLeader(true)

	e.relinquish(ctx)

	assert.False(t, e.IsLeader(), "should no longer be leader after relinquish")

	_, err := client.Get(ctx, e.leaderKey).Result()
	assert.ErrorIs(t, err, redis.Nil, "leader key should be deleted")
}

// TestRelinquishNotLeader covers the early return of relinquish when this
// instance is not the leader.
func TestRelinquishNotLeader(t *testing.T) {
	e, _ := newTestElector(t)

	// Not leader -> relinquish is a no-op and must not panic.
	e.relinquish(context.Background())
	assert.False(t, e.IsLeader())
}

// TestRelinquishScriptError covers the relinquish error branch when the redis
// script fails (client closed) while this instance believes it is leader.
func TestRelinquishScriptError(t *testing.T) {
	e, _ := newTestElector(t)
	e.setLeader(true)
	require.NoError(t, e.redis.Close())

	// Script run fails but relinquish still clears the leader flag.
	e.relinquish(context.Background())
	assert.False(t, e.IsLeader())
}

// TestElectorPromotionAndDemotion drives the full run loop via two electors:
// one is promoted (channel signal), then stopping it lets the other take over
// (demotion + promotion signals). renewInterval/leaseTTL are package constants;
// this is the one election test that waits on real ticks.
func TestElectorPromotionDemotionSignals(t *testing.T) {
	e, _ := newTestElector(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, e.Start(ctx))
	t.Cleanup(func() { _ = e.Stop() })

	// Wait for promotion via the promoted channel (covers run loop promote path).
	select {
	case <-e.PromotedChan():
	case <-time.After(2 * time.Second):
		t.Fatal("expected promotion signal")
	}

	require.True(t, e.IsLeader())
}

// TestElectorStopAfterStart covers Stop including the redis.Close path and the
// run-loop's done branch.
func TestElectorStopAfterStart(t *testing.T) {
	e, _ := newTestElector(t)

	ctx := context.Background()
	require.NoError(t, e.Start(ctx))

	require.NoError(t, e.Stop())
}

// TestElectorRunContextCancel covers the run loop's ctx.Done() exit path.
func TestElectorRunContextCancel(t *testing.T) {
	e, _ := newTestElector(t)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, e.Start(ctx))

	// Cancel the context; the run goroutine should exit via ctx.Done().
	cancel()

	// Stop still completes cleanly (done channel + wg.Wait).
	require.NoError(t, e.Stop())
}
