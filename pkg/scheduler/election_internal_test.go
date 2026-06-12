package scheduler

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failCommandHook is a go-redis hook that returns errFake for any command whose
// name matches one of the configured (lowercased) command names.
type failCommandHook struct {
	fail map[string]bool
}

func (h *failCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *failCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.fail[strings.ToLower(cmd.Name())] {
			return errFake
		}

		return next(ctx, cmd)
	}
}

func (h *failCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}

func newElectorWithMiniredis(t *testing.T) (*elector, *miniredis.Miniredis) {
	t.Helper()

	mr := miniredis.RunT(t)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	e, ok := NewLeaderElector(log, &redis.Options{Addr: mr.Addr()}).(*elector)
	require.True(t, ok)
	// Drive the election loop fast so tests stay in the millisecond range.
	e.renewInterval = 20 * time.Millisecond
	e.leaseTTL = 500 * time.Millisecond
	t.Cleanup(func() { _ = e.redis.Close() })

	return e, mr
}

// TestTryAcquireGetError covers the branch where SetArgs NX fails (key already
// held), then the follow-up Get returns a non-redis.Nil error.
func TestTryAcquireGetError(t *testing.T) {
	e, mr := newElectorWithMiniredis(t)
	ctx := context.Background()

	// Another instance owns the lock so SetArgs NX returns redis.Nil.
	require.NoError(t, mr.Set(e.leaderKey, "other-instance"))

	e.redis.AddHook(&failCommandHook{fail: map[string]bool{"get": true}})

	assert.False(t, e.tryAcquire(ctx), "Get failure must yield a non-leader result")
}

// TestTryAcquireExpireError covers the renew path where this instance owns the
// lock but the Expire (lease renewal) fails.
func TestTryAcquireExpireError(t *testing.T) {
	e, mr := newElectorWithMiniredis(t)
	ctx := context.Background()

	// This instance already owns the key, so SetArgs NX returns redis.Nil and
	// Get returns our own instance ID, taking the renew branch.
	require.NoError(t, mr.Set(e.leaderKey, e.instanceID))

	e.redis.AddHook(&failCommandHook{fail: map[string]bool{"expire": true}})

	assert.False(t, e.tryAcquire(ctx), "Expire failure must yield a non-leader result")
}

// TestStopRedisCloseError covers the Stop branch where the redis client Close
// returns an error (already closed).
func TestStopRedisCloseError(t *testing.T) {
	e, _ := newElectorWithMiniredis(t)

	require.NoError(t, e.Start(context.Background()))

	// Close the client out-of-band so Stop's own Close call fails (logged Warn).
	require.NoError(t, e.redis.Close())

	require.NoError(t, e.Stop())
}

// TestRunLoopDemotion drives the run loop so the elector is first promoted and
// then demoted when another instance steals the lock. This is the run-loop
// demotion path (setLeader(false) + demoted signal). It waits on real ticks.
func TestRunLoopDemotion(t *testing.T) {
	e, mr := newElectorWithMiniredis(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, e.Start(ctx))
	t.Cleanup(func() { _ = e.Stop() })

	// Wait for promotion.
	select {
	case <-e.PromotedChan():
	case <-time.After(2 * time.Second):
		t.Fatal("expected promotion")
	}
	require.True(t, e.IsLeader())

	// Steal the lock: overwrite the key with another owner. On the next renew
	// tick tryAcquire will see a different owner and demote this instance.
	require.NoError(t, mr.Set(e.leaderKey, "thief-instance"))

	select {
	case <-e.DemotedChan():
	case <-time.After(2 * time.Second):
		t.Fatal("expected demotion after lock was stolen")
	}

	assert.False(t, e.IsLeader())
}
