package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeaderElection(t *testing.T) {
	// Start mini redis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	redisOpt := &redis.Options{
		Addr: mr.Addr(),
	}

	t.Run("single instance becomes leader", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Clear any existing locks
		mr.FlushAll()

		elector := NewLeaderElector(log, redisOpt)
		require.NoError(t, elector.Start(ctx))
		defer elector.Stop()

		// Wait a bit for election to occur
		time.Sleep(renewInterval + 500*time.Millisecond)

		assert.True(t, elector.IsLeader(), "Single instance should become leader")
	})

	t.Run("multiple instances elect one leader", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Clear any existing locks
		mr.FlushAll()

		elector1 := NewLeaderElector(log, redisOpt)
		elector2 := NewLeaderElector(log, redisOpt)

		require.NoError(t, elector1.Start(ctx))
		defer elector1.Stop()

		require.NoError(t, elector2.Start(ctx))
		defer elector2.Stop()

		// Wait for election
		time.Sleep(renewInterval + 500*time.Millisecond)

		// Exactly one should be leader
		leaders := 0
		if elector1.IsLeader() {
			leaders++
		}
		if elector2.IsLeader() {
			leaders++
		}

		assert.Equal(t, 1, leaders, "Exactly one instance should be leader")
	})

	t.Run("leader failover", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// Clear any existing locks
		mr.FlushAll()

		elector1 := NewLeaderElector(log, redisOpt)
		elector2 := NewLeaderElector(log, redisOpt)

		require.NoError(t, elector1.Start(ctx))
		require.NoError(t, elector2.Start(ctx))

		// Wait for election
		time.Sleep(renewInterval + 500*time.Millisecond)

		// Determine who is leader and stop them
		var leader, follower LeaderElector
		if elector1.IsLeader() {
			leader = elector1
			follower = elector2
			defer elector2.Stop()
		} else {
			leader = elector2
			follower = elector1
			defer elector1.Stop()
		}

		require.NoError(t, leader.Stop())

		// Wait for lease to expire and new election
		// The follower should detect the expired lock and take over
		time.Sleep(leaseTTL + renewInterval + 500*time.Millisecond)

		assert.True(t, follower.IsLeader(), "Follower should become leader after leader stops")
	})

	t.Run("wait for leadership", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Clear any existing locks
		mr.FlushAll()

		elector := NewLeaderElector(log, redisOpt)
		require.NoError(t, elector.Start(ctx))
		defer elector.Stop()

		// Wait in goroutine to avoid blocking
		done := make(chan error, 1)
		go func() {
			done <- elector.WaitForLeadership(ctx)
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
			assert.True(t, elector.IsLeader())
		case <-time.After(renewInterval + time.Second):
			t.Fatal("Timed out waiting for leadership")
		}
	})
}
