package scheduler

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/redis/go-redis/v9"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// When this instance is leader, Stop() must terminate the ticker goroutine.
// The ticker context derives from the context passed to Start, which the
// engine supplies as context.Background(), so Stop() is the only shutdown
// path: if it doesn't cancel the ticker, every leader shutdown blocks until
// ShutdownTimeout and leaks the ticker goroutine.
func TestStopHaltsTickerWhenLeader(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	log, hook := logrustest.NewNullLogger()

	cfg := &Config{
		Concurrency:     1,
		Consolidation:   "@every 10m",
		ShutdownTimeout: 2,
	}

	redisOpt := &redis.Options{Addr: mr.Addr()}

	svc, err := NewService(log, cfg, redisOpt, &testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{}, &adminfake.FakeAdminService{})
	require.NoError(t, err)

	// Mirror production: the engine starts the scheduler with a background context.
	require.NoError(t, svc.Start(context.Background()))

	var (
		stopOnce sync.Once
		stopErr  error
	)

	stop := func() error {
		stopOnce.Do(func() { stopErr = svc.Stop() })

		return stopErr
	}
	t.Cleanup(func() { _ = stop() })

	// Wait until this instance wins leadership and the ticker loop is running.
	require.Eventually(t, func() bool {
		return hookContains(hook, "Starting ticker service")
	}, 15*time.Second, 100*time.Millisecond, "instance was never promoted to leader")

	require.NoError(t, stop())

	assert.False(t, hookContains(hook, "shutdown timed out"),
		"Stop() hit the shutdown timeout instead of stopping the ticker")
	assert.True(t,
		hookContains(hook, "Ticker stopped via Stop()") || hookContains(hook, "Ticker context canceled"),
		"ticker goroutine did not exit during Stop()")
}

func hookContains(hook *logrustest.Hook, substr string) bool {
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, substr) {
			return true
		}
	}

	return false
}
