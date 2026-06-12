package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTracker(t *testing.T) *redisScheduleTracker {
	t.Helper()

	_, client := testutil.NewMiniredisClient(t)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	tracker, ok := newScheduleTracker(log, client).(*redisScheduleTracker)
	require.True(t, ok)

	return tracker
}

// TestTrackerGetLastRunParseError covers the timestamp parse-error branch of
// GetLastRun by storing a non-RFC3339 value directly in Redis.
func TestTrackerGetLastRunParseError(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()

	taskID := "bad:timestamp"
	key := scheduleKeyPrefix + taskID
	require.NoError(t, tracker.redis.Set(ctx, key, "not-a-timestamp", 0).Err())

	_, err := tracker.GetLastRun(ctx, taskID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse timestamp")
}

// TestTrackerGetLastRunRedisError covers the non-redis.Nil error branch of
// GetLastRun by closing the underlying client before the call.
func TestTrackerGetLastRunRedisError(t *testing.T) {
	tracker := newTestTracker(t)
	require.NoError(t, tracker.redis.Close())

	_, err := tracker.GetLastRun(context.Background(), "any:task")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get last run")
}

// TestTrackerSetLastRunRedisError covers the error branch of SetLastRun by
// closing the underlying client before the call.
func TestTrackerSetLastRunRedisError(t *testing.T) {
	tracker := newTestTracker(t)
	require.NoError(t, tracker.redis.Close())

	err := tracker.SetLastRun(context.Background(), "any:task", time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set last run")
}

// TestTrackerCloseNilRedis covers the nil-redis path of Close.
func TestTrackerCloseNilRedis(t *testing.T) {
	tracker := &redisScheduleTracker{
		log:   logrus.New().WithField("component", "schedule_tracker"),
		redis: nil,
	}

	require.NoError(t, tracker.Close())
}
