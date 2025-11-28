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

func TestParseScheduleInterval(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		want     time.Duration
		wantErr  bool
	}{
		{
			name:     "@every 1s",
			schedule: "@every 1s",
			want:     1 * time.Second,
			wantErr:  false,
		},
		{
			name:     "@every 30s",
			schedule: "@every 30s",
			want:     30 * time.Second,
			wantErr:  false,
		},
		{
			name:     "@every 5m",
			schedule: "@every 5m",
			want:     5 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "@every 1h",
			schedule: "@every 1h",
			want:     1 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "cron expression every minute",
			schedule: "*/1 * * * *",
			want:     1 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "cron expression every 5 minutes",
			schedule: "*/5 * * * *",
			want:     5 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "invalid @every format",
			schedule: "@every invalid",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "invalid cron expression",
			schedule: "invalid cron",
			want:     0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseScheduleInterval(tt.schedule)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestRedisScheduleTracker(t *testing.T) {
	// Start miniredis
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer redisClient.Close()

	tracker := newScheduleTracker(log, redisClient)
	ctx := context.Background()

	t.Run("GetLastRun returns zero time for non-existent task", func(t *testing.T) {
		mr.FlushAll()

		lastRun, err := tracker.GetLastRun(ctx, "test:nonexistent")
		require.NoError(t, err)
		assert.True(t, lastRun.IsZero(), "Expected zero time for non-existent task")
	})

	t.Run("SetLastRun and GetLastRun work correctly", func(t *testing.T) {
		mr.FlushAll()

		taskID := "test:task1"
		now := time.Now().UTC().Truncate(time.Second)

		err := tracker.SetLastRun(ctx, taskID, now)
		require.NoError(t, err)

		lastRun, err := tracker.GetLastRun(ctx, taskID)
		require.NoError(t, err)
		assert.Equal(t, now.Unix(), lastRun.Unix(), "Timestamps should match")
	})

	t.Run("DeleteLastRun removes task timestamp", func(t *testing.T) {
		mr.FlushAll()

		taskID := "test:task3"
		now := time.Now().UTC().Truncate(time.Second)

		err := tracker.SetLastRun(ctx, taskID, now)
		require.NoError(t, err)

		err = tracker.DeleteLastRun(ctx, taskID)
		require.NoError(t, err)

		lastRun, err := tracker.GetLastRun(ctx, taskID)
		require.NoError(t, err)
		assert.True(t, lastRun.IsZero(), "Timestamp should be deleted")
	})
}
