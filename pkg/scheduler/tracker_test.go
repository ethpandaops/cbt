package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleTracker(t *testing.T) {
	t.Skip("Skipping Redis integration tests - run manually with Redis available")

	// Skip if Redis not available
	redisOpt := &redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use test DB
	}
	client := redis.NewClient(redisOpt)
	ctx := context.Background()

	// Ping to check connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available, skipping tracker tests")
	}

	// Clean up test keys after test
	defer func() {
		keys, _ := client.Keys(ctx, "cbt:scheduler:task:test:*").Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Close()
	}()

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	tracker := newScheduleTracker(log, client)

	t.Run("GetLastRun returns zero time for non-existent task", func(t *testing.T) {
		lastRun, err := tracker.GetLastRun(ctx, "test:nonexistent")
		require.NoError(t, err)
		assert.True(t, lastRun.IsZero(), "Expected zero time for non-existent task")
	})

	t.Run("SetLastRun and GetLastRun work correctly", func(t *testing.T) {
		taskID := "test:task1"
		now := time.Now().UTC().Truncate(time.Second)

		// Set last run
		err := tracker.SetLastRun(ctx, taskID, now)
		require.NoError(t, err)

		// Get last run
		lastRun, err := tracker.GetLastRun(ctx, taskID)
		require.NoError(t, err)
		assert.Equal(t, now.Unix(), lastRun.Unix(), "Timestamps should match")
	})

	t.Run("SetLastRun updates existing timestamp", func(t *testing.T) {
		taskID := "test:task2"
		time1 := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
		time2 := time.Now().UTC().Truncate(time.Second)

		// Set initial timestamp
		err := tracker.SetLastRun(ctx, taskID, time1)
		require.NoError(t, err)

		// Update timestamp
		err = tracker.SetLastRun(ctx, taskID, time2)
		require.NoError(t, err)

		// Verify updated timestamp
		lastRun, err := tracker.GetLastRun(ctx, taskID)
		require.NoError(t, err)
		assert.Equal(t, time2.Unix(), lastRun.Unix(), "Should return updated timestamp")
	})

	t.Run("DeleteLastRun removes task timestamp", func(t *testing.T) {
		taskID := "test:task3"
		now := time.Now().UTC().Truncate(time.Second)

		// Set timestamp
		err := tracker.SetLastRun(ctx, taskID, now)
		require.NoError(t, err)

		// Delete timestamp
		err = tracker.DeleteLastRun(ctx, taskID)
		require.NoError(t, err)

		// Verify it's gone
		lastRun, err := tracker.GetLastRun(ctx, taskID)
		require.NoError(t, err)
		assert.True(t, lastRun.IsZero(), "Timestamp should be deleted")
	})

	t.Run("GetAllTaskIDs returns all tracked tasks", func(t *testing.T) {
		// Set up multiple tasks
		tasks := []string{"test:alpha", "test:beta", "test:gamma"}
		now := time.Now().UTC()

		for _, taskID := range tasks {
			err := tracker.SetLastRun(ctx, taskID, now)
			require.NoError(t, err)
		}

		// Get all task IDs
		allIDs, err := tracker.GetAllTaskIDs(ctx)
		require.NoError(t, err)

		// Verify all test tasks are present
		for _, taskID := range tasks {
			assert.Contains(t, allIDs, taskID, "Should contain task %s", taskID)
		}
	})

	t.Run("Timestamps persist with RFC3339 format", func(t *testing.T) {
		taskID := "test:task4"
		now := time.Now().UTC().Truncate(time.Second)

		// Set timestamp
		err := tracker.SetLastRun(ctx, taskID, now)
		require.NoError(t, err)

		// Read raw value from Redis
		key := scheduleKeyPrefix + taskID
		rawValue, err := client.Get(ctx, key).Result()
		require.NoError(t, err)

		// Verify it's in RFC3339 format
		parsed, err := time.Parse(time.RFC3339, rawValue)
		require.NoError(t, err)
		assert.Equal(t, now.Unix(), parsed.Unix(), "Should parse correctly from RFC3339")
	})
}

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
