package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTracker implements scheduleTracker for unit testing without Redis
type mockTracker struct {
	lastRuns map[string]time.Time
}

func newMockTracker() *mockTracker {
	return &mockTracker{
		lastRuns: make(map[string]time.Time),
	}
}

func (m *mockTracker) GetLastRun(_ context.Context, taskID string) (time.Time, error) {
	if lastRun, ok := m.lastRuns[taskID]; ok {
		return lastRun, nil
	}
	return time.Time{}, nil
}

func (m *mockTracker) SetLastRun(_ context.Context, taskID string, timestamp time.Time) error {
	m.lastRuns[taskID] = timestamp
	return nil
}

func (m *mockTracker) DeleteLastRun(_ context.Context, taskID string) error {
	delete(m.lastRuns, taskID)
	return nil
}

func (m *mockTracker) GetAllTaskIDs(_ context.Context) ([]string, error) {
	ids := make([]string, 0, len(m.lastRuns))
	for id := range m.lastRuns {
		ids = append(ids, id)
	}
	return ids, nil
}

func TestScheduleTracker(t *testing.T) {
	tracker := newMockTracker()
	ctx := context.Background()

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
		// Create fresh tracker
		freshTracker := newMockTracker()

		// Set up multiple tasks
		tasks := []string{"test:alpha", "test:beta", "test:gamma"}
		now := time.Now().UTC()

		for _, taskID := range tasks {
			err := freshTracker.SetLastRun(ctx, taskID, now)
			require.NoError(t, err)
		}

		// Get all task IDs
		allIDs, err := freshTracker.GetAllTaskIDs(ctx)
		require.NoError(t, err)

		// Verify all test tasks are present
		assert.Len(t, allIDs, len(tasks), "Should have all tasks")
		for _, taskID := range tasks {
			assert.Contains(t, allIDs, taskID, "Should contain task %s", taskID)
		}
	})

	t.Run("Mock tracker behavior matches interface", func(_ *testing.T) {
		// Verify the mock implements the scheduleTracker interface
		var _ scheduleTracker = (*mockTracker)(nil)
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

	t.Run("GetAllTaskIDs returns all tracked tasks using SCAN", func(t *testing.T) {
		mr.FlushAll()

		// Set up multiple tasks
		tasks := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		now := time.Now().UTC()

		for _, taskID := range tasks {
			err := tracker.SetLastRun(ctx, taskID, now)
			require.NoError(t, err)
		}

		// Get all task IDs
		allIDs, err := tracker.GetAllTaskIDs(ctx)
		require.NoError(t, err)

		// Verify all test tasks are present
		assert.Len(t, allIDs, len(tasks), "Should have all tasks")

		for _, taskID := range tasks {
			assert.Contains(t, allIDs, taskID, "Should contain task %s", taskID)
		}
	})

	t.Run("GetAllTaskIDs only returns scheduler keys", func(t *testing.T) {
		mr.FlushAll()

		// Add scheduler keys
		schedulerTasks := []string{"task1", "task2"}
		now := time.Now().UTC()

		for _, taskID := range schedulerTasks {
			err := tracker.SetLastRun(ctx, taskID, now)
			require.NoError(t, err)
		}

		// Add non-scheduler keys directly to Redis
		err := redisClient.Set(ctx, "other:key1", "value1", 0).Err()
		require.NoError(t, err)
		err = redisClient.Set(ctx, "another:key2", "value2", 0).Err()
		require.NoError(t, err)

		// GetAllTaskIDs should only return scheduler tasks
		allIDs, err := tracker.GetAllTaskIDs(ctx)
		require.NoError(t, err)

		assert.Len(t, allIDs, len(schedulerTasks), "Should only return scheduler tasks")

		for _, taskID := range schedulerTasks {
			assert.Contains(t, allIDs, taskID)
		}

		assert.NotContains(t, allIDs, "other:key1")
		assert.NotContains(t, allIDs, "another:key2")
	})

	t.Run("GetAllTaskIDs returns empty slice when no tasks exist", func(t *testing.T) {
		mr.FlushAll()

		allIDs, err := tracker.GetAllTaskIDs(ctx)
		require.NoError(t, err)
		assert.Empty(t, allIDs, "Should return empty slice when no tasks exist")
	})

	t.Run("GetAllTaskIDs handles many keys correctly", func(t *testing.T) {
		mr.FlushAll()

		// Create more tasks than the SCAN batch size (100) to verify iteration works
		numTasks := 250
		now := time.Now().UTC()

		for i := 0; i < numTasks; i++ {
			taskID := fmt.Sprintf("task:%d", i)
			err := tracker.SetLastRun(ctx, taskID, now)
			require.NoError(t, err)
		}

		// Get all task IDs
		allIDs, err := tracker.GetAllTaskIDs(ctx)
		require.NoError(t, err)

		assert.Len(t, allIDs, numTasks, "Should return all %d tasks", numTasks)
	})
}
