package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockScheduleTracker implements scheduleTracker for testing
type mockScheduleTracker struct {
	mu       sync.RWMutex
	lastRuns map[string]time.Time
	setRuns  map[string]time.Time
}

func newMockScheduleTracker() *mockScheduleTracker {
	return &mockScheduleTracker{
		lastRuns: make(map[string]time.Time),
		setRuns:  make(map[string]time.Time),
	}
}

func (m *mockScheduleTracker) GetLastRun(_ context.Context, taskID string) (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if lastRun, ok := m.lastRuns[taskID]; ok {
		return lastRun, nil
	}
	return time.Time{}, nil
}

func (m *mockScheduleTracker) SetLastRun(_ context.Context, taskID string, timestamp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setRuns[taskID] = timestamp
	m.lastRuns[taskID] = timestamp
	return nil
}

func (m *mockScheduleTracker) DeleteLastRun(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lastRuns, taskID)
	delete(m.setRuns, taskID)
	return nil
}

func (m *mockScheduleTracker) GetAllTaskIDs(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.lastRuns))
	for id := range m.lastRuns {
		ids = append(ids, id)
	}
	return ids, nil
}

// getSetRuns returns a copy of the setRuns map (thread-safe)
func (m *mockScheduleTracker) getSetRuns() map[string]time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]time.Time, len(m.setRuns))
	for k, v := range m.setRuns {
		result[k] = v
	}
	return result
}

// setInitialLastRun sets the initial last run time for a task (thread-safe)
func (m *mockScheduleTracker) setInitialLastRun(taskID string, timestamp time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastRuns[taskID] = timestamp
}

func TestTickerService(t *testing.T) {
	// Start miniredis for all ticker tests
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	t.Run("ticker enqueues task when interval elapsed", func(t *testing.T) {
		mockTracker := newMockScheduleTracker()

		// Create Asynq client connected to miniredis
		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task that should run (last run was 2 seconds ago, interval is 1 second)
		taskID := "test:task1"
		mockTracker.setInitialLastRun(taskID, time.Now().Add(-2*time.Second))

		tasks := []scheduledTask{
			{
				ID:       taskID,
				Schedule: "@every 1s",
				Interval: 1 * time.Second,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		// Start ticker in background
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Wait for ticker to run
		time.Sleep(1500 * time.Millisecond)

		// Stop ticker
		cancel()
		ticker.Stop()

		// Verify task was enqueued by checking if SetLastRun was called
		setRuns := mockTracker.getSetRuns()
		assert.Contains(t, setRuns, taskID, "Task should have been enqueued and timestamp updated")
	})

	t.Run("ticker does not enqueue task when interval not elapsed", func(t *testing.T) {
		mockTracker := newMockScheduleTracker()

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task that should NOT run (last run was 500ms ago, interval is 1 minute)
		taskID := "test:task2"
		mockTracker.setInitialLastRun(taskID, time.Now().Add(-500*time.Millisecond))

		tasks := []scheduledTask{
			{
				ID:       taskID,
				Schedule: "@every 1m",
				Interval: 1 * time.Minute,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		// Start ticker in background
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Wait for ticker to run
		time.Sleep(1500 * time.Millisecond)

		// Stop ticker
		cancel()
		ticker.Stop()

		// Verify task was NOT enqueued
		setRuns := mockTracker.getSetRuns()
		assert.NotContains(t, setRuns, taskID, "Task should not have been enqueued")
	})

	t.Run("ticker enqueues task on first run (zero time)", func(t *testing.T) {
		mockTracker := newMockScheduleTracker()

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task that has never run (zero time)
		taskID := "test:task3"
		// Don't set lastRuns - it will return zero time

		tasks := []scheduledTask{
			{
				ID:       taskID,
				Schedule: "@every 1s",
				Interval: 1 * time.Second,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		// Start ticker in background
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Wait for ticker to run
		time.Sleep(1500 * time.Millisecond)

		// Stop ticker
		cancel()
		ticker.Stop()

		// Verify task was enqueued on first run
		setRuns := mockTracker.getSetRuns()
		assert.Contains(t, setRuns, taskID, "Task should be enqueued on first run")
	})

	t.Run("ticker stops gracefully on context cancel", func(t *testing.T) {
		mockTracker := newMockScheduleTracker()

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		tasks := []scheduledTask{
			{
				ID:       "test:task4",
				Schedule: "@every 1s",
				Interval: 1 * time.Second,
				Task:     asynq.NewTask("test:task4", nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		ctx, cancel := context.WithCancel(context.Background())

		// Start ticker
		done := make(chan error)
		go func() {
			done <- ticker.Start(ctx)
		}()

		// Let it run briefly
		time.Sleep(500 * time.Millisecond)

		// Cancel context
		cancel()

		// Should stop within reasonable time
		select {
		case err := <-done:
			assert.Error(t, err, "Should return context.Canceled error")
		case <-time.After(2 * time.Second):
			t.Fatal("Ticker did not stop within timeout")
		}
	})

	t.Run("ticker stops gracefully on Stop call", func(t *testing.T) {
		mockTracker := newMockScheduleTracker()

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		tasks := []scheduledTask{
			{
				ID:       "test:task5",
				Schedule: "@every 1s",
				Interval: 1 * time.Second,
				Task:     asynq.NewTask("test:task5", nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		ctx := context.Background()

		// Start ticker
		done := make(chan error)
		go func() {
			done <- ticker.Start(ctx)
		}()

		// Let it run briefly
		time.Sleep(500 * time.Millisecond)

		// Call Stop
		err := ticker.Stop()
		require.NoError(t, err)

		// Should stop within reasonable time
		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("Ticker did not stop within timeout")
		}
	})
}
