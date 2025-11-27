package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	schedulermock "github.com/ethpandaops/cbt/pkg/scheduler/mock"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTickerService_NextRunCaching(t *testing.T) {
	// These tests verify the nextRun caching optimization that reduces Redis calls.
	// The optimization caches the next run time locally so we skip Redis lookups
	// for tasks that aren't due yet.

	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	t.Run("skips Redis call when cached nextRun is in future", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		taskID := "test:cached_skip"
		// Set lastRun to now, so next run should be 1 minute from now
		lastRunTime := time.Now()

		// GetLastRun should be called exactly once on first check to populate cache
		getLastRunCalls := 0
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			DoAndReturn(func(_ context.Context, _ string) (time.Time, error) {
				getLastRunCalls++
				return lastRunTime, nil
			}).
			Times(1) // Only called once to populate cache

		// SetLastRun should never be called since task is not due
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			Times(0)

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Let ticker run for 3 seconds (3+ tick cycles)
		// Without caching, this would call GetLastRun 3+ times
		// With caching, it should only call once (to populate cache)
		time.Sleep(3 * time.Second)

		cancel()
		ticker.Stop()

		// Verify GetLastRun was only called once
		assert.Equal(t, 1, getLastRunCalls,
			"GetLastRun should only be called once to populate cache, not on every tick")
	})

	t.Run("cache is updated after task execution", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		taskID := "test:cache_update"
		// Task was last run 2 seconds ago with 1 second interval, so it's due
		lastRunTime := time.Now().Add(-2 * time.Second)

		getLastRunCalls := 0
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			DoAndReturn(func(_ context.Context, _ string) (time.Time, error) {
				getLastRunCalls++
				return lastRunTime, nil
			}).
			AnyTimes()

		taskEnqueued := make(chan struct{}, 10)
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, newTime time.Time) error {
				// Update lastRunTime so subsequent checks see the new value
				lastRunTime = newTime
				select {
				case taskEnqueued <- struct{}{}:
				default:
				}
				return nil
			}).
			AnyTimes()

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Wait for first enqueue
		select {
		case <-taskEnqueued:
		case <-time.After(3 * time.Second):
			t.Fatal("Task should have been enqueued")
		}

		// Record calls after first enqueue
		callsAfterFirstEnqueue := getLastRunCalls

		// Sleep for 500ms - the cache should prevent Redis calls since next run is ~1s away
		time.Sleep(500 * time.Millisecond)

		cancel()
		ticker.Stop()

		// After the first enqueue, the cache should be updated with next run time
		// So no additional Redis calls should have been made in the 500ms window
		assert.Equal(t, callsAfterFirstEnqueue, getLastRunCalls,
			"No additional GetLastRun calls should be made when cache shows task not due")
	})

	t.Run("multiple tasks with different intervals use independent caches", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task 1: 1 minute interval, not due
		task1ID := "test:multi_1"
		task1LastRun := time.Now()

		// Task 2: 1 second interval, due
		task2ID := "test:multi_2"
		task2LastRun := time.Now().Add(-2 * time.Second)

		task1Calls := 0
		task2Calls := 0

		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), task1ID).
			DoAndReturn(func(_ context.Context, _ string) (time.Time, error) {
				task1Calls++
				return task1LastRun, nil
			}).
			AnyTimes()

		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), task2ID).
			DoAndReturn(func(_ context.Context, _ string) (time.Time, error) {
				task2Calls++
				return task2LastRun, nil
			}).
			AnyTimes()

		task2Enqueued := make(chan struct{}, 1)
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), task2ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, newTime time.Time) error {
				task2LastRun = newTime
				select {
				case task2Enqueued <- struct{}{}:
				default:
				}
				return nil
			}).
			AnyTimes()

		tasks := []scheduledTask{
			{
				ID:       task1ID,
				Schedule: "@every 1m",
				Interval: 1 * time.Minute,
				Task:     asynq.NewTask(task1ID, nil),
				Queue:    QueueName,
			},
			{
				ID:       task2ID,
				Schedule: "@every 1s",
				Interval: 1 * time.Second,
				Task:     asynq.NewTask(task2ID, nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Wait for task2 to be enqueued
		select {
		case <-task2Enqueued:
		case <-time.After(3 * time.Second):
			t.Fatal("Task 2 should have been enqueued")
		}

		// Wait a bit more
		time.Sleep(500 * time.Millisecond)

		cancel()
		ticker.Stop()

		// Task 1 should only have 1 GetLastRun call (to populate cache)
		// since its interval (1m) hasn't elapsed
		assert.Equal(t, 1, task1Calls,
			"Task 1 should only call GetLastRun once to populate cache")

		// Task 2 might have more calls since it actually executed
		assert.GreaterOrEqual(t, task2Calls, 1,
			"Task 2 should have at least 1 GetLastRun call")
	})

	t.Run("cache handles zero time (never run) correctly", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		taskID := "test:zero_time"

		// Task has never run (zero time) - should be immediately due
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			Return(time.Time{}, nil).
			AnyTimes()

		taskEnqueued := make(chan struct{}, 1)
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ time.Time) error {
				select {
				case taskEnqueued <- struct{}{}:
				default:
				}
				return nil
			}).
			AnyTimes()

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Task with zero time should be immediately enqueued
		select {
		case <-taskEnqueued:
			// Success
		case <-time.After(3 * time.Second):
			t.Fatal("Task with zero lastRun should be immediately enqueued")
		}

		cancel()
		ticker.Stop()
	})

	t.Run("cache correctly rechecks when time advances past nextRun", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		taskID := "test:recheck"
		// Task will be due in 1.5 seconds
		lastRunTime := time.Now().Add(-500 * time.Millisecond)

		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			DoAndReturn(func(_ context.Context, _ string) (time.Time, error) {
				return lastRunTime, nil
			}).
			AnyTimes()

		enqueuedCount := 0
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, newTime time.Time) error {
				lastRunTime = newTime
				enqueuedCount++
				return nil
			}).
			AnyTimes()

		tasks := []scheduledTask{
			{
				ID:       taskID,
				Schedule: "@every 2s",
				Interval: 2 * time.Second,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			},
		}

		ticker := newTickerService(log, mockTracker, asynqClient, tasks)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go ticker.Start(ctx)

		// Wait for task to become due and execute (after ~1.5s)
		time.Sleep(2500 * time.Millisecond)

		cancel()
		ticker.Stop()

		// Task should have been enqueued at least once
		assert.GreaterOrEqual(t, enqueuedCount, 1,
			"Task should be enqueued when time advances past cached nextRun")
	})
}

func TestTickerService(t *testing.T) {
	// Start miniredis for all ticker tests
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	t.Run("ticker enqueues task when interval elapsed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		// Create Asynq client connected to miniredis
		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task that should run (last run was 2 seconds ago, interval is 1 second)
		taskID := "test:task1"
		lastRunTime := time.Now().Add(-2 * time.Second)

		// Expect GetLastRun to be called and return the old timestamp
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			Return(lastRunTime, nil).
			AnyTimes()

		// Expect SetLastRun to be called when task is enqueued
		setLastRunCalled := make(chan struct{}, 1)

		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ time.Time) error {
				select {
				case setLastRunCalled <- struct{}{}:
				default:
				}

				return nil
			}).
			AnyTimes()

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

		// Wait for SetLastRun to be called
		select {
		case <-setLastRunCalled:
			// Success - task was enqueued
		case <-time.After(3 * time.Second):
			t.Fatal("Task should have been enqueued and SetLastRun called")
		}

		// Stop ticker
		cancel()
		ticker.Stop()
	})

	t.Run("ticker does not enqueue task when interval not elapsed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task that should NOT run (last run was 500ms ago, interval is 1 minute)
		taskID := "test:task2"
		lastRunTime := time.Now().Add(-500 * time.Millisecond)

		// Expect GetLastRun to be called and return recent timestamp
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			Return(lastRunTime, nil).
			AnyTimes()

		// SetLastRun should NOT be called since interval hasn't elapsed
		// We use Times(0) to assert it's never called
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			Times(0)

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

		// Wait for ticker to run a few cycles
		time.Sleep(1500 * time.Millisecond)

		// Stop ticker
		cancel()
		ticker.Stop()
	})

	t.Run("ticker enqueues task on first run (zero time)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		// Task that has never run (zero time)
		taskID := "test:task3"

		// Expect GetLastRun to return zero time (never run before)
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			Return(time.Time{}, nil).
			AnyTimes()

		// Expect SetLastRun to be called when task is enqueued
		setLastRunCalled := make(chan struct{}, 1)

		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ time.Time) error {
				select {
				case setLastRunCalled <- struct{}{}:
				default:
				}

				return nil
			}).
			AnyTimes()

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

		// Wait for SetLastRun to be called
		select {
		case <-setLastRunCalled:
			// Success - task was enqueued on first run
		case <-time.After(3 * time.Second):
			t.Fatal("Task should be enqueued on first run")
		}

		// Stop ticker
		cancel()
		ticker.Stop()
	})

	t.Run("ticker stops gracefully on context cancel", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		taskID := "test:task4"

		// Allow any calls to tracker methods
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			Return(time.Time{}, nil).
			AnyTimes()
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			Return(nil).
			AnyTimes()

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
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

		asynqClient := asynq.NewClient(asynq.RedisClientOpt{
			Addr: mr.Addr(),
		})
		defer asynqClient.Close()

		taskID := "test:task5"

		// Allow any calls to tracker methods
		mockTracker.EXPECT().
			GetLastRun(gomock.Any(), taskID).
			Return(time.Time{}, nil).
			AnyTimes()
		mockTracker.EXPECT().
			SetLastRun(gomock.Any(), taskID, gomock.Any()).
			Return(nil).
			AnyTimes()

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
