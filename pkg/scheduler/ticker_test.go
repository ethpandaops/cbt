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
