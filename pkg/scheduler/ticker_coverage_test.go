package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	schedulermock "github.com/ethpandaops/cbt/pkg/scheduler/mock"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var errTrackerBoom = errors.New("tracker boom")

func newTickerImpl(t *testing.T, tracker scheduleTracker, client *asynq.Client, tasks []scheduledTask) *tickerServiceImpl {
	t.Helper()

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	tk, ok := newTickerService(log, tracker, client, tasks).(*tickerServiceImpl)
	require.True(t, ok)

	return tk
}

// TestCheckSchedulesGetLastRunError covers the branch in checkSchedules where
// GetLastRun fails and the task is skipped (warn + continue).
func TestCheckSchedulesGetLastRunError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracker := schedulermock.NewMockscheduleTracker(ctrl)
	mockTracker.EXPECT().
		GetLastRun(gomock.Any(), "task:err").
		Return(time.Time{}, errTrackerBoom).
		Times(1)
	// SetLastRun must never be reached when GetLastRun fails.
	mockTracker.EXPECT().SetLastRun(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	tasks := []scheduledTask{
		{
			ID:       "task:err",
			Schedule: "@every 1s",
			Interval: time.Second,
			Task:     asynq.NewTask("task:err", nil),
			Queue:    QueueName,
		},
	}

	// Nil queue client is safe: GetLastRun fails before any enqueue.
	tk := newTickerImpl(t, mockTracker, nil, tasks)

	tk.checkSchedules(context.Background())
}

// TestCheckSchedulesEnqueueError covers the branch where enqueueTask returns a
// non-conflict error: the task is logged and SetLastRun is not called.
func TestCheckSchedulesEnqueueError(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracker := schedulermock.NewMockscheduleTracker(ctrl)
	// Zero last-run time means the task is immediately due.
	mockTracker.EXPECT().
		GetLastRun(gomock.Any(), "task:enqfail").
		Return(time.Time{}, nil).
		AnyTimes()
	// SetLastRun must not be called because enqueue fails.
	mockTracker.EXPECT().SetLastRun(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	// A closed asynq client makes EnqueueContext fail.
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: mr.Addr()})
	require.NoError(t, client.Close())

	tasks := []scheduledTask{
		{
			ID:       "task:enqfail",
			Schedule: "@every 1s",
			Interval: time.Second,
			Task:     asynq.NewTask("task:enqfail", nil),
			Queue:    QueueName,
		},
	}

	tk := newTickerImpl(t, mockTracker, client, tasks)

	tk.checkSchedules(context.Background())
}

// TestCheckSchedulesSetLastRunError covers the path where the task enqueues
// successfully but SetLastRun fails (logged error, then nextRun still updated).
func TestCheckSchedulesSetLastRunError(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracker := schedulermock.NewMockscheduleTracker(ctrl)
	mockTracker.EXPECT().
		GetLastRun(gomock.Any(), "task:setfail").
		Return(time.Time{}, nil).
		Times(1)
	mockTracker.EXPECT().
		SetLastRun(gomock.Any(), "task:setfail", gomock.Any()).
		Return(errTrackerBoom).
		Times(1)

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	tasks := []scheduledTask{
		{
			ID:       "task:setfail",
			Schedule: "@every 1s",
			Interval: time.Second,
			Task:     asynq.NewTask("task:setfail", nil),
			Queue:    QueueName,
		},
	}

	tk := newTickerImpl(t, mockTracker, client, tasks)

	tk.checkSchedules(context.Background())
}

// TestEnqueueTaskTaskIDConflict covers the ErrTaskIDConflict branch of
// enqueueTask: a duplicate task ID returns nil (skip) rather than an error.
func TestEnqueueTaskTaskIDConflict(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	task := scheduledTask{
		ID:       "task:dup",
		Schedule: "@every 1s",
		Interval: time.Second,
		Task:     asynq.NewTask("task:dup", nil),
		Queue:    QueueName,
	}

	tk := newTickerImpl(t, mockTracker, client, []scheduledTask{task})

	now := time.Now().UTC()

	// First enqueue succeeds.
	require.NoError(t, tk.enqueueTask(context.Background(), task, now))

	// Second enqueue with the same task ID conflicts; enqueueTask swallows it.
	require.NoError(t, tk.enqueueTask(context.Background(), task, now))
}

// TestEnqueueTaskError covers the generic error wrap branch of enqueueTask by
// using a closed asynq client.
func TestEnqueueTaskError(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTracker := schedulermock.NewMockscheduleTracker(ctrl)

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: mr.Addr()})
	require.NoError(t, client.Close())

	task := scheduledTask{
		ID:       "task:enqerr",
		Schedule: "@every 1s",
		Interval: time.Second,
		Task:     asynq.NewTask("task:enqerr", nil),
		Queue:    QueueName,
	}

	tk := newTickerImpl(t, mockTracker, client, []scheduledTask{task})

	err = tk.enqueueTask(context.Background(), task, time.Now().UTC())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to enqueue task")
}
