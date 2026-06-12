package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	schedulermock "github.com/ethpandaops/cbt/pkg/scheduler/mock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// UpdateTasks is called from the live-override poller while the ticker loop
// is iterating the task list, so the two must be safe to run concurrently.
// This test only proves the invariant under the race detector (go test -race).
func TestTickerUpdateTasksConcurrentWithCheckSchedules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	mockTracker := schedulermock.NewMockscheduleTracker(ctrl)
	// Last run is "now" with hour-long intervals, so no task is ever due and
	// the nil queue client is never touched.
	mockTracker.EXPECT().
		GetLastRun(gomock.Any(), gomock.Any()).
		Return(time.Now(), nil).
		AnyTimes()

	tk, ok := newTickerService(log, mockTracker, nil, tickerConcurrencyTasks(4)).(*tickerServiceImpl)
	require.True(t, ok)

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
				tk.checkSchedules(context.Background())
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer close(stop)

		for range 500 {
			tk.UpdateTasks(tickerConcurrencyTasks(4))
		}
	}()

	wg.Wait()
}

func tickerConcurrencyTasks(n int) []scheduledTask {
	tasks := make([]scheduledTask, 0, n)
	for i := range n {
		tasks = append(tasks, scheduledTask{
			ID:       fmt.Sprintf("transformation:db.race_%d:forward", i),
			Schedule: "@every 1h",
			Interval: time.Hour,
		})
	}

	return tasks
}
