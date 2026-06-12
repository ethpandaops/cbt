package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	transformationmock "github.com/ethpandaops/cbt/pkg/models/transformation/mock"
)

// newPlainHandler returns a Handler mock that implements only transformation.Handler
// (not IntervalHandler or ScheduleHandler), so the coordinator's optional-interface
// type assertions fail. The paths under test only perform type assertions against
// this handler, so no method expectations are required.
func newPlainHandler(ctrl *gomock.Controller) *transformationmock.MockHandler {
	return transformationmock.NewMockHandler(ctrl)
}

// coordHarness wires a *service with a real asynq queue manager and inspector
// backed by miniredis, but without launching the background goroutines started
// by Start. This keeps tests fully deterministic while still exercising the
// real queue/inspector code paths against an in-memory Redis.
type coordHarness struct {
	svc       *service
	mr        *miniredis.Miniredis
	asynqOpt  *asynq.RedisClientOpt
	dag       *testutil.FakeDAGReader
	admin     *adminfake.FakeAdminService
	validator *validation.MockValidator
	hook      *logrustest.Hook
}

// newCoordHarness builds a coordinator service backed by miniredis with the
// queue manager and inspector initialized, ready to drive the unexported
// methods directly.
func newCoordHarness(t *testing.T) *coordHarness {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	redisOpt := &redis.Options{Addr: mr.Addr()}
	asynqOpt := r.NewAsynqOptions(redisOpt)

	dag := &testutil.FakeDAGReader{NodeByID: make(map[string]models.Node)}
	adminSvc := &adminfake.FakeAdminService{LastPositions: make(map[string]uint64)}
	validator := validation.NewMockValidator()

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	hook := logrustest.NewLocal(log)

	svc := &service{
		log:          log.WithField("service", "coordinator"),
		redisOpt:     redisOpt,
		dag:          dag,
		admin:        adminSvc,
		validator:    validator,
		done:         make(chan struct{}),
		taskCheck:    make(chan taskOperation),
		taskMark:     make(chan string, 100),
		queueManager: tasks.NewQueueManager(asynqOpt),
		inspector:    asynq.NewInspector(*asynqOpt),
		marshalJSON:  json.Marshal,
	}

	t.Cleanup(func() {
		if svc.inspector != nil {
			_ = svc.inspector.Close()
		}
		if svc.queueManager != nil {
			_ = svc.queueManager.Close()
		}
	})

	return &coordHarness{
		svc:       svc,
		mr:        mr,
		asynqOpt:  asynqOpt,
		dag:       dag,
		admin:     adminSvc,
		validator: validator,
		hook:      hook,
	}
}

// startTaskTracker launches the channel-based task tracker goroutine and
// arranges for it to be stopped on test cleanup.
func (h *coordHarness) startTaskTracker(t *testing.T) {
	t.Helper()

	h.svc.wg.Add(1)
	go h.svc.taskTracker()

	t.Cleanup(func() {
		close(h.svc.done)
		h.svc.wg.Wait()
	})
}

// seedCompletedTask enqueues a transformation task and runs a short-lived asynq
// server to process it to completion (retained), then shuts the server down so
// the harness can synchronously inspect the completed task. The payload is the
// JSON-encoded body for a model:transformation task.
func (h *coordHarness) seedCompletedTask(t *testing.T, queue string, payload []byte) {
	t.Helper()

	client := asynq.NewClient(*h.asynqOpt)
	defer client.Close()

	task := asynq.NewTask(tasks.TypeModelTransformation, payload)
	_, err := client.Enqueue(task,
		asynq.Queue(queue),
		asynq.Retention(5*time.Minute),
		asynq.MaxRetry(0),
	)
	require.NoError(t, err)

	srv := asynq.NewServer(*h.asynqOpt, asynq.Config{
		Concurrency: 1,
		Queues:      map[string]int{queue: 1},
		LogLevel:    asynq.FatalLevel,
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.TypeModelTransformation, func(_ context.Context, _ *asynq.Task) error {
		return nil
	})
	require.NoError(t, srv.Start(mux))

	insp := asynq.NewInspector(*h.asynqOpt)
	defer insp.Close()

	deadline := time.Now().Add(15 * time.Second)
	got := false
	for time.Now().Before(deadline) {
		h.mr.FastForward(time.Second)
		completed, lerr := insp.ListCompletedTasks(queue, asynq.PageSize(100))
		if lerr == nil && len(completed) > 0 {
			got = true

			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	srv.Shutdown()
	require.True(t, got, "completed task did not appear in time")
}
