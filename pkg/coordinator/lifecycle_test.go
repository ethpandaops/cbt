package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPollCompletedTasksTick starts the poll loop with a short interval and waits
// for at least one tick, covering the ticker branch of pollCompletedTasks.
func TestPollCompletedTasksTick(t *testing.T) {
	h := newCoordHarness(t)
	h.svc.pollInterval = 5 * time.Millisecond

	// A transformation whose queue exists keeps checkCompletedTasks cheap.
	h.dag.Transformations = nil

	h.svc.wg.Add(1)
	go h.svc.pollCompletedTasks()

	// Give the ticker time to fire at least once.
	time.Sleep(50 * time.Millisecond)

	close(h.svc.done)
	h.svc.wg.Wait()
}

// TestMonitorArchiveTick starts the archive monitor with a short interval and
// waits for at least one tick, covering the ticker branch of monitorArchive.
func TestMonitorArchiveTick(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	asynqOpt := r.NewAsynqOptions(&redis.Options{Addr: mr.Addr()})
	insp := asynq.NewInspector(*asynqOpt)
	t.Cleanup(func() { _ = insp.Close() })

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	h := &archiveHandler{
		log:           log.WithField("service", "archive-handler"),
		inspector:     insp,
		done:          make(chan struct{}),
		checkInterval: 5 * time.Millisecond,
		batchSize:     100,
	}

	require.NoError(t, h.Start(context.Background()))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, h.Stop())
}

// TestStartNilArchiveFactoryFallback covers the nil-guard fallback in Start where
// newArchiveHandler is unset and defaults to NewArchiveHandler.
func TestStartNilArchiveFactoryFallback(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	// Construct the service directly so newArchiveHandler stays nil.
	svc := &service{
		log:          log.WithField("service", "coordinator"),
		redisOpt:     &redis.Options{Addr: mr.Addr()},
		dag:          &testutil.FakeDAGReader{},
		admin:        &adminfake.FakeAdminService{},
		validator:    validation.NewMockValidator(),
		done:         make(chan struct{}),
		taskCheck:    make(chan taskOperation),
		taskMark:     make(chan string, 100),
		marshalJSON:  nil,
		pollInterval: time.Hour, // avoid ticks during the test
	}

	require.NoError(t, svc.Start(context.Background()))
	require.NoError(t, svc.Stop())
}

// TestProcessArchivedTasksQueuesError covers the Queues() error branch in
// processArchivedTasks by using a closed inspector.
func TestProcessArchivedTasksQueuesError(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	asynqOpt := r.NewAsynqOptions(&redis.Options{Addr: mr.Addr()})
	insp := asynq.NewInspector(*asynqOpt)
	require.NoError(t, insp.Close()) // force Queues() to error

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	h := &archiveHandler{
		log:           log.WithField("service", "archive-handler"),
		inspector:     insp,
		done:          make(chan struct{}),
		checkInterval: time.Second,
		batchSize:     100,
	}

	assert.NotPanics(t, h.processArchivedTasks)
}
