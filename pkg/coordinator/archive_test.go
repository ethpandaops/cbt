package coordinator

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newArchiveHarness builds an archiveHandler with an inspector and client wired
// to a fresh miniredis instance.
func newArchiveHarness(t *testing.T) (*archiveHandler, *asynq.Inspector, *asynq.Client) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	asynqOpt := r.NewAsynqOptions(&redis.Options{Addr: mr.Addr()})

	insp := asynq.NewInspector(*asynqOpt)
	client := asynq.NewClient(*asynqOpt)
	t.Cleanup(func() {
		_ = insp.Close()
		_ = client.Close()
	})

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	h := &archiveHandler{
		log:           log.WithField("service", "archive-handler"),
		inspector:     insp,
		done:          make(chan struct{}),
		checkInterval: time.Second,
		batchSize:     100,
	}

	return h, insp, client
}

// TestProcessArchivedTasks covers the no-queues, archived-tasks-found, and
// empty-queue branches of processArchivedTasks.
func TestProcessArchivedTasks(t *testing.T) {
	t.Run("no queues is a no-op", func(t *testing.T) {
		h, _, _ := newArchiveHarness(t)
		assert.NotPanics(t, h.processArchivedTasks)
	})

	t.Run("archived tasks are deleted", func(t *testing.T) {
		h, insp, client := newArchiveHarness(t)

		// Enqueue then archive a task so it appears in the archived set.
		info, err := client.Enqueue(
			asynq.NewTask("model:transformation", []byte(`{"model_id":"db.model"}`)),
			asynq.Queue("db.model"),
			asynq.TaskID("db.model:archived"),
			asynq.MaxRetry(0),
		)
		require.NoError(t, err)

		require.NoError(t, insp.ArchiveTask("db.model", info.ID))

		archived, err := insp.ListArchivedTasks("db.model", asynq.PageSize(100))
		require.NoError(t, err)
		require.Len(t, archived, 1)

		h.processArchivedTasks()

		// After processing, the archived task should be deleted.
		archived, err = insp.ListArchivedTasks("db.model", asynq.PageSize(100))
		require.NoError(t, err)
		assert.Empty(t, archived)
	})

	t.Run("queue with no archived tasks is skipped", func(t *testing.T) {
		h, _, client := newArchiveHarness(t)

		// Enqueue a pending task so the queue exists but has no archived tasks.
		_, err := client.Enqueue(
			asynq.NewTask("model:transformation", []byte(`{"model_id":"db.empty"}`)),
			asynq.Queue("db.empty"),
			asynq.TaskID("db.empty:pending"),
			asynq.MaxRetry(0),
		)
		require.NoError(t, err)

		assert.NotPanics(t, h.processArchivedTasks)
	})
}

// fakeArchiveInspector is a controllable archiveInspector for exercising the
// list-error continue path.
type fakeArchiveInspector struct {
	queues     []string
	queuesErr  error
	listErr    error
	listResult []*asynq.TaskInfo
	deleted    []string
	deleteErr  error
}

func (f *fakeArchiveInspector) Queues() ([]string, error) { return f.queues, f.queuesErr }

func (f *fakeArchiveInspector) ListArchivedTasks(_ string, _ ...asynq.ListOption) ([]*asynq.TaskInfo, error) {
	return f.listResult, f.listErr
}

func (f *fakeArchiveInspector) DeleteTask(_, id string) error {
	f.deleted = append(f.deleted, id)

	return f.deleteErr
}

var _ archiveInspector = (*fakeArchiveInspector)(nil)

// TestProcessArchivedTasksListError covers the ListArchivedTasks error continue
// branch using a fake inspector.
func TestProcessArchivedTasksListError(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	h := &archiveHandler{
		log:           log.WithField("service", "archive-handler"),
		inspector:     &fakeArchiveInspector{queues: []string{"db.model"}, listErr: errBoom},
		done:          make(chan struct{}),
		checkInterval: time.Second,
		batchSize:     100,
	}

	assert.NotPanics(t, h.processArchivedTasks)
}

// TestProcessArchivedTask covers the delete-success and delete-error branches.
func TestProcessArchivedTask(t *testing.T) {
	t.Run("delete success", func(t *testing.T) {
		h, insp, client := newArchiveHarness(t)

		info, err := client.Enqueue(
			asynq.NewTask("model:transformation", []byte(`{"model_id":"db.model"}`)),
			asynq.Queue("db.model"),
			asynq.TaskID("db.model:archived"),
			asynq.MaxRetry(0),
		)
		require.NoError(t, err)
		require.NoError(t, insp.ArchiveTask("db.model", info.ID))

		archived, err := insp.ListArchivedTasks("db.model", asynq.PageSize(100))
		require.NoError(t, err)
		require.Len(t, archived, 1)

		h.processArchivedTask("db.model", archived[0])

		got, err := insp.ListArchivedTasks("db.model", asynq.PageSize(100))
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("delete error is logged", func(t *testing.T) {
		h, _, _ := newArchiveHarness(t)

		// A TaskInfo referencing a non-existent queue/task forces DeleteTask to error.
		taskInfo := &asynq.TaskInfo{
			ID:    "does-not-exist",
			Type:  "model:transformation",
			Queue: "no.such.queue",
		}
		assert.NotPanics(t, func() {
			h.processArchivedTask("no.such.queue", taskInfo)
		})
	})
}

var _ = logrus.DebugLevel
