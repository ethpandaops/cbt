package tasks

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errMarshal is returned by unmarshalablePayload to force a marshal failure.
var errMarshal = errors.New("marshal failure")

// unmarshalablePayload implements Payload but fails to JSON-marshal, exercising the
// error branch in EnqueueTransformation.
type unmarshalablePayload struct {
	modelID string
}

func (p unmarshalablePayload) GetModelID() string           { return p.modelID }
func (p unmarshalablePayload) GetEnqueuedAt() time.Time     { return time.Time{} }
func (p unmarshalablePayload) GetType() Type                { return TypeIncremental }
func (p unmarshalablePayload) UniqueID() string             { return p.modelID + ":forward" }
func (p unmarshalablePayload) QueueName() string            { return p.modelID }
func (p unmarshalablePayload) MarshalJSON() ([]byte, error) { return nil, errMarshal }

var _ json.Marshaler = unmarshalablePayload{}

// newTestQueueManager builds a QueueManager backed by an in-memory miniredis server.
func newTestQueueManager(t *testing.T) *QueueManager {
	t.Helper()

	mr := miniredis.RunT(t)
	redisOpt := &asynq.RedisClientOpt{Addr: mr.Addr()}
	qm := NewQueueManager(redisOpt)

	t.Cleanup(func() {
		if err := qm.Close(); err != nil {
			t.Logf("failed to close queue manager: %v", err)
		}
	})

	return qm
}

func TestNewQueueManager(t *testing.T) {
	mr := miniredis.RunT(t)
	redisOpt := &asynq.RedisClientOpt{Addr: mr.Addr()}

	qm := NewQueueManager(redisOpt)
	require.NotNil(t, qm)
	assert.NotNil(t, qm.client)
	assert.NotNil(t, qm.inspector)

	require.NoError(t, qm.Close())
}

func TestQueueManager_EnqueueTransformation(t *testing.T) {
	qm := newTestQueueManager(t)

	payload := IncrementalPayload{
		Type:       TypeIncremental,
		ModelID:    "model.test",
		Position:   100,
		Interval:   50,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	err := qm.EnqueueTransformation(payload)
	require.NoError(t, err)

	// Verify the task is now pending.
	pending, err := qm.IsTaskPendingOrRunning(payload)
	require.NoError(t, err)
	assert.True(t, pending)
}

func TestQueueManager_EnqueueTransformation_WithOptions(t *testing.T) {
	qm := newTestQueueManager(t)

	payload := ScheduledPayload{
		Type:          TypeScheduled,
		ModelID:       "model.scheduled",
		ExecutionTime: time.Now(),
		EnqueuedAt:    time.Now(),
	}

	// Pass an extra option to exercise the append path.
	err := qm.EnqueueTransformation(payload, asynq.MaxRetry(5))
	require.NoError(t, err)

	pending, err := qm.IsTaskPendingOrRunning(payload)
	require.NoError(t, err)
	assert.True(t, pending)
}

func TestQueueManager_EnqueueTransformation_MarshalError(t *testing.T) {
	qm := newTestQueueManager(t)

	err := qm.EnqueueTransformation(unmarshalablePayload{modelID: "model.bad"})
	require.ErrorIs(t, err, errMarshal)
	assert.Contains(t, err.Error(), "failed to marshal transformation payload")
}

func TestQueueManager_EnqueueTransformation_DuplicateError(t *testing.T) {
	qm := newTestQueueManager(t)

	payload := IncrementalPayload{
		Type:       TypeIncremental,
		ModelID:    "model.dup",
		Position:   100,
		Interval:   50,
		Direction:  DirectionForward,
		EnqueuedAt: time.Now(),
	}

	require.NoError(t, qm.EnqueueTransformation(payload))

	// Enqueuing the same unique task ID again should fail with a wrapped error.
	err := qm.EnqueueTransformation(payload)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue transformation task")
}

func TestQueueManager_IsTaskPendingOrRunning(t *testing.T) {
	t.Run("task not found returns false", func(t *testing.T) {
		qm := newTestQueueManager(t)

		payload := IncrementalPayload{
			Type:      TypeIncremental,
			ModelID:   "model.missing",
			Direction: DirectionForward,
		}

		pending, err := qm.IsTaskPendingOrRunning(payload)
		require.NoError(t, err)
		assert.False(t, pending)
	})

	t.Run("queue not found returns false", func(t *testing.T) {
		qm := newTestQueueManager(t)

		// A model that has never had a queue created returns ErrQueueNotFound,
		// which is treated as "not pending".
		payload := IncrementalPayload{
			Type:      TypeIncremental,
			ModelID:   "queue.never.created",
			Direction: DirectionBack,
		}

		pending, err := qm.IsTaskPendingOrRunning(payload)
		require.NoError(t, err)
		assert.False(t, pending)
	})

	t.Run("pending task returns true", func(t *testing.T) {
		qm := newTestQueueManager(t)

		payload := IncrementalPayload{
			Type:      TypeIncremental,
			ModelID:   "model.pending",
			Position:  10,
			Interval:  5,
			Direction: DirectionForward,
		}

		require.NoError(t, qm.EnqueueTransformation(payload))

		pending, err := qm.IsTaskPendingOrRunning(payload)
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("inspector error returns wrapped error", func(t *testing.T) {
		mr := miniredis.RunT(t)
		redisOpt := &asynq.RedisClientOpt{Addr: mr.Addr()}
		qm := NewQueueManager(redisOpt)

		// Enqueue so the queue exists, then close the underlying redis to force an error.
		payload := IncrementalPayload{
			Type:      TypeIncremental,
			ModelID:   "model.err",
			Direction: DirectionForward,
		}
		require.NoError(t, qm.EnqueueTransformation(payload))

		mr.Close() // Closing miniredis forces inspector calls to error.

		_, err := qm.IsTaskPendingOrRunning(payload)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get task info")

		_ = qm.Close()
	})
}

func TestQueueManager_Enqueue(t *testing.T) {
	qm := newTestQueueManager(t)

	payload := ScheduledPayload{
		Type:    TypeScheduled,
		ModelID: "model.generic",
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	task := asynq.NewTask(TypeModelTransformation, data)
	info, err := qm.Enqueue(task, asynq.Queue("model.generic"))
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "model.generic", info.Queue)
}

func TestQueueManager_Close(t *testing.T) {
	t.Run("close succeeds", func(t *testing.T) {
		mr := miniredis.RunT(t)
		redisOpt := &asynq.RedisClientOpt{Addr: mr.Addr()}
		qm := NewQueueManager(redisOpt)

		require.NoError(t, qm.Close())
	})

	t.Run("double close returns joined errors", func(t *testing.T) {
		mr := miniredis.RunT(t)
		redisOpt := &asynq.RedisClientOpt{Addr: mr.Addr()}
		qm := NewQueueManager(redisOpt)

		require.NoError(t, qm.Close())

		// Second close should report errors for both already-closed client and inspector.
		err := qm.Close()
		require.Error(t, err)
	})
}
