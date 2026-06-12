package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestCheckAndEnqueuePositionWithTrigger drives the enqueue path against the real
// queue manager backed by miniredis, covering the dedup, validation-error,
// not-satisfied, and successful-enqueue branches.
func TestCheckAndEnqueuePositionWithTrigger(t *testing.T) {
	t.Run("validation error records error and returns", func(t *testing.T) {
		h := newCoordHarness(t)
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{}, errBoom
		}
		trans := &testutil.FakeTransformation{ID: "db.model"}
		h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))

		info, err := h.svc.queueManager.IsTaskPendingOrRunning(tasks.IncrementalPayload{
			ModelID: "db.model", Direction: string(DirectionForward),
		})
		require.NoError(t, err)
		assert.False(t, info, "no task should be enqueued on validation error")
	})

	t.Run("dependencies not satisfied does not enqueue", func(t *testing.T) {
		h := newCoordHarness(t)
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: false}, nil
		}
		trans := &testutil.FakeTransformation{ID: "db.model"}
		h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))

		pending, err := h.svc.queueManager.IsTaskPendingOrRunning(tasks.IncrementalPayload{
			ModelID: "db.model", Direction: string(DirectionForward),
		})
		require.NoError(t, err)
		assert.False(t, pending)
	})

	t.Run("successful enqueue", func(t *testing.T) {
		h := newCoordHarness(t)
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		trans := &testutil.FakeTransformation{ID: "db.model"}
		h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))

		pending, err := h.svc.queueManager.IsTaskPendingOrRunning(tasks.IncrementalPayload{
			ModelID: "db.model", Direction: string(DirectionForward),
		})
		require.NoError(t, err)
		assert.True(t, pending, "task should be pending after enqueue")
	})

	t.Run("already pending skips enqueue", func(t *testing.T) {
		h := newCoordHarness(t)
		validateCalls := 0
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			validateCalls++

			return validation.Result{CanProcess: true}, nil
		}
		trans := &testutil.FakeTransformation{ID: "db.model"}
		// First enqueue makes the task pending.
		h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))
		require.Equal(t, 1, validateCalls)

		// Second call should detect the pending task and short-circuit before validating.
		h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))
		assert.Equal(t, 1, validateCalls, "validation should be skipped when task already pending")
	})
}

// TestOnTaskComplete exercises the dependent-triggering logic including the
// continue branches for missing nodes, position errors, and start-position
// errors, plus the successful dependent enqueue path.
func TestOnTaskComplete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("nil payload returns early", func(t *testing.T) {
		h := newCoordHarness(t)
		assert.NotPanics(t, func() {
			h.svc.onTaskComplete(context.Background(), nil)
		})
	})

	t.Run("scheduled payload logs without position and no dependents", func(t *testing.T) {
		h := newCoordHarness(t)
		h.svc.onTaskComplete(context.Background(), tasks.ScheduledPayload{
			Type:    tasks.TypeScheduled,
			ModelID: "db.model",
		})
	})

	t.Run("dependent node missing is skipped", func(t *testing.T) {
		h := newCoordHarness(t)
		h.dag.Dependents = map[string][]string{"db.model": {"db.dep"}}
		// GetTransformationNode returns not-found for db.dep.
		h.svc.onTaskComplete(context.Background(), tasks.IncrementalPayload{
			Type:     tasks.TypeIncremental,
			ModelID:  "db.model",
			Position: 100,
		})
	})

	t.Run("dependent next position error is skipped", func(t *testing.T) {
		depHandler := backfillHandler(ctrl, 100, 0, nil, true, true)
		depTrans := &testutil.FakeTransformation{ID: "db.dep", Handler: depHandler}

		h := newCoordHarness(t)
		h.dag.Dependents = map[string][]string{"db.model": {"db.dep"}}
		h.dag.TransformationByID = map[string]models.Transformation{"db.dep": depTrans}
		h.admin.GetNextUnprocessedPositionFn = func(_ context.Context, _ string) (uint64, error) {
			return 0, errBoom
		}
		h.svc.onTaskComplete(context.Background(), tasks.IncrementalPayload{
			Type:     tasks.TypeIncremental,
			ModelID:  "db.model",
			Position: 100,
		})
	})

	t.Run("dependent start position error is skipped", func(t *testing.T) {
		depHandler := backfillHandler(ctrl, 100, 0, nil, true, true)
		depTrans := &testutil.FakeTransformation{ID: "db.dep", Handler: depHandler}

		h := newCoordHarness(t)
		h.dag.Dependents = map[string][]string{"db.model": {"db.dep"}}
		h.dag.TransformationByID = map[string]models.Transformation{"db.dep": depTrans}
		// next pos 0 -> compute start position -> error.
		h.validator.GetStartPositionFunc = func(_ context.Context, _ string) (uint64, error) {
			return 0, errBoom
		}
		h.svc.onTaskComplete(context.Background(), tasks.IncrementalPayload{
			Type:     tasks.TypeIncremental,
			ModelID:  "db.model",
			Position: 100,
		})
	})

	t.Run("dependent triggered with computed start position and plain handler", func(t *testing.T) {
		// Plain handler (no IntervalHandler) -> interval stays zero branch.
		depTrans := &testutil.FakeTransformation{ID: "db.dep", Handler: newPlainHandler(ctrl)}

		h := newCoordHarness(t)
		h.dag.Dependents = map[string][]string{"db.model": {"db.dep"}}
		h.dag.TransformationByID = map[string]models.Transformation{"db.dep": depTrans}
		h.validator.GetStartPositionFunc = func(_ context.Context, _ string) (uint64, error) {
			return 500, nil
		}
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		h.svc.onTaskComplete(context.Background(), tasks.IncrementalPayload{
			Type:     tasks.TypeIncremental,
			ModelID:  "db.model",
			Position: 100,
		})

		pending, err := h.svc.queueManager.IsTaskPendingOrRunning(tasks.IncrementalPayload{
			ModelID: "db.dep", Direction: string(DirectionForward),
		})
		require.NoError(t, err)
		assert.True(t, pending)
	})

	t.Run("dependent triggered with existing position and interval handler", func(t *testing.T) {
		depHandler := backfillHandler(ctrl, 200, 0, nil, true, true)
		depTrans := &testutil.FakeTransformation{ID: "db.dep2", Handler: depHandler}

		h := newCoordHarness(t)
		h.dag.Dependents = map[string][]string{"db.model": {"db.dep2"}}
		h.dag.TransformationByID = map[string]models.Transformation{"db.dep2": depTrans}
		h.admin.LastPositions = map[string]uint64{"db.dep2": 1000}
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		h.svc.onTaskComplete(context.Background(), tasks.IncrementalPayload{
			Type:     tasks.TypeIncremental,
			ModelID:  "db.model",
			Position: 100,
		})

		pending, err := h.svc.queueManager.IsTaskPendingOrRunning(tasks.IncrementalPayload{
			ModelID: "db.dep2", Direction: string(DirectionForward),
		})
		require.NoError(t, err)
		assert.True(t, pending)
	})
}

// TestCheckCompletedTasks seeds a real completed task in miniredis then runs the
// poller loop synchronously, covering the success path, the queue-error continue
// path, and the already-processed skip path.
func TestCheckCompletedTasks(t *testing.T) {
	t.Run("processes completed task and triggers dependents", func(t *testing.T) {
		h := newCoordHarness(t)
		h.startTaskTracker(t)

		trans := &testutil.FakeTransformation{ID: "db.model"}
		h.dag.Transformations = []models.Transformation{trans}

		payload, err := json.Marshal(tasks.IncrementalPayload{
			Type:       tasks.TypeIncremental,
			ModelID:    "db.model",
			Position:   100,
			Interval:   50,
			Direction:  string(DirectionForward),
			EnqueuedAt: time.Now(),
		})
		require.NoError(t, err)

		h.seedCompletedTask(t, "db.model", payload)

		// First pass processes the completed task.
		h.svc.checkCompletedTasks()

		// Second pass: the task is already processed, exercising the skip branch.
		h.svc.checkCompletedTasks()
	})

	t.Run("queue without completed tasks is skipped", func(t *testing.T) {
		h := newCoordHarness(t)
		h.startTaskTracker(t)

		// Transformation whose queue does not exist yet -> ListCompletedTasks errors.
		trans := &testutil.FakeTransformation{ID: "missing.queue"}
		h.dag.Transformations = []models.Transformation{trans}

		assert.NotPanics(t, h.svc.checkCompletedTasks)
	})
}

var _ = logrus.DebugLevel
