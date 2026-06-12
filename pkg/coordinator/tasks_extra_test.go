package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckAndEnqueueErrorBranches covers the IsTaskPendingOrRunning error and
// the EnqueueTransformation error branches.
func TestCheckAndEnqueueErrorBranches(t *testing.T) {
	t.Run("task status check error", func(t *testing.T) {
		h := newCoordHarness(t)
		// Closing the queue manager makes IsTaskPendingOrRunning error.
		require.NoError(t, h.svc.queueManager.Close())

		trans := &testutil.FakeTransformation{ID: "db.model"}
		assert.NotPanics(t, func() {
			h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))
		})
	})

	t.Run("enqueue error from invalid queue name", func(t *testing.T) {
		h := newCoordHarness(t)
		h.validator.ValidateDependenciesFunc = func(_ context.Context, _ string, _, _ uint64) (validation.Result, error) {
			return validation.Result{CanProcess: true}, nil
		}
		// Empty model ID => empty queue name => EnqueueTransformation errors while
		// IsTaskPendingOrRunning succeeds with (false, nil).
		trans := &testutil.FakeTransformation{ID: ""}
		assert.NotPanics(t, func() {
			h.svc.checkAndEnqueuePositionWithTrigger(context.Background(), trans, 100, 50, string(DirectionForward))
		})
	})
}

// TestCheckCompletedTasksInvalidPayload covers the json.Unmarshal error branch:
// a completed task with an unparsable payload must be marked processed without
// triggering dependents.
func TestCheckCompletedTasksInvalidPayload(t *testing.T) {
	h := newCoordHarness(t)
	h.startTaskTracker(t)

	trans := &testutil.FakeTransformation{ID: "db.model"}
	h.dag.Transformations = []models.Transformation{trans}

	// Seed a completed task whose payload is not valid Payload JSON.
	h.seedCompletedTask(t, "db.model", []byte("not-json"))

	assert.NotPanics(t, h.svc.checkCompletedTasks)
}

// TestCheckCompletedTasksNullPayload covers the branch where json.Unmarshal
// succeeds with a nil payload (the JSON literal null), reaching onTaskComplete.
func TestCheckCompletedTasksNullPayload(t *testing.T) {
	h := newCoordHarness(t)
	h.startTaskTracker(t)

	trans := &testutil.FakeTransformation{ID: "db.model"}
	h.dag.Transformations = []models.Transformation{trans}

	// A null payload unmarshals into a nil tasks.Payload without error.
	h.seedCompletedTask(t, "db.model", []byte("null"))

	assert.NotPanics(t, h.svc.checkCompletedTasks)
}

// TestMarkTaskProcessedDoneBranch covers the markTaskProcessed shutdown branch by
// filling the buffer and closing done so the send cannot proceed.
func TestMarkTaskProcessedDoneBranch(t *testing.T) {
	h := newCoordHarness(t)

	// Fill the buffered taskMark channel (capacity 100) so a further send blocks.
	for range cap(h.svc.taskMark) {
		h.svc.taskMark <- "x"
	}

	close(h.svc.done)

	// With the buffer full and done closed, markTaskProcessed selects the done case.
	assert.NotPanics(t, func() {
		h.svc.markTaskProcessed("overflow")
	})
}

// TestIsTaskProcessedDoneBranch covers the isTaskProcessed shutdown branch on the
// send to taskCheck (no tracker running, done closed).
func TestIsTaskProcessedDoneBranch(t *testing.T) {
	h := newCoordHarness(t)

	close(h.svc.done)

	// taskCheck is unbuffered and no tracker is running, so the send blocks and the
	// done case is selected, returning false.
	assert.False(t, h.svc.isTaskProcessed("anything"))
}

// TestIsTaskProcessedDoneAfterSend covers the isTaskProcessed branch where the
// check operation is delivered but the tracker shuts down before responding, so
// the caller selects the done case while awaiting the response.
func TestIsTaskProcessedDoneAfterSend(t *testing.T) {
	h := newCoordHarness(t)

	// A drainer consumes the operation without responding, then closes done so the
	// caller's inner select falls through to the done case.
	go func() {
		<-h.svc.taskCheck
		close(h.svc.done)
	}()

	assert.False(t, h.svc.isTaskProcessed("x"))
}

// TestTaskTrackerResponseDoneBranch covers the taskTracker inner select where the
// response send blocks (unbuffered, unread response channel) and done is closed,
// so the tracker returns via the done case.
func TestTaskTrackerResponseDoneBranch(t *testing.T) {
	h := newCoordHarness(t)

	finished := make(chan struct{})
	h.svc.wg.Add(1)
	go func() {
		h.svc.taskTracker()
		close(finished)
	}()

	// Unbuffered, never-read response channel: the tracker's send will block.
	op := taskOperation{taskID: "x", response: make(chan bool)}
	h.svc.taskCheck <- op

	// Closing done lets the tracker's inner select choose the done case.
	close(h.svc.done)

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("taskTracker did not return via response done branch")
	}
}

// TestTaskTrackerShutdownViaDone covers the taskTracker top-level done return.
func TestTaskTrackerShutdownViaDone(t *testing.T) {
	h := newCoordHarness(t)

	done := make(chan struct{})
	h.svc.wg.Add(1)
	go func() {
		h.svc.taskTracker()
		close(done)
	}()

	close(h.svc.done)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("taskTracker did not return after done closed")
	}
}
