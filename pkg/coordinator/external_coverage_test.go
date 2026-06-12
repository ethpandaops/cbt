package coordinator

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errMarshal forces the marshal error branch.
func errMarshalJSON(_ any) ([]byte, error) { return nil, errBoom }

// TestProcessExternalScanCoverage covers the incremental scan type, marshal
// error, and enqueue (non-conflict) error branches.
func TestProcessExternalScanCoverage(t *testing.T) {
	t.Run("incremental scan type enqueues", func(t *testing.T) {
		h := newCoordHarness(t)
		h.svc.ProcessExternalScan("db.model", tasks.ScanTypeIncremental)

		info, err := h.svc.inspector.GetTaskInfo("db.model", "external:db.model:incremental")
		require.NoError(t, err)
		assert.Equal(t, ExternalIncrementalTaskType, info.Type)
	})

	t.Run("marshal error is logged and returns", func(t *testing.T) {
		h := newCoordHarness(t)
		h.svc.marshalJSON = errMarshalJSON

		h.svc.ProcessExternalScan("db.model", tasks.ScanTypeFull)

		entry := h.hook.LastEntry()
		require.NotNil(t, entry)
		assert.Equal(t, logrus.ErrorLevel, entry.Level)
		assert.Contains(t, entry.Message, "Failed to marshal external scan task payload")
	})

	t.Run("non-conflict enqueue error is logged", func(t *testing.T) {
		h := newCoordHarness(t)
		// Close the underlying client so Enqueue fails with a non-conflict error.
		require.NoError(t, h.svc.queueManager.Close())

		h.svc.ProcessExternalScan("db.model", tasks.ScanTypeFull)

		var found bool
		for _, e := range h.hook.AllEntries() {
			if e.Level == logrus.ErrorLevel && e.Message == "Failed to enqueue external scan task" {
				found = true
			}
		}
		assert.True(t, found, "expected enqueue error log")
	})
}

// TestTriggerBoundsRefreshCoverage covers the marshal error, non-conflict
// enqueue error and successful enqueue branches.
func TestTriggerBoundsRefreshCoverage(t *testing.T) {
	t.Run("marshal error", func(t *testing.T) {
		h := newCoordHarness(t)
		h.svc.marshalJSON = errMarshalJSON

		err := h.svc.TriggerBoundsRefresh(context.Background(), "db.model")
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("non-conflict enqueue error", func(t *testing.T) {
		h := newCoordHarness(t)
		require.NoError(t, h.svc.queueManager.Close())

		err := h.svc.TriggerBoundsRefresh(context.Background(), "db.model")
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrRefreshInProgress)
	})

	t.Run("successful enqueue", func(t *testing.T) {
		h := newCoordHarness(t)
		require.NoError(t, h.svc.TriggerBoundsRefresh(context.Background(), "db.model"))
	})
}
