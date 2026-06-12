package coordinator

import (
	"context"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// scheduledTrans builds a FakeTransformation with the given config type.
func scheduledTrans(id string, cfgType transformation.Type) *testutil.FakeTransformation {
	return &testutil.FakeTransformation{
		ID:     id,
		Config: transformation.Config{Type: cfgType, Database: "db", Table: "model"},
	}
}

// TestTriggerScheduledRun covers every branch of TriggerScheduledRun.
func TestTriggerScheduledRun(t *testing.T) {
	t.Run("get node error", func(t *testing.T) {
		h := newCoordHarness(t)
		h.dag.GetNodeErr = errBoom

		err := h.svc.TriggerScheduledRun(context.Background(), "db.model")
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("non-transformation node returns ErrNotScheduledModel", func(t *testing.T) {
		h := newCoordHarness(t)
		h.dag.NodeByID["db.ext"] = models.Node{NodeType: models.NodeTypeExternal}

		err := h.svc.TriggerScheduledRun(context.Background(), "db.ext")
		require.ErrorIs(t, err, ErrNotScheduledModel)
	})

	t.Run("get transformation node error", func(t *testing.T) {
		h := newCoordHarness(t)
		h.dag.NodeByID["db.model"] = models.Node{NodeType: models.NodeTypeTransformation}
		h.dag.GetTransformationErr = errBoom

		err := h.svc.TriggerScheduledRun(context.Background(), "db.model")
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("non-scheduled type returns ErrNotScheduledModel", func(t *testing.T) {
		h := newCoordHarness(t)
		trans := scheduledTrans("db.model", transformation.TypeIncremental)
		h.dag.NodeByID["db.model"] = models.Node{NodeType: models.NodeTypeTransformation, Model: trans}

		err := h.svc.TriggerScheduledRun(context.Background(), "db.model")
		require.ErrorIs(t, err, ErrNotScheduledModel)
	})

	t.Run("nil queue manager returns ErrQueueManagerNil", func(t *testing.T) {
		h := newCoordHarness(t)
		trans := scheduledTrans("db.model", transformation.TypeScheduled)
		h.dag.NodeByID["db.model"] = models.Node{NodeType: models.NodeTypeTransformation, Model: trans}
		h.svc.queueManager = nil

		err := h.svc.TriggerScheduledRun(context.Background(), "db.model")
		require.ErrorIs(t, err, ErrQueueManagerNil)
	})

	t.Run("duplicate enqueue returns ErrScheduledRunInProgress", func(t *testing.T) {
		h := newCoordHarness(t)
		trans := scheduledTrans("db.model", transformation.TypeScheduled)
		h.dag.NodeByID["db.model"] = models.Node{NodeType: models.NodeTypeTransformation, Model: trans}

		require.NoError(t, h.svc.TriggerScheduledRun(context.Background(), "db.model"))

		err := h.svc.TriggerScheduledRun(context.Background(), "db.model")
		require.ErrorIs(t, err, ErrScheduledRunInProgress)
	})

	t.Run("non-conflict enqueue error wrapped", func(t *testing.T) {
		h := newCoordHarness(t)
		trans := scheduledTrans("db.model", transformation.TypeScheduled)
		h.dag.NodeByID["db.model"] = models.Node{NodeType: models.NodeTypeTransformation, Model: trans}
		require.NoError(t, h.svc.queueManager.Close())

		err := h.svc.TriggerScheduledRun(context.Background(), "db.model")
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrScheduledRunInProgress)
		require.NotErrorIs(t, err, ErrQueueManagerNil)
	})

	t.Run("successful enqueue", func(t *testing.T) {
		h := newCoordHarness(t)
		trans := scheduledTrans("db.model2", transformation.TypeScheduled)
		h.dag.NodeByID["db.model2"] = models.Node{NodeType: models.NodeTypeTransformation, Model: trans}

		require.NoError(t, h.svc.TriggerScheduledRun(context.Background(), "db.model2"))
	})
}

var _ = logrus.DebugLevel
