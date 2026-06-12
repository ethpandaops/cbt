package coordinator

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMiniredisCoordinator builds and starts a coordinator service backed by miniredis.
func newMiniredisCoordinator(t *testing.T, log logrus.FieldLogger) Service {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	redisOpt := &redis.Options{Addr: mr.Addr()}

	mockDAG := &testutil.FakeDAGReader{NodeByID: make(map[string]models.Node)}
	mockAdmin := &adminfake.FakeAdminService{LastPositions: make(map[string]uint64)}
	mockValidator := validation.NewMockValidator()

	svc, err := NewService(log, redisOpt, mockDAG, mockAdmin, mockValidator)
	require.NoError(t, err)

	require.NoError(t, svc.Start(context.Background()))
	t.Cleanup(func() { _ = svc.Stop() })

	return svc
}

// A second refresh while one is still queued must surface as ErrRefreshInProgress.
// asynq reports the duplicate via asynq.ErrTaskIDConflict.
func TestTriggerBoundsRefreshReturnsErrRefreshInProgressOnDuplicate(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	svc := newMiniredisCoordinator(t, log)

	ctx := context.Background()
	require.NoError(t, svc.TriggerBoundsRefresh(ctx, "db.model"))

	err := svc.TriggerBoundsRefresh(ctx, "db.model")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRefreshInProgress)
}

// Re-enqueueing a scan that is already queued is expected during normal
// operation and must not be logged at error level.
func TestProcessExternalScanDuplicateIsNotLoggedAsError(t *testing.T) {
	log, hook := logrustest.NewNullLogger()

	svc := newMiniredisCoordinator(t, log)

	svc.ProcessExternalScan("db.model", "full")
	svc.ProcessExternalScan("db.model", "full")

	for _, entry := range hook.AllEntries() {
		assert.NotEqual(t, logrus.ErrorLevel, entry.Level,
			"duplicate external scan logged as error: %s", entry.Message)
	}
}
