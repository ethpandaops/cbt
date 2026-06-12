package coordinator

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// stubArchiveHandler is a controllable ArchiveHandler for exercising the
// service's start/stop error paths.
type stubArchiveHandler struct {
	startErr error
	stopErr  error
}

func (s *stubArchiveHandler) Start(_ context.Context) error { return s.startErr }
func (s *stubArchiveHandler) Stop() error                   { return s.stopErr }

var _ ArchiveHandler = (*stubArchiveHandler)(nil)

// newServiceForStart builds a service wired to miniredis with an injectable
// archive-handler factory.
func newServiceForStart(t *testing.T) *service {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	svc, err := NewService(
		log,
		&redis.Options{Addr: mr.Addr()},
		&testutil.FakeDAGReader{},
		&adminfake.FakeAdminService{},
		validation.NewMockValidator(),
	)
	require.NoError(t, err)

	return svc.(*service)
}

// TestStartArchiveHandlerCreateError covers the create-error branch in Start.
func TestStartArchiveHandlerCreateError(t *testing.T) {
	svc := newServiceForStart(t)
	svc.newArchiveHandler = func(_ logrus.FieldLogger, _ *redis.Options) (ArchiveHandler, error) {
		return nil, errBoom
	}

	err := svc.Start(context.Background())
	require.ErrorIs(t, err, errBoom)

	// Background goroutines were not started; clean up the queue resources.
	_ = svc.inspector.Close()
	_ = svc.queueManager.Close()
}

// TestStartArchiveHandlerStartError covers the start-error branch in Start.
func TestStartArchiveHandlerStartError(t *testing.T) {
	svc := newServiceForStart(t)
	svc.newArchiveHandler = func(_ logrus.FieldLogger, _ *redis.Options) (ArchiveHandler, error) {
		return &stubArchiveHandler{startErr: errBoom}, nil
	}

	err := svc.Start(context.Background())
	require.ErrorIs(t, err, errBoom)

	_ = svc.inspector.Close()
	_ = svc.queueManager.Close()
}

// TestStopAggregatesErrors covers the archive-stop, inspector-close,
// queue-manager-close and combined-error formatting branches in Stop.
func TestStopAggregatesErrors(t *testing.T) {
	h := newCoordHarness(t)
	// Stop closes s.done, so do not rely on cleanup that also closes it.
	h.svc.archiveHandler = &stubArchiveHandler{stopErr: errBoom}

	// Pre-close inspector and queue manager so their Close calls in Stop error.
	require.NoError(t, h.svc.inspector.Close())
	require.NoError(t, h.svc.queueManager.Close())

	err := h.svc.Stop()
	require.ErrorIs(t, err, ErrShutdownErrors)
}

// TestStopNoErrors covers the clean shutdown path where every component
// shuts down without error.
func TestStopNoErrors(t *testing.T) {
	h := newCoordHarness(t)
	h.svc.archiveHandler = &stubArchiveHandler{}

	require.NoError(t, h.svc.Stop())
}
