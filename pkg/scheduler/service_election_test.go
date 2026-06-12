package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errFake = errors.New("fake error")

// fakeElector is a controllable LeaderElector for driving service logic.
type fakeElector struct {
	startErr error
	stopErr  error
	promoted chan struct{}
	demoted  chan struct{}
}

func newFakeElector() *fakeElector {
	return &fakeElector{
		promoted: make(chan struct{}, 1),
		demoted:  make(chan struct{}, 1),
	}
}

func (f *fakeElector) Start(_ context.Context) error { return f.startErr }
func (f *fakeElector) Stop() error                   { return f.stopErr }
func (f *fakeElector) IsLeader() bool                { return false }
func (f *fakeElector) PromotedChan() <-chan struct{} { return f.promoted }
func (f *fakeElector) DemotedChan() <-chan struct{}  { return f.demoted }

var _ LeaderElector = (*fakeElector)(nil)

// erroringTicker is a tickerService whose Start/Stop return configurable errors.
type erroringTicker struct {
	startErr error
	stopErr  error
	started  chan struct{}
}

func (e *erroringTicker) Start(ctx context.Context) error {
	if e.started != nil {
		select {
		case e.started <- struct{}{}:
		default:
		}
	}
	if e.startErr != nil {
		return e.startErr
	}
	// Block until canceled to mimic the real ticker loop.
	<-ctx.Done()
	return ctx.Err()
}
func (e *erroringTicker) Stop() error                   { return e.stopErr }
func (e *erroringTicker) UpdateTasks(_ []scheduledTask) {}

var _ tickerService = (*erroringTicker)(nil)

// newServiceForElection builds a service wired with a fake elector and a fake
// ticker factory, with no real Redis dependencies in the election loop.
func newServiceForElection(t *testing.T, fe *fakeElector, ticker tickerService) *service {
	t.Helper()

	svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})
	svc.elector = fe
	svc.tracker = noopTrackerForElection{}
	svc.newTicker = func(_ []scheduledTask) tickerService { return ticker }

	return svc
}

type noopTrackerForElection struct{}

func (noopTrackerForElection) GetLastRun(_ context.Context, _ string) (time.Time, error) {
	return time.Time{}, nil
}

func (noopTrackerForElection) SetLastRun(_ context.Context, _ string, _ time.Time) error {
	return nil
}
func (noopTrackerForElection) Close() error { return nil }

// TestHandleLeaderElectionPromotedTickerError covers the promoted branch where
// the ticker's Start returns a non-context.Canceled error (logged).
func TestHandleLeaderElectionPromotedTickerError(t *testing.T) {
	fe := newFakeElector()
	started := make(chan struct{}, 1)
	ticker := &erroringTicker{startErr: errFake, started: started}

	svc := newServiceForElection(t, fe, ticker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.handleLeaderElection(ctx)
		close(done)
	}()

	// Trigger promotion.
	fe.promoted <- struct{}{}

	// Ticker.Start should be invoked (and return the error, which is logged).
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ticker Start was not called after promotion")
	}

	cancel()
	<-done

	// stopTicker is invoked on shutdown; reset state cleanly.
	svc.stopTicker()
}

// TestHandleLeaderElectionDemoted covers the demoted branch (stopTicker) and the
// stopTicker error path when ticker.Stop returns an error.
func TestHandleLeaderElectionDemoted(t *testing.T) {
	fe := newFakeElector()
	started := make(chan struct{}, 1)
	ticker := &erroringTicker{stopErr: errFake, started: started}

	svc := newServiceForElection(t, fe, ticker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.handleLeaderElection(ctx)
		close(done)
	}()

	// Promote first so a ticker is installed.
	fe.promoted <- struct{}{}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ticker Start was not called after promotion")
	}

	// Now demote: stopTicker runs and ticker.Stop returns an error (logged).
	fe.demoted <- struct{}{}

	// Give the loop a moment to process demotion, then cancel.
	require.Eventually(t, func() bool {
		svc.tickerMu.Lock()
		defer svc.tickerMu.Unlock()
		return svc.ticker == nil
	}, 2*time.Second, 5*time.Millisecond, "ticker should be cleared after demotion")

	cancel()
	<-done
}

// TestHandleLeaderElectionContextCancel covers the ctx.Done() return path.
func TestHandleLeaderElectionContextCancel(t *testing.T) {
	fe := newFakeElector()
	svc := newServiceForElection(t, fe, &erroringTicker{})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		svc.handleLeaderElection(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLeaderElection did not return on context cancel")
	}
}

// TestHandleLeaderElectionDoneReturn covers the s.done return path.
func TestHandleLeaderElectionDoneReturn(t *testing.T) {
	fe := newFakeElector()
	svc := newServiceForElection(t, fe, &erroringTicker{})

	done := make(chan struct{})
	go func() {
		svc.handleLeaderElection(context.Background())
		close(done)
	}()

	close(svc.done)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleLeaderElection did not return on done close")
	}
}

// TestStartElectorError covers the Start path where elector.Start fails.
func TestStartElectorError(t *testing.T) {
	fe := newFakeElector()
	fe.startErr = errFake

	svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})
	svc.elector = fe

	err := svc.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start leader election")
}

// TestStartServerError covers the Start path where server.Start fails because
// the underlying asynq server has already been shut down.
func TestStartServerError(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	cfg := &Config{Concurrency: 1, ShutdownTimeout: 2}
	svc, err := NewService(log, cfg, &redis.Options{Addr: mr.Addr()},
		&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{}, &adminfake.FakeAdminService{})
	require.NoError(t, err)

	s, ok := svc.(*service)
	require.True(t, ok)

	// Swap in a fake elector so Start gets past leader election. Start the real
	// asynq server first so the service's own server.Start call fails with
	// "the server is already running".
	s.elector = newFakeElector()
	require.NoError(t, s.server.Start(s.mux))
	t.Cleanup(s.server.Shutdown)

	err = s.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start scheduler server")

	// handleLeaderElection goroutine was started; stop it cleanly.
	close(s.done)
	s.wg.Wait()
}

// TestStopErrorBranches covers the warn-and-continue branches of Stop when the
// elector, asynq client, and tracker all fail to close.
func TestStopErrorBranches(t *testing.T) {
	fe := newFakeElector()
	fe.stopErr = errFake

	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	cfg := &Config{Concurrency: 1, ShutdownTimeout: 2}
	svc, err := NewService(log, cfg, &redis.Options{Addr: mr.Addr()},
		&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{}, &adminfake.FakeAdminService{})
	require.NoError(t, err)

	s, ok := svc.(*service)
	require.True(t, ok)
	s.elector = fe

	// Close the asynq client and tracker up front so their Close() in Stop fails.
	require.NoError(t, s.client.Close())
	require.NoError(t, s.tracker.Close())

	// Stop should swallow all close errors and still return nil.
	require.NoError(t, s.Stop())
}

// TestStopTickerStopError covers stopTicker when ticker.Stop returns an error.
func TestStopTickerStopError(t *testing.T) {
	svc := newTestService(&testutil.FakeDAGReader{}, &coordinatorfake.FakeCoordinator{})

	_, cancel := context.WithCancel(context.Background())
	svc.ticker = &erroringTicker{stopErr: errFake}
	svc.tickerCancel = cancel

	// Must not panic; the Stop error is logged and the ticker cleared.
	svc.stopTicker()

	assert.Nil(t, svc.ticker)
	assert.Nil(t, svc.tickerCancel)
}
