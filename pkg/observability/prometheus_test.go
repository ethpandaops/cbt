package observability

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errSentinel is a static error used to drive the fatal path in tests.
var errSentinel = errors.New("metrics server failure")

func TestNewMetricsServer(t *testing.T) {
	srv := newMetricsServer("127.0.0.1:9999")
	require.NotNil(t, srv)
	assert.Equal(t, "127.0.0.1:9999", srv.Addr)
	assert.Equal(t, 15*time.Second, srv.ReadHeaderTimeout)
	require.NotNil(t, srv.Handler)

	// The handler must expose /metrics.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	srv.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "# HELP")
}

func TestRunMetricsServer_CleanShutdown(t *testing.T) {
	original := fatalf
	t.Cleanup(func() { fatalf = original })

	fatalCalled := make(chan struct{}, 1)
	fatalf = func(error) { fatalCalled <- struct{}{} }

	// Reserve an ephemeral port, then release it so runMetricsServer can bind it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	srv := &http.Server{Addr: addr, ReadHeaderTimeout: time.Second} //nolint:gosec // test server

	done := make(chan struct{})
	go func() {
		defer close(done)
		runMetricsServer(srv)
	}()

	// Wait until the server is actually accepting connections before closing it
	// so ListenAndServe is in progress and returns http.ErrServerClosed.
	require.Eventually(t, func() bool {
		c, derr := net.Dial("tcp", addr)
		if derr != nil {
			return false
		}

		_ = c.Close()

		return true
	}, 2*time.Second, 5*time.Millisecond)

	require.NoError(t, srv.Close())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runMetricsServer did not return after shutdown")
	}

	// A clean shutdown must not trigger fatalf.
	select {
	case <-fatalCalled:
		t.Fatal("fatalf invoked on clean shutdown")
	default:
	}
}

func TestRunMetricsServer_FatalOnError(t *testing.T) {
	original := fatalf
	t.Cleanup(func() { fatalf = original })

	var captured error

	done := make(chan struct{})
	fatalf = func(err error) {
		captured = err
		close(done)
	}

	// An invalid address makes ListenAndServe fail immediately with a
	// non-ErrServerClosed error, triggering fatalf.
	srv := &http.Server{Addr: "127.0.0.1:-1", ReadHeaderTimeout: time.Second} //nolint:gosec // test
	go runMetricsServer(srv)

	select {
	case <-done:
		require.Error(t, captured)
	case <-time.After(2 * time.Second):
		t.Fatal("fatalf was not invoked on listen error")
	}
}

func TestDefaultFatalf(t *testing.T) {
	original := osExit
	t.Cleanup(func() { osExit = original })

	var gotCode int

	called := false
	osExit = func(code int) {
		called = true
		gotCode = code
	}

	defaultFatalf(errSentinel)

	assert.True(t, called)
	assert.Equal(t, 1, gotCode)
}

func TestStartMetricsServer(t *testing.T) {
	// Prevent the real server goroutine from terminating the process on bind
	// failure during this package's tests.
	original := fatalf
	t.Cleanup(func() { fatalf = original })
	fatalf = func(error) {}

	// once ensures the body runs at most once for the whole test binary; calling
	// it twice exercises the singleton guard without starting two servers.
	StartMetricsServer("127.0.0.1:0")
	StartMetricsServer("127.0.0.1:0")

	require.NotNil(t, metricsServerInstance)

	t.Cleanup(func() {
		if metricsServerInstance != nil {
			_ = metricsServerInstance.Close()
		}
	})
}
