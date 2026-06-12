package api

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/api/handlers"
	"github.com/ethpandaops/cbt/pkg/management"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeManagement is a minimal management.Service used to exercise the
// managementService.RegisterRoutes branch in Start.
type fakeManagement struct {
	registered bool
}

func (f *fakeManagement) RegisterRoutes(router fiber.Router) {
	f.registered = true
	router.Get("/mgmt-ping", func(c fiber.Ctx) error {
		return c.SendString("mgmt-ok")
	})
}

func (f *fakeManagement) GetFrontendConfig() management.FrontendConfig {
	return management.FrontendConfig{}
}

func (f *fakeManagement) SetBaseConfigProvider(_ management.BaseConfigProvider) {}

// managementArg returns a properly-typed management.Service, returning a true
// nil interface when mgmt is nil to avoid the typed-nil interface pitfall.
func managementArg(mgmt *fakeManagement) management.Service {
	if mgmt == nil {
		return nil
	}

	return mgmt
}

var _ management.Service = (*fakeManagement)(nil)

// frontendStub is a minimal http.Handler used to exercise the frontendHandler
// branch in Start.
type frontendStub struct{}

func (frontendStub) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("frontend-ok"))
}

// freeAddr returns a loopback address with an OS-assigned free port.
func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

func newTestService(t *testing.T, cfg *Config, frontend http.Handler, mgmt *fakeManagement) (*service, *logrustest.Hook) {
	t.Helper()

	log, hook := logrustest.NewNullLogger()

	modelsSvc := &testutil.FakeModelsService{}
	adminSvc := &adminfake.FakeAdminService{}

	mgmtArg := managementArg(mgmt)

	svc := NewService(
		cfg,
		modelsSvc,
		adminSvc,
		handlers.IntervalTypesConfig{},
		frontend,
		mgmtArg,
		log,
	)

	s, ok := svc.(*service)
	require.True(t, ok)

	return s, hook
}

func TestNewService(t *testing.T) {
	log, _ := logrustest.NewNullLogger()

	svc := NewService(
		&Config{},
		&testutil.FakeModelsService{},
		&adminfake.FakeAdminService{},
		handlers.IntervalTypesConfig{},
		nil,
		nil,
		log,
	)

	require.NotNil(t, svc)
}

func TestService_Start_Disabled(t *testing.T) {
	s, hook := newTestService(t, &Config{Enabled: false}, nil, nil)

	require.NoError(t, s.Start(context.Background()))
	assert.Nil(t, s.server, "server must not be created when disabled")

	var found bool

	for _, e := range hook.AllEntries() {
		if e.Message == "API service is disabled" {
			found = true
		}
	}

	assert.True(t, found, "expected disabled log message")
}

func TestService_StartStop_Enabled(t *testing.T) {
	tests := []struct {
		name       string
		withFront  bool
		withMgmt   bool
		probePath  string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "frontend_and_management",
			withFront:  true,
			withMgmt:   true,
			probePath:  "/api/v1/mgmt-ping",
			wantBody:   "mgmt-ok",
			wantStatus: http.StatusOK,
		},
		{
			name:       "frontend_fallback_serves_frontend",
			withFront:  true,
			withMgmt:   false,
			probePath:  "/some/spa/route",
			wantBody:   "frontend-ok",
			wantStatus: http.StatusOK,
		},
		{
			name:       "no_frontend_no_management",
			withFront:  false,
			withMgmt:   false,
			probePath:  "/missing",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := freeAddr(t)
			cfg := &Config{Enabled: true, Addr: addr}

			var frontend http.Handler
			if tt.withFront {
				frontend = frontendStub{}
			}

			var mgmt *fakeManagement
			if tt.withMgmt {
				mgmt = &fakeManagement{}
			}

			s, _ := newTestService(t, cfg, frontend, mgmt)

			require.NoError(t, s.Start(context.Background()))
			t.Cleanup(func() { _ = s.Stop() })

			require.NotNil(t, s.server)

			if tt.withMgmt {
				require.True(t, mgmt.registered)
			}

			body, status := getWithRetry(t, "http://"+addr+tt.probePath)
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, body)
			}
		})
	}
}

func TestService_Stop_NilServer(t *testing.T) {
	s, _ := newTestService(t, &Config{Enabled: false}, nil, nil)

	// Stop before Start: server is nil.
	require.NoError(t, s.Stop())
}

// blockingFrontend blocks until release is closed, keeping a request in flight.
type blockingFrontend struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingFrontend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	close(b.started)
	<-b.release
	w.WriteHeader(http.StatusOK)
}

func TestService_Stop_ShutdownError(t *testing.T) {
	// Shrink the shutdown timeout so Shutdown times out while a request is in
	// flight, exercising the error branch of Stop.
	orig := shutdownTimeout
	shutdownTimeout = 1 * time.Millisecond
	t.Cleanup(func() { shutdownTimeout = orig })

	front := &blockingFrontend{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(front.release) })

	addr := freeAddr(t)
	s, _ := newTestService(t, &Config{Enabled: true, Addr: addr}, front, nil)

	require.NoError(t, s.Start(context.Background()))

	// Ensure the listener is accepting before issuing the blocking request.
	waitListening(t, addr)

	// Fire a request that the frontend handler will block on.
	go func() {
		resp, err := http.Get("http://" + addr + "/blocking") //nolint:gosec,noctx // localhost test URL
		if err == nil {
			_ = resp.Body.Close()
		}
	}()

	// Wait until the handler is actively processing the request.
	select {
	case <-front.started:
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached the blocking handler")
	}

	err := s.Stop()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to shutdown server")
}

func TestService_Start_ListenError(t *testing.T) {
	// Occupy a port so the service's ListenAndServe fails immediately.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	addr := l.Addr().String()

	log := logrus.New()
	log.SetOutput(io.Discard)

	errLogged := make(chan struct{}, 1)
	log.AddHook(&messageHook{
		match: "Server failed to start",
		fired: errLogged,
	})

	svc := NewService(
		&Config{Enabled: true, Addr: addr},
		&testutil.FakeModelsService{},
		&adminfake.FakeAdminService{},
		handlers.IntervalTypesConfig{},
		nil,
		nil,
		log,
	)

	require.NoError(t, svc.Start(context.Background()))
	t.Cleanup(func() { _ = svc.Stop() })

	select {
	case <-errLogged:
	case <-time.After(5 * time.Second):
		t.Fatal("expected ListenAndServe error to be logged")
	}
}

// messageHook fires its channel when an entry with the given message is logged.
type messageHook struct {
	match string
	fired chan struct{}
}

func (h *messageHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *messageHook) Fire(e *logrus.Entry) error {
	if e.Message == h.match {
		select {
		case h.fired <- struct{}{}:
		default:
		}
	}

	return nil
}

// waitListening blocks until a TCP connection to addr succeeds.
func waitListening(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()

			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("server never started listening on %s: %v", addr, err)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// getWithRetry polls the URL until the server is ready or the deadline passes.
func getWithRetry(t *testing.T, url string) (body string, status int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		resp, err := http.Get(url) //nolint:gosec,noctx // test-controlled localhost URL
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			return string(body), resp.StatusCode
		}

		if time.Now().After(deadline) {
			t.Fatalf("server never became reachable: %v", err)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
