package engine

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/frontend"
	"github.com/ethpandaops/cbt/pkg/liveconfig"
	"github.com/ethpandaops/cbt/pkg/management"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/scheduler"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/ethpandaops/cbt/pkg/worker"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.New("injected failure")

// quietLogger returns a logger that suppresses output during tests.
func quietLogger() *logrus.Logger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	return log
}

// freeAddr returns an ephemeral loopback address that is immediately closed,
// so HTTP servers in tests bind without colliding on a fixed port.
func freeAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

func clickhouseConfig() clickhouse.Config {
	return clickhouse.Config{URL: "clickhouse://localhost:9000"}
}

func newServiceConfig(redisURL string) *Config {
	return &Config{
		ClickHouse: clickhouseConfig(),
		Redis:      RedisConfig{URL: redisURL},
		Scheduler:  scheduler.Config{Concurrency: 10, ShutdownTimeout: 1},
		Worker:     worker.Config{Concurrency: 10, ShutdownTimeout: 1},
		Frontend:   FrontendConfig{Enabled: false},
		Management: management.Config{Enabled: false},
	}
}

func TestNewService(t *testing.T) {
	// Not parallel: stubs the frontend handler seam so the frontend-enabled
	// case does not depend on a built frontend (build/frontend is gitignored
	// and absent in CI).
	origFrontend := newFrontendHandler
	newFrontendHandler = func(*frontend.InjectedConfig) (http.Handler, error) {
		return http.NotFoundHandler(), nil
	}
	t.Cleanup(func() { newFrontendHandler = origFrontend })

	mr := miniredis.RunT(t)
	redisURL := "redis://" + mr.Addr()

	tests := []struct {
		name       string
		mutate     func(c *Config)
		wantErr    bool
		wantErrSub string
	}{
		{
			name:    "success minimal",
			mutate:  func(_ *Config) {},
			wantErr: false,
		},
		{
			name: "success management and frontend enabled",
			mutate: func(c *Config) {
				c.Frontend = FrontendConfig{Enabled: true, Addr: ":0"}
				c.Management = management.Config{Enabled: true}
				c.IntervalTypes = IntervalTypesConfig{
					"slot": {{Name: "timestamp", Format: "datetime"}},
				}
			},
			wantErr: false,
		},
		{
			name:       "invalid config",
			mutate:     func(c *Config) { c.Redis.URL = "" },
			wantErr:    true,
			wantErrSub: "invalid configuration",
		},
		{
			name:       "invalid redis url",
			mutate:     func(c *Config) { c.Redis.URL = "not-a-redis-url" },
			wantErr:    true,
			wantErrSub: "failed to parse Redis URL",
		},
		{
			name:       "clickhouse client error",
			mutate:     func(c *Config) { c.ClickHouse.URL = "clickhouse://host:notaport" },
			wantErr:    true,
			wantErrSub: "failed to setup ClickHouse client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newServiceConfig(redisURL)
			tt.mutate(cfg)

			svc, err := NewService(quietLogger(), cfg)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, svc)
				if tt.wantErrSub != "" {
					assert.Contains(t, err.Error(), tt.wantErrSub)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, svc)
			require.NotNil(t, svc.coordinator)
			require.NotNil(t, svc.scheduler)
			require.NotNil(t, svc.worker)
			require.NotNil(t, svc.api)
		})
	}
}

func TestConvertToAPIIntervalTypes(t *testing.T) {
	t.Parallel()

	in := IntervalTypesConfig{
		"slot": {
			{Name: "timestamp", Expression: "value*12", Format: "datetime"},
			{Name: "epoch", Expression: "value/32"},
		},
		"empty": {},
	}

	out := convertToAPIIntervalTypes(in)

	require.Len(t, out, 2)
	require.Len(t, out["slot"], 2)
	assert.Equal(t, "timestamp", out["slot"][0].Name)
	assert.Equal(t, "value*12", out["slot"][0].Expression)
	assert.Equal(t, "datetime", out["slot"][0].Format)
	assert.Equal(t, "epoch", out["slot"][1].Name)
	assert.Empty(t, out["empty"])
}

// --- in-package fakes for the sub-service interfaces ---

type fakeCoordinator struct {
	startErr error
	stopErr  error
}

func (f *fakeCoordinator) Start(_ context.Context) error                        { return f.startErr }
func (f *fakeCoordinator) Stop() error                                          { return f.stopErr }
func (f *fakeCoordinator) Process(models.Transformation, coordinator.Direction) {}
func (f *fakeCoordinator) ProcessExternalScan(_, _ string)                      {}
func (f *fakeCoordinator) RunConsolidation(_ context.Context)                   {}
func (f *fakeCoordinator) TriggerBoundsRefresh(_ context.Context, _ string) error {
	return nil
}
func (f *fakeCoordinator) TriggerScheduledRun(_ context.Context, _ string) error {
	return nil
}

type fakeScheduler struct {
	startErr      error
	stopErr       error
	liveOverrides *liveconfig.Applier
}

func (f *fakeScheduler) Start(_ context.Context) error           { return f.startErr }
func (f *fakeScheduler) Stop() error                             { return f.stopErr }
func (f *fakeScheduler) SetLiveOverrides(lo *liveconfig.Applier) { f.liveOverrides = lo }

type fakeWorker struct {
	startErr error
	stopErr  error
}

func (f *fakeWorker) Start(_ context.Context) error { return f.startErr }
func (f *fakeWorker) Stop() error                   { return f.stopErr }

type fakeAPI struct {
	startErr error
	stopErr  error
}

func (f *fakeAPI) Start(_ context.Context) error { return f.startErr }
func (f *fakeAPI) Stop() error                   { return f.stopErr }

// startErrModels wraps the fake models service to inject a Start error.
type startErrModels struct {
	testutil.FakeModelsService
	startErr error
}

func (m *startErrModels) Start() error { return m.startErr }

// errClickHouse injects Start/Stop errors over the fake clickhouse client.
type errClickHouse struct {
	testutil.FakeClickHouseClient
	startErr error
	stopErr  error
}

func (c *errClickHouse) Start() error { return c.startErr }
func (c *errClickHouse) Stop() error  { return c.stopErr }

// runnableService builds a Service with fake sub-services and a real admin
// service backed by miniredis (so the liveconfig applier path is exercised).
type runnableService struct {
	svc         *Service
	mr          *miniredis.Miniredis
	redisClient *redis.Client
	coord       *fakeCoordinator
	sched       *fakeScheduler
	wrk         *fakeWorker
	apiSvc      *fakeAPI
}

func newRunnableService(t *testing.T, withManagement bool) *runnableService {
	t.Helper()

	log := quietLogger()

	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	chClient := &testutil.FakeClickHouseClient{}

	adminManager := admin.NewService(log, chClient, "", "", admin.TableConfig{
		IncrementalDatabase: "admin", IncrementalTable: "cbt_incremental",
		ScheduledDatabase: "admin", ScheduledTable: "cbt_scheduled",
	}, redisClient)

	cfg := newServiceConfig("redis://" + mr.Addr())

	modelsService, err := models.NewService(log, &cfg.Models, &cfg.ClickHouse)
	require.NoError(t, err)

	coord := &fakeCoordinator{}
	sched := &fakeScheduler{}
	wrk := &fakeWorker{}
	apiSvc := &fakeAPI{}

	var mgmt management.Service
	if withManagement {
		mgmt = management.NewService(&cfg.Management, adminManager, modelsService, coord, redisClient, log)
	}

	svc := &Service{
		log:          log,
		config:       cfg,
		redisOptions: &redis.Options{Addr: mr.Addr()},
		redisClient:  redisClient,
		chClient:     chClient,
		coordinator:  coord,
		scheduler:    sched,
		worker:       wrk,
		admin:        adminManager,
		models:       modelsService,
		management:   mgmt,
		api:          apiSvc,
	}

	return &runnableService{
		svc: svc, mr: mr, redisClient: redisClient,
		coord: coord, sched: sched, wrk: wrk, apiSvc: apiSvc,
	}
}

func TestServiceStartStopFull(t *testing.T) {
	t.Parallel()

	rs := newRunnableService(t, true)
	rs.svc.config.MetricsAddr = freeAddr(t)
	rs.svc.config.HealthCheckAddr = freeAddr(t)
	rs.svc.config.PProfAddr = freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, rs.svc.Start(ctx))

	// liveOverrides must have been wired into the scheduler.
	require.NotNil(t, rs.sched.liveOverrides)

	// Health server should be listening.
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", rs.svc.config.HealthCheckAddr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()

		return true
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, rs.svc.Stop())
}

func TestServiceStartNoOptionalServers(t *testing.T) {
	t.Parallel()

	// HealthCheckAddr and PProfAddr empty: those start* branches are skipped.
	rs := newRunnableService(t, false)
	rs.svc.config.MetricsAddr = freeAddr(t)
	rs.svc.config.HealthCheckAddr = ""
	rs.svc.config.PProfAddr = ""

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, rs.svc.Start(ctx))
	require.NoError(t, rs.svc.Stop())
}

func TestServiceStartWithoutRedisClient(t *testing.T) {
	t.Parallel()

	// redisClient nil: the liveOverrides wiring block is skipped entirely.
	rs := newRunnableService(t, false)
	rs.svc.config.MetricsAddr = freeAddr(t)
	_ = rs.svc.redisClient.Close()
	rs.svc.redisClient = nil

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, rs.svc.Start(ctx))
	require.Nil(t, rs.sched.liveOverrides)
	require.NoError(t, rs.svc.Stop())
}

func TestServiceStartLiveOverridesWarn(t *testing.T) {
	t.Parallel()

	// Closing miniredis makes CheckAndApply fail; Start logs a warning and
	// continues, so the warn branch is exercised without aborting startup.
	rs := newRunnableService(t, false)
	rs.svc.config.MetricsAddr = freeAddr(t)
	rs.mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, rs.svc.Start(ctx))
	require.NoError(t, rs.svc.Stop())
}

func TestServiceStartErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(rs *runnableService)
		wantSub string
	}{
		{
			name: "clickhouse start error",
			setup: func(rs *runnableService) {
				rs.svc.chClient = &errClickHouse{startErr: errInjected}
			},
			wantSub: "failed to start ClickHouse client",
		},
		{
			name: "models start error",
			setup: func(rs *runnableService) {
				rs.svc.models = &startErrModels{startErr: errInjected}
			},
			wantSub: "failed to start models",
		},
		{
			name:    "coordinator start error",
			setup:   func(rs *runnableService) { rs.coord.startErr = errInjected },
			wantSub: "failed to start coordinator",
		},
		{
			name:    "scheduler start error",
			setup:   func(rs *runnableService) { rs.sched.startErr = errInjected },
			wantSub: "failed to start scheduler",
		},
		{
			name:    "worker start error",
			setup:   func(rs *runnableService) { rs.wrk.startErr = errInjected },
			wantSub: "failed to start worker",
		},
		{
			name:    "api start error",
			setup:   func(rs *runnableService) { rs.apiSvc.startErr = errInjected },
			wantSub: "failed to start API and frontend service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rs := newRunnableService(t, false)
			rs.svc.config.MetricsAddr = freeAddr(t)
			tt.setup(rs)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := rs.svc.Start(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantSub)

			require.NoError(t, rs.svc.Stop())
		})
	}
}

func TestServiceStopNilServices(t *testing.T) {
	t.Parallel()

	// All sub-services nil: Stop short-circuits every guard cleanly.
	svc := &Service{log: quietLogger()}

	require.NoError(t, svc.Stop())
}

func TestServiceStopServiceErrors(t *testing.T) {
	t.Parallel()

	// Each non-critical service returns an error from Stop; these are logged
	// but Stop still returns nil. The redis client is closed too.
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	svc := &Service{
		log:         quietLogger(),
		scheduler:   &fakeScheduler{stopErr: errInjected},
		coordinator: &fakeCoordinator{stopErr: errInjected},
		worker:      &fakeWorker{stopErr: errInjected},
		api:         &fakeAPI{stopErr: errInjected},
		redisClient: redisClient,
		models:      &testutil.FakeModelsService{},
		chClient:    &testutil.FakeClickHouseClient{},
	}

	require.NoError(t, svc.Stop())
}

func TestServiceStopChClientError(t *testing.T) {
	t.Parallel()

	svc := &Service{
		log:      quietLogger(),
		chClient: &errClickHouse{stopErr: errInjected},
	}

	err := svc.Stop()
	require.ErrorIs(t, err, errInjected)
}

func TestStartHealthCheckEndpoints(t *testing.T) {
	t.Parallel()

	addr := freeAddr(t)
	svc := &Service{
		log:    quietLogger(),
		config: &Config{HealthCheckAddr: addr},
	}

	svc.startHealthCheck()
	require.NotNil(t, svc.healthServer)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = svc.healthServer.Shutdown(ctx)
	})

	for _, path := range []string{"/health", "/ready"} {
		var (
			status int
			body   string
		)

		require.Eventually(t, func() bool {
			resp, err := http.Get("http://" + addr + path) //nolint:noctx // simple test probe
			if err != nil {
				return false
			}
			defer func() { _ = resp.Body.Close() }()

			b, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return false
			}
			status = resp.StatusCode
			body = string(b)

			return true
		}, 5*time.Second, 50*time.Millisecond)

		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "OK", body)
	}
}

func TestStartServersBindError(t *testing.T) {
	t.Parallel()

	// An out-of-range port makes ListenAndServe fail immediately with a
	// non-ErrServerClosed error, exercising the goroutine error branches.
	svc := &Service{
		log:    quietLogger(),
		config: &Config{HealthCheckAddr: "127.0.0.1:99999", PProfAddr: "127.0.0.1:99999"},
	}

	svc.startHealthCheck()
	svc.startPProf()

	require.NotNil(t, svc.healthServer)
	require.NotNil(t, svc.pprofServer)

	// Give the goroutines time to attempt to bind and hit the error branch.
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = svc.healthServer.Shutdown(ctx)
	_ = svc.pprofServer.Shutdown(ctx)
}

func TestNewServiceConstructorErrors(t *testing.T) {
	// Not parallel: mutates package-level constructor seams.
	mr := miniredis.RunT(t)
	redisURL := "redis://" + mr.Addr()

	origModels := newModelsService
	origCoord := newCoordinatorService
	origSched := newSchedulerService
	origWorker := newWorkerService
	origFrontend := newFrontendHandler

	t.Cleanup(func() {
		newModelsService = origModels
		newCoordinatorService = origCoord
		newSchedulerService = origSched
		newWorkerService = origWorker
		newFrontendHandler = origFrontend
	})

	tests := []struct {
		name    string
		inject  func()
		mutate  func(c *Config)
		wantSub string
	}{
		{
			name: "models constructor error",
			inject: func() {
				newModelsService = func(logrus.FieldLogger, *models.Config, *clickhouse.Config) (models.Service, error) {
					return nil, errInjected
				}
			},
			wantSub: "failed to create models service",
		},
		{
			name: "coordinator constructor error",
			inject: func() {
				newCoordinatorService = func(logrus.FieldLogger, *redis.Options, models.DAGReader, admin.Service, validation.Validator) (coordinator.Service, error) { //nolint:lll // signature mirrors coordinator.NewService
					return nil, errInjected
				}
			},
			wantSub: "failed to create coordinator service",
		},
		{
			name: "scheduler constructor error",
			inject: func() {
				newSchedulerService = func(logrus.FieldLogger, *scheduler.Config, *redis.Options, models.DAGReader, coordinator.Service, admin.Service) (scheduler.Service, error) { //nolint:lll // signature mirrors scheduler.NewService
					return nil, errInjected
				}
			},
			wantSub: "failed to create scheduler service",
		},
		{
			name: "worker constructor error",
			inject: func() {
				newWorkerService = func(logrus.FieldLogger, *worker.Config, clickhouse.ClientInterface, admin.Service, models.Service, *redis.Options, validation.Validator) (worker.Service, error) { //nolint:lll // signature mirrors worker.NewService
					return nil, errInjected
				}
			},
			wantSub: "failed to create worker service",
		},
		{
			name: "frontend handler error",
			inject: func() {
				newFrontendHandler = func(*frontend.InjectedConfig) (http.Handler, error) {
					return nil, errInjected
				}
			},
			mutate:  func(c *Config) { c.Frontend = FrontendConfig{Enabled: true, Addr: ":0"} },
			wantSub: "failed to create frontend handler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Restore seams before each case to avoid cross-contamination.
			newModelsService = origModels
			newCoordinatorService = origCoord
			newSchedulerService = origSched
			newWorkerService = origWorker
			newFrontendHandler = origFrontend

			tt.inject()

			cfg := newServiceConfig(redisURL)
			if tt.mutate != nil {
				tt.mutate(cfg)
			}

			svc, err := NewService(quietLogger(), cfg)
			require.Error(t, err)
			assert.Nil(t, svc)
			assert.Contains(t, err.Error(), tt.wantSub)
			require.ErrorIs(t, err, errInjected)
		})
	}
}

func TestStopServiceNilFunc(t *testing.T) {
	t.Parallel()

	// Directly exercise the nil-guard branch of the stopService helper.
	svc := &Service{log: quietLogger()}
	svc.stopService("noop", nil)
}

func TestServiceStopHTTPServers(t *testing.T) {
	t.Parallel()

	// Start the optional HTTP servers, then Stop to exercise their Shutdown
	// branches.
	rs := newRunnableService(t, false)
	rs.svc.config.MetricsAddr = freeAddr(t)
	rs.svc.config.HealthCheckAddr = freeAddr(t)
	rs.svc.config.PProfAddr = freeAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, rs.svc.Start(ctx))
	require.NotNil(t, rs.svc.healthServer)
	require.NotNil(t, rs.svc.pprofServer)
	require.NoError(t, rs.svc.Stop())
}
