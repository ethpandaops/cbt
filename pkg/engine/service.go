package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is intentionally exposed when pprofAddr is configured
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api"
	"github.com/ethpandaops/cbt/pkg/api/handlers"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/frontend"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/scheduler"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/ethpandaops/cbt/pkg/worker"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Service encapsulates the worker application logic
type Service struct {
	config *Config
	log    *logrus.Logger

	chClient    clickhouse.ClientInterface
	coordinator coordinator.Service
	scheduler   scheduler.Service
	worker      worker.Service
	admin       admin.Service
	models      models.Service
	api         api.Service

	// Servers
	healthServer *http.Server
	pprofServer  *http.Server

	redisOptions *redis.Options
	redisClient  *redis.Client
}

// NewService creates a new worker application
func NewService(log *logrus.Logger, cfg *Config) (*Service, error) {
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	redisOptions, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	redisClient := redis.NewClient(redisOptions)

	chClient, err := clickhouse.NewClient(log, &cfg.ClickHouse)
	if err != nil {
		log.WithError(err).Fatal("Failed to setup ClickHouse client")
	}

	adminManager := admin.NewService(log, chClient, cfg.ClickHouse.Cluster, cfg.ClickHouse.LocalSuffix, admin.TableConfig{
		IncrementalDatabase: cfg.ClickHouse.Admin.Incremental.Database,
		IncrementalTable:    cfg.ClickHouse.Admin.Incremental.Table,
		ScheduledDatabase:   cfg.ClickHouse.Admin.Scheduled.Database,
		ScheduledTable:      cfg.ClickHouse.Admin.Scheduled.Table,
	}, redisClient)

	modelsService, err := models.NewService(log, &cfg.Models, &cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create models service: %w", err)
	}

	validator := validation.NewDependencyValidator(log, chClient, adminManager, modelsService)

	coordinatorService, err := coordinator.NewService(log, redisOptions, modelsService.GetDAG(), adminManager, validator)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator service: %w", err)
	}

	schedulerService, err := scheduler.NewService(log, &cfg.Scheduler, redisOptions, modelsService.GetDAG(), coordinatorService, adminManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create scheduler service: %w", err)
	}

	workerService, err := worker.NewService(log, &cfg.Worker, chClient, adminManager, modelsService, redisOptions, validator)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker service: %w", err)
	}

	// Convert interval types config to API format
	apiIntervalTypes := convertToAPIIntervalTypes(cfg.IntervalTypes)

	// Create frontend handler if enabled
	var frontendHandler http.Handler
	if cfg.Frontend.Enabled {
		var err error
		frontendHandler, err = frontend.NewHandler()
		if err != nil {
			return nil, fmt.Errorf("failed to create frontend handler: %w", err)
		}
	}

	// Create API service with frontend handler
	apiConfig := &api.Config{
		Enabled: cfg.Frontend.Enabled,
		Addr:    cfg.Frontend.Addr,
	}
	apiService := api.NewService(apiConfig, modelsService, adminManager, apiIntervalTypes, frontendHandler, log)

	return &Service{
		log:    log,
		config: cfg,

		redisOptions: redisOptions,
		redisClient:  redisClient,
		chClient:     chClient,
		coordinator:  coordinatorService,
		scheduler:    schedulerService,
		worker:       workerService,
		admin:        adminManager,
		models:       modelsService,
		api:          apiService,
	}, nil
}

// Start initializes and starts the worker application
func (a *Service) Start() error {
	a.log.Info("Starting CBT Engine...")

	ctx := context.Background()

	// Start metrics server
	observability.StartMetricsServer(a.config.MetricsAddr)
	a.log.WithField("addr", a.config.MetricsAddr).Info("Started metrics server")

	// Start health check server if configured
	if a.config.HealthCheckAddr != "" {
		a.startHealthCheck()
	}

	// Start pprof server if configured
	if a.config.PProfAddr != "" {
		a.startPProf()
	}

	// Start ClickHouse client
	if err := a.chClient.Start(); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	// Start models service
	if err := a.models.Start(); err != nil {
		return fmt.Errorf("failed to start models: %w", err)
	}

	// Start coordinator service
	if err := a.coordinator.Start(ctx); err != nil {
		return fmt.Errorf("failed to start coordinator: %w", err)
	}

	// Start scheduler service
	if err := a.scheduler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	// Start worker service
	if err := a.worker.Start(ctx); err != nil {
		return fmt.Errorf("failed to start worker: %w", err)
	}

	// Start API and frontend service
	if err := a.api.Start(ctx); err != nil {
		return fmt.Errorf("failed to start API and frontend service: %w", err)
	}

	a.log.Info("CBT Engine started successfully")

	return nil
}

// Stop gracefully shuts down the worker application
func (a *Service) Stop() error {
	a.log.Info("Shutting down worker...")

	// Create a timeout context for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Helper function to stop a service
	stopService := func(name string, stopFunc func() error) {
		if stopFunc == nil {
			return
		}
		if err := stopFunc(); err != nil {
			a.log.WithError(err).Errorf("Failed to stop %s", name)
		}
	}

	// Stop all services.
	// 1. Stop scheduler first (stop creating new tasks)
	if a.scheduler != nil {
		stopService("scheduler service", a.scheduler.Stop)
	}

	// 2. Stop coordinator (stop coordinating tasks)
	if a.coordinator != nil {
		stopService("coordinator service", a.coordinator.Stop)
	}

	// 3. Stop worker (finish in-flight tasks)
	if a.worker != nil {
		stopService("worker service", a.worker.Stop)
	}

	// 4. Stop API/frontend
	if a.api != nil {
		stopService("API and frontend service", a.api.Stop)
	}

	// 5. Close Redis (now safe, nothing is using it)
	if a.redisClient != nil {
		stopService("Redis client", a.redisClient.Close)
	}

	// 6. Stop models service
	if a.models != nil {
		stopService("models service", a.models.Stop)
	}

	// Stop ClickHouse client (critical - return error if fails)
	if a.chClient != nil {
		if err := a.chClient.Stop(); err != nil {
			a.log.WithError(err).Error("Failed to stop ClickHouse client")
			return err
		}
	}

	// Stop HTTP servers
	if a.healthServer != nil {
		stopService("health check server", func() error { return a.healthServer.Shutdown(ctx) })
	}
	if a.pprofServer != nil {
		stopService("pprof server", func() error { return a.pprofServer.Shutdown(ctx) })
	}

	return nil
}

func (a *Service) startHealthCheck() {
	a.log.WithField("addr", a.config.HealthCheckAddr).Info("Starting health check server")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	a.healthServer = &http.Server{
		Addr:              a.config.HealthCheckAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := a.healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.WithError(err).Error("Health check server failed")
		}
	}()
}

func (a *Service) startPProf() {
	a.log.WithField("addr", a.config.PProfAddr).Info("Starting pprof server")

	a.pprofServer = &http.Server{
		Addr:              a.config.PProfAddr,
		ReadHeaderTimeout: 120 * time.Second,
	}

	go func() {
		if err := a.pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.WithError(err).Error("Pprof server failed")
		}
	}()
}

// convertToAPIIntervalTypes converts engine config interval types to API handlers format
func convertToAPIIntervalTypes(engineConfig IntervalTypesConfig) handlers.IntervalTypesConfig {
	result := make(handlers.IntervalTypesConfig, len(engineConfig))

	for typeName, transformations := range engineConfig {
		apiTransformations := make([]handlers.IntervalTypeTransformation, len(transformations))
		for i, t := range transformations {
			apiTransformations[i] = handlers.IntervalTypeTransformation{
				Name:       t.Name,
				Expression: t.Expression,
				Format:     t.Format,
			}
		}
		result[typeName] = apiTransformations
	}

	return result
}
