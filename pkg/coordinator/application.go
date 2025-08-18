package coordinator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is intentionally exposed when pprofAddr is configured
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/dependencies"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Application encapsulates the coordinator application logic
type Application struct {
	config       *Config
	logger       *logrus.Logger
	coordinator  *Coordinator
	chClient     clickhouse.ClientInterface
	queueManager *tasks.QueueManager
	modelConfigs []models.ModelConfig
	healthServer *http.Server
	pprofServer  *http.Server
}

// NewApplication creates a new coordinator application
func NewApplication(cfg *Config, logger *logrus.Logger) *Application {
	return &Application{
		config: cfg,
		logger: logger,
	}
}

// Start initializes and starts the coordinator application
func (a *Application) Start() error {
	// Validate configuration
	if err := a.config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	a.logger.Info("Starting CBT Coordinator...")

	// Start metrics server
	ctx := context.Background()
	observability.StartMetricsServer(ctx, a.config.MetricsAddr)
	a.logger.WithField("addr", a.config.MetricsAddr).Info("Started metrics server")

	// Start health check server if configured
	if a.config.HealthCheckAddr != "" {
		a.startHealthCheck()
	}

	// Start pprof server if configured
	if a.config.PProfAddr != "" {
		a.startPProf()
	}

	// Setup clients
	opt, chClient, adminManager, err := a.setupClients()
	if err != nil {
		return fmt.Errorf("failed to setup clients: %w", err)
	}
	a.chClient = chClient

	// Load and parse models
	modelConfigs, err := a.loadModels()
	if err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	// Build dependency graph
	depManager := dependencies.NewDependencyGraph()
	if err := depManager.BuildGraph(modelConfigs); err != nil {
		return fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// Update modelConfigs from the graph (ensures consistency)
	for i := range modelConfigs {
		modelID := fmt.Sprintf("%s.%s", modelConfigs[i].Database, modelConfigs[i].Table)
		if enrichedConfig, exists := depManager.GetModelConfig(modelID); exists {
			modelConfigs[i] = enrichedConfig
		}
	}

	a.modelConfigs = modelConfigs

	// Create managers and coordinator
	queueManager, coordinator := a.createManagers(opt, chClient, adminManager, depManager)
	a.queueManager = queueManager
	a.coordinator = coordinator

	// Start the coordinator
	if err := coordinator.Start(); err != nil {
		return fmt.Errorf("failed to start coordinator: %w", err)
	}

	a.logger.Info("Coordinator started successfully")
	a.logger.WithField("models", len(modelConfigs)).Info("Loaded models")

	return nil
}

// Stop gracefully shuts down the coordinator application
func (a *Application) Stop() error {
	a.logger.Info("Shutting down coordinator...")

	// Create a timeout context for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop health check server
	if a.healthServer != nil {
		if err := a.healthServer.Shutdown(ctx); err != nil {
			a.logger.WithError(err).Error("Failed to shutdown health check server")
		}
	}

	// Stop pprof server
	if a.pprofServer != nil {
		if err := a.pprofServer.Shutdown(ctx); err != nil {
			a.logger.WithError(err).Error("Failed to shutdown pprof server")
		}
	}

	// Stop the coordinator
	if a.coordinator != nil {
		if err := a.coordinator.Stop(); err != nil {
			a.logger.WithError(err).Error("Error stopping coordinator")
			// Continue with cleanup
		}
	}

	// Close queue manager
	if a.queueManager != nil {
		if err := a.queueManager.Close(); err != nil {
			a.logger.WithError(err).Error("Failed to close queue manager")
		}
	}

	// Stop ClickHouse client
	if a.chClient != nil {
		if err := a.chClient.Stop(); err != nil {
			a.logger.WithError(err).Error("Failed to stop ClickHouse client")
		}
	}

	return nil
}

func (a *Application) setupClients() (*redis.Options, clickhouse.ClientInterface, *clickhouse.AdminTableManager, error) {
	// Parse Redis URL
	opt, err := redis.ParseURL(a.config.Redis.URL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	// Create ClickHouse client with admin manager
	chClient, adminManager, err := clickhouse.SetupClientWithAdmin(&a.config.ClickHouse, a.logger)
	if err != nil {
		return nil, nil, nil, err
	}

	return opt, chClient, adminManager, nil
}

func (a *Application) loadModels() ([]models.ModelConfig, error) {
	// Load all models using the consolidated loader
	logger := logrus.NewEntry(a.logger)
	modelConfigMap, err := models.LoadAllModels(logger, &a.config.Models)
	if err != nil {
		return nil, fmt.Errorf("failed to load models: %w", err)
	}

	// Convert map to slice
	modelConfigs := make([]models.ModelConfig, 0, len(modelConfigMap))
	for modelID := range modelConfigMap {
		modelConfigs = append(modelConfigs, modelConfigMap[modelID])
		a.logger.WithField("modelID", modelID).Info("Loaded model")
	}

	return modelConfigs, nil
}

func (a *Application) createManagers(opt *redis.Options, chClient clickhouse.ClientInterface, adminManager *clickhouse.AdminTableManager, depManager *dependencies.DependencyGraph) (*tasks.QueueManager, *Coordinator) {
	// Create queue manager
	asynqRedis := asynq.RedisClientOpt{
		Addr: opt.Addr,
		DB:   opt.DB,
	}
	queueManager := tasks.NewQueueManager(&asynqRedis)

	// Create Asynq Inspector for monitoring completed tasks
	inspector := asynq.NewInspector(asynqRedis)

	// Create Redis client for cache
	redisClient := redis.NewClient(opt)

	// Create cache manager
	cacheManager := models.NewExternalCacheManager(redisClient)

	// Create external model executor with cache support
	externalExecutor := validation.NewExternalModelExecutor(chClient, a.logger, cacheManager)

	// Create dependency validator
	validator := validation.NewDependencyValidator(
		adminManager,
		externalExecutor,
		depManager,
		a.logger,
	)

	// Create the coordinator
	coord := NewCoordinator(
		queueManager,
		depManager,
		validator,
		adminManager,
		inspector,
		&asynqRedis,
		a.logger,
	)

	return queueManager, coord
}

func (a *Application) startHealthCheck() {
	a.logger.WithField("addr", a.config.HealthCheckAddr).Info("Starting health check server")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		// Check if coordinator is running
		if a.coordinator != nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("READY"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("NOT READY"))
		}
	})

	a.healthServer = &http.Server{
		Addr:              a.config.HealthCheckAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := a.healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.WithError(err).Error("Health check server failed")
		}
	}()
}

func (a *Application) startPProf() {
	a.logger.WithField("addr", a.config.PProfAddr).Info("Starting pprof server")

	a.pprofServer = &http.Server{
		Addr:              a.config.PProfAddr,
		ReadHeaderTimeout: 120 * time.Second,
	}

	go func() {
		if err := a.pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.WithError(err).Error("Pprof server failed")
		}
	}()
}
