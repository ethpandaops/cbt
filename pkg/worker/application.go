package worker

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
	"github.com/ethpandaops/cbt/pkg/models/rendering"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Application encapsulates the worker application logic
type Application struct {
	config         *Config
	logger         *logrus.Logger
	server         *asynq.Server
	chClient       clickhouse.ClientInterface
	adminManager   *clickhouse.AdminTableManager
	modelConfigs   map[string]models.ModelConfig
	filteredModels map[string]models.ModelConfig
	healthServer   *http.Server
	pprofServer    *http.Server
}

// NewApplication creates a new worker application
func NewApplication(cfg *Config, logger *logrus.Logger) *Application {
	return &Application{
		config: cfg,
		logger: logger,
	}
}

// Start initializes and starts the worker application
func (a *Application) Start() error {
	// Validate configuration
	if err := a.config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	a.logger.Info("Starting CBT Worker...")

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

	// Setup Redis
	asynqRedis, err := a.setupRedis()
	if err != nil {
		return fmt.Errorf("failed to setup Redis: %w", err)
	}

	// Setup ClickHouse
	if err := a.setupClickHouse(); err != nil {
		return fmt.Errorf("failed to setup ClickHouse: %w", err)
	}

	// Load and process models
	if err := a.loadModels(); err != nil {
		return fmt.Errorf("failed to load models: %w", err)
	}

	// Create and start worker server
	if err := a.startServer(&asynqRedis); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	a.logger.Info("Worker started successfully")
	a.logger.WithField("models", len(a.filteredModels)).Info("Processing models")

	return nil
}

// Stop gracefully shuts down the worker application
func (a *Application) Stop() error {
	a.logger.Info("Shutting down worker...")

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

	if a.server != nil {
		a.server.Shutdown()
	}

	if a.chClient != nil {
		if err := a.chClient.Stop(); err != nil {
			a.logger.WithError(err).Error("Failed to stop ClickHouse client")
			return err
		}
	}

	return nil
}

func (a *Application) setupRedis() (asynq.RedisClientOpt, error) {
	opt, err := redis.ParseURL(a.config.Redis.URL)
	if err != nil {
		return asynq.RedisClientOpt{}, err
	}

	return asynq.RedisClientOpt{
		Addr: opt.Addr,
		DB:   opt.DB,
	}, nil
}

func (a *Application) setupClickHouse() error {
	chClient, adminManager, err := clickhouse.SetupClientWithAdmin(&a.config.ClickHouse, a.logger)
	if err != nil {
		return err
	}
	a.chClient = chClient
	a.adminManager = adminManager
	return nil
}

func (a *Application) loadModels() error {
	// Load all models using the consolidated loader
	logger := logrus.NewEntry(a.logger)
	allModelConfigMap, err := models.LoadAllModels(logger)
	if err != nil {
		return err
	}

	// Filter transformation models
	transformationModelConfigMap := make(map[string]models.ModelConfig)
	for modelID := range allModelConfigMap {
		modelCfg := allModelConfigMap[modelID]
		if !modelCfg.External {
			transformationModelConfigMap[modelID] = modelCfg
		} else {
			a.logger.WithField("model", modelID).Debug("External model (not processed by workers)")
		}
	}

	// Apply tag filtering
	tagFilter := models.NewTagFilter(a.logger)
	var modelTags *models.ModelTags
	if a.config.Worker.ModelTags != nil {
		modelTags = &models.ModelTags{
			Include: a.config.Worker.ModelTags.Include,
			Exclude: a.config.Worker.ModelTags.Exclude,
			Require: a.config.Worker.ModelTags.Require,
		}
	}
	filteredModelConfigMap := tagFilter.FilterByTags(transformationModelConfigMap, modelTags)

	a.modelConfigs = allModelConfigMap
	a.filteredModels = filteredModelConfigMap

	return nil
}

func (a *Application) startServer(asynqRedis *asynq.RedisClientOpt) error {
	// Create task handler
	handler, err := a.createTaskHandler()
	if err != nil {
		return err
	}

	// Configure queues
	queues := make(map[string]int)
	queues["default"] = 1
	for modelID := range a.filteredModels {
		queues[modelID] = 10
	}

	// Create server
	srv := asynq.NewServer(*asynqRedis, asynq.Config{
		Concurrency: a.config.Worker.Concurrency,
		Queues:      queues,
	})

	// Setup routes
	mux := asynq.NewServeMux()
	for taskType, handlerFunc := range handler.Routes() {
		mux.HandleFunc(taskType, handlerFunc)
	}

	// Start server in background
	go func() {
		if runErr := srv.Run(mux); runErr != nil {
			a.logger.WithError(runErr).Fatal("Failed to run worker server")
		}
	}()

	a.server = srv
	return nil
}

func (a *Application) createTaskHandler() (*tasks.TaskHandler, error) {
	templateEngine := rendering.NewTemplateEngine()

	// Create dependency manager with ALL models (for validation)
	depManager := dependencies.NewDependencyGraph()
	allConfigs := make([]models.ModelConfig, 0, len(a.modelConfigs))
	for id := range a.modelConfigs {
		allConfigs = append(allConfigs, a.modelConfigs[id])
	}
	if err := depManager.BuildGraph(allConfigs); err != nil {
		return nil, err
	}

	// Create executors and validators
	externalExecutor := validation.NewExternalModelExecutor(a.chClient, a.logger)
	validator := validation.NewDependencyValidator(a.adminManager, externalExecutor, depManager, a.logger)
	modelExecutor := NewTransformationModelExecutor(&a.config.ClickHouse, &a.config.Worker, a.chClient, templateEngine, a.adminManager, a.logger)

	// Create handler with filtered models (for processing)
	handler := tasks.NewTaskHandler(a.chClient, a.adminManager, validator, modelExecutor, a.filteredModels)
	return handler, nil
}

func (a *Application) startHealthCheck() {
	a.logger.WithField("addr", a.config.HealthCheckAddr).Info("Starting health check server")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		// Check if worker server is running
		if a.server != nil {
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
