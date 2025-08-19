package worker

import (
	"fmt"
	_ "net/http/pprof" //nolint:gosec // pprof is intentionally exposed when pprofAddr is configured
	"slices"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Service encapsulates the worker application logic
type Service struct {
	config *Config
	log    *logrus.Logger

	chClient  clickhouse.ClientInterface
	admin     *admin.Service
	models    *models.Service
	validator *validation.DependencyValidator
	redisOpt  *redis.Options

	server *asynq.Server
}

// NewService creates a new worker application
func NewService(log *logrus.Logger, cfg *Config, chClient clickhouse.ClientInterface, adminService *admin.Service, modelsService *models.Service, redisOpt *redis.Options, validator *validation.DependencyValidator) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &Service{
		log:       log,
		config:    cfg,
		chClient:  chClient,
		admin:     adminService,
		models:    modelsService,
		validator: validator,
		redisOpt:  redisOpt,
	}, nil
}

// Start initializes and starts the worker service
func (s *Service) Start() error {
	transformations := filteredTransformations(s.models, s.config.Tags)

	modelExecutor := NewModelExecutor(s.log, s.chClient, s.models, s.admin)

	// Create handler with filtered models (for processing)
	handler := tasks.NewTaskHandler(s.log, s.chClient, s.admin, s.validator, modelExecutor, transformations)

	// Configure queues
	queues := make(map[string]int)
	for _, transformation := range transformations {
		queues[transformation.GetID()] = 10
	}

	s.log.WithFields(logrus.Fields{
		"transformations": len(transformations),
		"queues":          queues,
	}).Warn("Starting worker service")

	// Create server
	srv := asynq.NewServer(r.NewAsynqRedisOptions(s.redisOpt), asynq.Config{
		Concurrency: s.config.Concurrency,
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
			s.log.WithError(runErr).Fatal("Failed to run worker server")
		}
	}()

	s.server = srv

	s.log.Info("Worker service started successfully")

	return nil
}

// Stop gracefully shuts down the worker application
func (s *Service) Stop() error {
	if s.server != nil {
		s.server.Shutdown()
	}

	return nil
}

func filteredTransformations(modelsService *models.Service, tags []string) []models.Transformation {
	dag := modelsService.GetDAG()

	transformationNodes := dag.GetTransformationNodes()

	// If no tags are provided, return all transformations
	if len(tags) == 0 {
		return transformationNodes
	}

	filteredTransformations := make([]models.Transformation, 0)

	for _, tag := range tags {
		for _, transformation := range transformationNodes {
			if slices.Contains(transformation.GetConfig().Tags, tag) {
				filteredTransformations = append(filteredTransformations, transformation)
			}
		}
	}

	return filteredTransformations
}
