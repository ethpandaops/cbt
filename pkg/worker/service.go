package worker

import (
	"context"
	"fmt"
	_ "net/http/pprof" //nolint:gosec // pprof is intentionally exposed when pprofAddr is configured
	"slices"
	"sync"

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

// Service defines the public interface for the worker service
type Service interface {
	// Start initializes and starts the worker service
	Start(ctx context.Context) error

	// Stop gracefully shuts down the worker service
	Stop() error
}

// service encapsulates the worker application logic
type service struct {
	config *Config
	log    logrus.FieldLogger

	// Synchronization - per ethPandaOps standards
	done chan struct{}  // Signal shutdown
	wg   sync.WaitGroup // Track goroutines

	chClient  clickhouse.ClientInterface
	admin     admin.Service
	models    models.Service
	validator validation.Validator
	redisOpt  *redis.Options

	server *asynq.Server
}

// NewService creates a new worker application
func NewService(log logrus.FieldLogger, cfg *Config, chClient clickhouse.ClientInterface, adminService admin.Service, modelsService models.Service, redisOpt *redis.Options, validator validation.Validator) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &service{
		log:       log.WithField("service", "worker"),
		config:    cfg,
		done:      make(chan struct{}),
		chClient:  chClient,
		admin:     adminService,
		models:    modelsService,
		validator: validator,
		redisOpt:  redisOpt,
	}, nil
}

// Start initializes and starts the worker service
func (s *service) Start(_ context.Context) error {
	transformations := filteredTransformations(s.models, s.config.Tags)

	modelExecutor := NewModelExecutor(s.log, s.chClient, s.models, s.admin)

	// Create handler with filtered models (for processing)
	handler := tasks.NewTaskHandler(s.log, s.chClient, s.admin, s.validator, modelExecutor, transformations, s.models)

	// Configure queues with capacity hint
	queues := make(map[string]int, len(transformations))
	for _, transformation := range transformations {
		queues[transformation.GetID()] = 10
	}

	// Add queues for external models (for bounds cache tasks)
	dag := s.models.GetDAG()
	externalNodes := dag.GetExternalNodes()
	for _, node := range externalNodes {
		if external, ok := node.Model.(models.External); ok {
			queues[external.GetID()] = 10
		}
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

	// Start server in background with proper lifecycle management
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if runErr := srv.Run(mux); runErr != nil {
			s.log.WithError(runErr).Error("Worker server stopped with error")
		}
	}()

	s.server = srv

	s.log.Info("Worker service started successfully")

	return nil
}

// Stop gracefully shuts down the worker application
func (s *service) Stop() error {
	// Signal all goroutines to stop
	close(s.done)

	if s.server != nil {
		s.server.Shutdown()
	}

	// Wait for all goroutines to complete
	s.wg.Wait()

	s.log.Info("Worker service stopped successfully")

	return nil
}

func filteredTransformations(modelsService models.Service, tags []string) []models.Transformation {
	dag := modelsService.GetDAG()

	transformationNodes := dag.GetTransformationNodes()

	// If no tags are provided, return all transformations
	if len(tags) == 0 {
		return transformationNodes
	}

	filteredTransformations := make([]models.Transformation, 0, len(transformationNodes))

	for _, tag := range tags {
		for _, transformation := range transformationNodes {
			if slices.Contains(transformation.GetConfig().Tags, tag) {
				filteredTransformations = append(filteredTransformations, transformation)
			}
		}
	}

	return filteredTransformations
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
