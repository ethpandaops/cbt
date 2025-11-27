package worker

import (
	"context"
	"fmt"
	_ "net/http/pprof" //nolint:gosec // pprof is intentionally exposed when pprofAddr is configured
	"sync"
	"time"

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

// Ensure service implements the interface
var _ Service = (*service)(nil)

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
	wg sync.WaitGroup // Track goroutines

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
	handler := tasks.NewTaskHandler(s.log, s.chClient, s.admin, s.validator, modelExecutor, transformations)

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
	if s.server != nil {
		s.server.Shutdown()
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	timeout := time.Duration(s.config.ShutdownTimeout) * time.Second
	select {
	case <-done:
		s.log.Info("Worker service stopped successfully")
	case <-time.After(timeout):
		s.log.Warnf("Worker service shutdown timed out after %v, forcing shutdown", timeout)
		return fmt.Errorf("%w after %v", ErrWorkerShutdownTimeout, timeout)
	}

	return nil
}

// tagsProvider is an interface for handlers that provide tags.
type tagsProvider interface {
	GetTags() []string
}

func filteredTransformations(modelsService models.Service, tags []string) []models.Transformation {
	dag := modelsService.GetDAG()

	transformationNodes := dag.GetTransformationNodes()

	// If no tags are provided, return all transformations
	if len(tags) == 0 {
		return transformationNodes
	}

	// Build tag set for O(1) lookup.
	tagSet := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tagSet[tag] = struct{}{}
	}

	// Single pass through transformations
	filtered := make([]models.Transformation, 0, len(transformationNodes))

	for _, transformation := range transformationNodes {
		handler := transformation.GetHandler()
		if handler == nil {
			continue
		}

		provider, ok := handler.(tagsProvider)
		if !ok {
			continue
		}

		// Check if any of the transformation's tags match our filter set
		for _, transformationTag := range provider.GetTags() {
			if _, exists := tagSet[transformationTag]; exists {
				filtered = append(filtered, transformation)

				break
			}
		}
	}

	return filtered
}
