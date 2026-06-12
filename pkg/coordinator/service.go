// Package coordinator handles task coordination and dependency management
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/ethpandaops/cbt/pkg/validation"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrShutdownErrors is returned when errors occur during shutdown
	ErrShutdownErrors = errors.New("errors during shutdown")
	// ErrRefreshInProgress is returned when a bounds refresh is already in flight
	ErrRefreshInProgress = errors.New("bounds refresh already in progress")
	// ErrScheduledRunInProgress is returned when a scheduled run task is already in flight
	ErrScheduledRunInProgress = errors.New("scheduled run already in progress")
	// ErrNotScheduledModel is returned when trying to run-now a non-scheduled model
	ErrNotScheduledModel = errors.New("model is not a scheduled transformation")
	// ErrQueueManagerNil is returned when coordinator queue manager is not initialized.
	ErrQueueManagerNil = errors.New("queue manager is nil")
)

// Service defines the public interface for the coordinator
type Service interface {
	// Start initializes and starts the coordinator service
	Start(ctx context.Context) error

	// Stop gracefully shuts down the coordinator service
	Stop() error

	// Process handles transformation processing in the specified direction
	Process(transformation models.Transformation, direction Direction)

	// ProcessExternalScan handles external model scan processing
	ProcessExternalScan(modelID, scanType string)

	// RunConsolidation performs admin table consolidation for all models
	RunConsolidation(ctx context.Context)

	// TriggerBoundsRefresh enqueues a full external scan for admin-initiated bounds refresh
	TriggerBoundsRefresh(ctx context.Context, modelID string) error

	// TriggerScheduledRun enqueues an immediate run for a scheduled transformation
	TriggerScheduledRun(ctx context.Context, modelID string) error
}

// Direction represents the processing direction for tasks
type Direction string

const (
	// DirectionForward processes tasks in forward direction
	DirectionForward Direction = "forward"
	// DirectionBack processes tasks in backward direction
	DirectionBack Direction = "back"

	// ExternalIncrementalTaskType is the task type for external incremental scan
	ExternalIncrementalTaskType = "external:incremental"
	// ExternalFullTaskType is the task type for external full scan
	ExternalFullTaskType = "external:full"
)

// service coordinates task processing and dependencies
type service struct {
	log logrus.FieldLogger

	// Synchronization - per ethPandaOps standards
	done chan struct{}  // Signal shutdown
	wg   sync.WaitGroup // Track goroutines

	// Channel-based task tracking (ethPandaOps: prefer channels over mutexes)
	taskCheck chan taskOperation // Check if task is processed
	taskMark  chan string        // Mark task as processed

	redisOpt  *redis.Options
	dag       models.DAGReader
	admin     admin.Service
	validator validation.Validator

	queueManager   *tasks.QueueManager
	inspector      *asynq.Inspector
	archiveHandler ArchiveHandler

	// newArchiveHandler constructs the archive handler. It defaults to
	// NewArchiveHandler and exists as a seam so the start-up error paths can be
	// exercised in tests; production behavior is unchanged.
	newArchiveHandler func(log logrus.FieldLogger, redisOpt *redis.Options) (ArchiveHandler, error)

	// marshalJSON indirects encoding/json.Marshal so the otherwise-unreachable
	// payload marshal error paths can be exercised in tests. Defaults to
	// json.Marshal; production behavior is unchanged.
	marshalJSON func(v any) ([]byte, error)

	// pollInterval controls the completed-task polling cadence. Zero means the
	// default of 5s; it exists as a seam so the poll loop can be exercised quickly
	// in tests without changing production behavior.
	pollInterval time.Duration
}

// NewService creates a new coordinator service
func NewService(log logrus.FieldLogger, redisOpt *redis.Options, dag models.DAGReader, adminService admin.Service, validator validation.Validator) (Service, error) {
	return &service{
		log:               log.WithField("service", "coordinator"),
		redisOpt:          redisOpt,
		dag:               dag,
		admin:             adminService,
		validator:         validator,
		done:              make(chan struct{}),
		taskCheck:         make(chan taskOperation),
		taskMark:          make(chan string, 100), // Buffered to avoid blocking
		newArchiveHandler: NewArchiveHandler,
		marshalJSON:       json.Marshal,
	}, nil
}

// Start initializes and starts the coordinator service
func (s *service) Start(ctx context.Context) error {
	asynqRedis := r.NewAsynqOptions(s.redisOpt)

	s.queueManager = tasks.NewQueueManager(asynqRedis)

	s.inspector = asynq.NewInspector(*asynqRedis)

	newArchiveHandler := s.newArchiveHandler
	if newArchiveHandler == nil {
		newArchiveHandler = NewArchiveHandler
	}

	archiveHandler, err := newArchiveHandler(s.log, s.redisOpt)
	if err != nil {
		return fmt.Errorf("failed to create archive handler: %w", err)
	}

	s.archiveHandler = archiveHandler

	if err := s.archiveHandler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start archive handler: %w", err)
	}

	// Start task tracker goroutine (channel-based state management)
	s.wg.Add(1)
	go s.taskTracker()

	// Keep existing completed task polling
	s.wg.Add(1)
	go s.pollCompletedTasks()

	s.log.Info("Coordinator service started successfully")

	return nil
}

// Stop gracefully shuts down the coordinator service
func (s *service) Stop() error {
	var errs []error

	// Signal all goroutines to stop
	close(s.done)

	// Stop archive handler
	if s.archiveHandler != nil {
		if err := s.archiveHandler.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop archive handler: %w", err))
		}
	}

	// Wait for all goroutines to complete
	s.wg.Wait()

	if s.inspector != nil {
		if err := s.inspector.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close inspector: %w", err))
		}
	}

	if s.queueManager != nil {
		if err := s.queueManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close queue manager: %w", err))
		}
	}

	if len(errs) > 0 {
		// Combine all errors into a single error message
		errStrs := make([]string, 0, len(errs))
		for _, err := range errs {
			errStrs = append(errStrs, err.Error())
		}
		return fmt.Errorf("%w: %s", ErrShutdownErrors, errStrs)
	}

	return nil
}

// Process handles transformation processing in the specified direction
func (s *service) Process(trans models.Transformation, direction Direction) {
	switch direction {
	case DirectionForward:
		s.processForward(trans)
	case DirectionBack:
		s.processBack(trans)
	}
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
