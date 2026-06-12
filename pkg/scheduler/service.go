package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/liveconfig"
	"github.com/ethpandaops/cbt/pkg/models"
	r "github.com/ethpandaops/cbt/pkg/redis"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	// TransformationTaskPrefix is the prefix for transformation scheduled tasks
	TransformationTaskPrefix = "transformation:"
	// ConsolidationTaskType is the task type for consolidation
	ConsolidationTaskType = "consolidation"
	// ExternalTaskPrefix is the prefix for external model tasks
	ExternalTaskPrefix = "external:"
	// QueueName is the queue name for scheduler tasks
	QueueName = "scheduler"
	// TransformationTypeScheduled is the transformation type for scheduled transformations
	TransformationTypeScheduled = "scheduled"
	// maxJitterSeconds is the maximum jitter in seconds to prevent thundering herd.
	maxJitterSeconds = 10
	// defaultLiveOverridePollInterval is how often the live override poller runs.
	defaultLiveOverridePollInterval = 5 * time.Second
)

var (
	// ErrNotScheduledType is returned when transformation is not a scheduled type
	ErrNotScheduledType = errors.New("transformation is not a scheduled type")
	// ErrInvalidExternalTaskType is returned when the external task type format is invalid
	ErrInvalidExternalTaskType = errors.New("invalid external task type format")
)

// Service defines the public interface for the scheduler
type Service interface {
	// Start initializes and starts the scheduler service
	Start(ctx context.Context) error

	// Stop gracefully shuts down the scheduler service
	Stop() error

	// SetLiveOverrides sets the live override applier (must be called before Start)
	SetLiveOverrides(lo *liveconfig.Applier)
}

// CoordinatorClient defines the minimal interface needed from coordinator
type CoordinatorClient interface {
	Process(transformation models.Transformation, direction coordinator.Direction)
}

// service manages scheduled tasks for transformations
type service struct {
	log logrus.FieldLogger // Using FieldLogger interface per ethPandaOps
	cfg *Config

	// Synchronization - per ethPandaOps standards
	done chan struct{}  // Signal shutdown
	wg   sync.WaitGroup // Track goroutines

	redisOpt    *redis.Options
	dag         models.DAGReader
	coordinator coordinator.Service
	admin       admin.Service

	client    *asynq.Client
	server    *asynq.Server
	mux       *asynq.ServeMux
	inspector *asynq.Inspector

	tracker      scheduleTracker
	tickerMu     sync.Mutex // Guards ticker/tickerCancel (election loop, override poller, Stop)
	ticker       tickerService
	tickerCancel context.CancelFunc // To stop ticker when demoted

	elector       LeaderElector
	liveOverrides *liveconfig.Applier

	// sleep performs the anti-thundering-herd jitter wait before an initial
	// external scan. It is a field so tests can replace the real time.Sleep
	// with a no-op; production always uses time.Sleep.
	sleep func(time.Duration)

	// liveOverridePollInterval controls how often pollLiveOverrides checks for
	// live config changes. It is a field so tests can use a short interval;
	// production uses defaultLiveOverridePollInterval.
	liveOverridePollInterval time.Duration

	// newTicker constructs the ticker service on leader promotion. It is a field
	// so tests can inject a fake ticker; production uses newTickerService.
	newTicker func(tasks []scheduledTask) tickerService
}

// NewService creates a new scheduler service
func NewService(log logrus.FieldLogger, cfg *Config, redisOpt *redis.Options, dag models.DAGReader, coord coordinator.Service, adminService admin.Service) (Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	asynqRedis := r.NewAsynqOptions(redisOpt)

	// Create client for enqueueing tasks
	client := asynq.NewClient(asynqRedis)

	// Create server for processing scheduled tasks
	server := asynq.NewServer(asynqRedis, asynq.Config{
		Queues: map[string]int{
			QueueName: 10,
		},
		Concurrency: cfg.Concurrency,
	})

	// Create inspector for managing tasks
	inspector := asynq.NewInspector(asynqRedis)

	// Create leader elector
	elector := NewLeaderElector(log, redisOpt)

	// Initialize ServeMux for task handlers (needed before server starts)
	mux := asynq.NewServeMux()

	// Create Redis client for schedule tracker
	redisClient := redis.NewClient(redisOpt)

	// Create schedule tracker
	tracker := newScheduleTracker(log, redisClient)

	s := &service{
		log:  log.WithField("service", "scheduler"), // Add service-specific field per ethPandaOps
		cfg:  cfg,
		done: make(chan struct{}),

		redisOpt:    redisOpt,
		dag:         dag,
		coordinator: coord,
		admin:       adminService,

		client:                   client,
		server:                   server,
		mux:                      mux,
		inspector:                inspector,
		tracker:                  tracker,
		ticker:                   nil, // Created on leader promotion
		tickerCancel:             nil,
		elector:                  elector,
		sleep:                    time.Sleep,
		liveOverridePollInterval: defaultLiveOverridePollInterval,
	}

	s.newTicker = func(scheduledTasks []scheduledTask) tickerService {
		return newTickerService(s.log, s.tracker, s.client, scheduledTasks)
	}

	return s, nil
}

// Start initializes and starts the scheduler service
func (s *service) Start(ctx context.Context) error {
	// Register all task handlers on this instance before starting
	// This ensures any instance can process scheduled tasks from the queue,
	// not just the leader. The leader is responsible for scheduling the tasks,
	// but all instances must be able to execute them when picked up from Redis.
	s.registerAllHandlers()

	// Start leader election
	if err := s.elector.Start(ctx); err != nil {
		return fmt.Errorf("failed to start leader election: %w", err)
	}

	// Handle leader election events
	s.wg.Go(func() {
		s.handleLeaderElection(ctx)
	})

	// Start server in background (always runs for processing scheduled tasks).
	// Start (not Run) is required: Run blocks waiting for OS signals that the
	// engine already handles, which would keep this goroutine alive forever
	// and force every Stop() into the shutdown timeout.
	if err := s.server.Start(s.mux); err != nil {
		return fmt.Errorf("failed to start scheduler server: %w", err)
	}

	// Start live override polling on all instances
	if s.liveOverrides != nil {
		s.wg.Go(func() {
			s.pollLiveOverrides(ctx)
		})
	}

	s.log.Info("Scheduler service started (participating in leader election)")

	return nil
}

// Stop gracefully shuts down the scheduler service
func (s *service) Stop() error {
	// Signal all goroutines to stop
	close(s.done)

	// Stop the ticker if this instance is leader. The ticker context derives
	// from the Start context (a background context in production), so without
	// this the ticker goroutine never exits and wg.Wait below always times out.
	s.stopTicker()

	// Stop leader election first
	if s.elector != nil {
		if err := s.elector.Stop(); err != nil {
			s.log.WithError(err).Warn("Failed to stop leader elector")
		}
	}

	// Shutdown server first
	if s.server != nil {
		s.server.Shutdown()
	}

	// Wait for all goroutines to complete BEFORE closing asynq client with timeout
	// This prevents "redis: client is closed" errors from ticker goroutines
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	timeout := time.Duration(s.cfg.ShutdownTimeout) * time.Second
	select {
	case <-done:
		s.log.Info("Scheduler goroutines stopped successfully")
	case <-time.After(timeout):
		s.log.Warnf("Scheduler shutdown timed out after %v, forcing shutdown", timeout)
	}

	// Now safe to close asynq client
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			s.log.WithError(err).Warn("Failed to close asynq client")
		}
	}

	// Close schedule tracker's Redis client
	if s.tracker != nil {
		if err := s.tracker.Close(); err != nil {
			s.log.WithError(err).Warn("Failed to close schedule tracker")
		}
	}

	s.log.Info("Scheduler service stopped successfully")

	return nil
}

// handleLeaderElection manages scheduler lifecycle based on leader election events
func (s *service) handleLeaderElection(ctx context.Context) {
	promoted := s.elector.PromotedChan()
	demoted := s.elector.DemotedChan()

	for {
		select {
		case <-s.done:
			return

		case <-ctx.Done():
			return

		case <-promoted:
			s.log.Info("Promoted to leader, starting ticker service")

			// Build task list from current DAG config
			scheduledTasks := s.buildScheduledTasks()

			// Create ticker service with current task list
			ticker := s.newTicker(scheduledTasks)
			tickerCtx, cancel := context.WithCancel(ctx) //nolint:gosec // G118 - cancel is stored in s.tickerCancel and called on demotion or Stop

			s.tickerMu.Lock()
			s.ticker = ticker
			s.tickerCancel = cancel
			s.tickerMu.Unlock()

			// Start ticker in goroutine
			s.wg.Go(func() {
				if err := ticker.Start(tickerCtx); err != nil && !errors.Is(err, context.Canceled) {
					s.log.WithError(err).Error("Ticker service error")
				}
			})

			// Trigger initial external scans (keep this - still needed)
			s.triggerInitialExternalScans(ctx)

		case <-demoted:
			s.log.Info("Demoted from leader, stopping ticker service")

			s.stopTicker()
		}
	}
}

// stopTicker cancels and stops the running ticker, if any. It is called from
// the election loop on demotion and from Stop() at shutdown; the mutex makes
// the two paths safe to race and the second call a no-op.
func (s *service) stopTicker() {
	s.tickerMu.Lock()
	defer s.tickerMu.Unlock()

	if s.tickerCancel != nil {
		s.tickerCancel()
		s.tickerCancel = nil
	}

	if s.ticker != nil {
		if err := s.ticker.Stop(); err != nil {
			s.log.WithError(err).Error("Failed to stop ticker")
		}

		s.ticker = nil
	}
}

// triggerInitialExternalScans checks for external models without cache and triggers initial scans
func (s *service) triggerInitialExternalScans(ctx context.Context) {
	externalNodes := s.dag.GetExternalNodes()
	if len(externalNodes) == 0 {
		s.log.Info("No external models found for initial scan check")
		return
	}

	s.log.WithField("count", len(externalNodes)).Info("Checking external models for initial scans")

	// Check each external model for cache
	for _, node := range externalNodes {
		model, ok := node.Model.(models.External)
		if !ok {
			s.log.WithField("node", node).Debug("Node is not an external model")
			continue
		}

		modelID := model.GetID()
		config := model.GetConfig()

		// Skip if cache config not defined
		if config.Cache == nil {
			s.log.WithField("model_id", modelID).Debug("Skipping model without cache config")
			continue
		}

		// Check if cache exists using admin service
		if s.admin != nil {
			// Try to get bounds - if nil or initial scan incomplete, trigger initial scan
			bounds, err := s.admin.GetExternalBounds(ctx, modelID)
			logFields := logrus.Fields{
				"model_id":              modelID,
				"bounds_nil":            bounds == nil,
				"initial_scan_complete": bounds != nil && bounds.InitialScanComplete,
			}
			if err != nil {
				logFields["error"] = err
			}
			s.log.WithFields(logFields).Debug("Checked external model bounds")

			if bounds == nil || !bounds.InitialScanComplete {
				// Add jitter to prevent thundering herd
				jitter := time.Duration(rand.IntN(maxJitterSeconds)) * time.Second //nolint:gosec // G404: jitter doesn't need cryptographic randomness

				s.log.WithFields(logrus.Fields{
					"model_id": modelID,
					"jitter":   jitter,
				}).Info("Triggering initial full scan for external model")

				// Sleep with jitter
				s.sleep(jitter)

				// Trigger full scan immediately
				s.coordinator.ProcessExternalScan(modelID, tasks.ScanTypeFull)
			}
		} else {
			s.log.Debug("Admin service is nil, cannot check external bounds")
		}
	}
}

// SetLiveOverrides sets the live override applier. Must be called before Start.
func (s *service) SetLiveOverrides(lo *liveconfig.Applier) {
	s.liveOverrides = lo
}

// pollLiveOverrides polls for live config override changes every 5 seconds.
// Runs on all instances. When changes are detected and this instance is the
// leader, it rebuilds the ticker task list.
func (s *service) pollLiveOverrides(ctx context.Context) {
	ticker := time.NewTicker(s.liveOverridePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			changed, err := s.liveOverrides.CheckAndApply(ctx)
			if err != nil {
				s.log.WithError(err).Warn("Failed to check live overrides")

				continue
			}

			if changed {
				// Leader: rebuild task list with updated config
				s.tickerMu.Lock()
				if s.ticker != nil {
					s.ticker.UpdateTasks(s.buildScheduledTasks())
					s.log.Info("Rebuilt scheduled tasks after live override change")
				}
				s.tickerMu.Unlock()
			}
		}
	}
}

// Ensure service implements the interface
var _ Service = (*service)(nil)
