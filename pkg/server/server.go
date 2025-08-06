package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	//nolint:gosec // only exposed if pprofAddr config is set
	_ "net/http/pprof"

	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/redis"
	r "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Server represents the main application server
type Server struct {
	log       logrus.FieldLogger
	config    *Config
	namespace string

	redis *r.Client

	pprofServer  *http.Server
	healthServer *http.Server
	apiServer    *http.Server
}

// NewServer creates a new server instance
func NewServer(_ context.Context, log logrus.FieldLogger, namespace string, config *Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	redisClient, err := redis.New(config.Redis)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	return &Server{
		config:    config,
		log:       log,
		namespace: namespace,
		redis:     redisClient,
	}, nil
}

// Start starts the server and all its components
func (s *Server) Start(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	// Log component states
	s.log.WithFields(logrus.Fields{
		"has_redis": s.redis != nil,
	}).Debug("Server component states")

	// Start metrics server
	g.Go(func() error {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.log.WithField("panic", recovered).Error("Panic in metrics server goroutine")
			}
		}()
		observability.StartMetricsServer(ctx, s.config.MetricsAddr)
		<-ctx.Done()

		return nil
	})

	// Start pprof server if configured
	if s.config.PProfAddr != nil {
		g.Go(func() error {
			if err := s.startPProf(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}

			<-ctx.Done()

			return nil
		})
	}

	// Start health check server if configured
	if s.config.HealthCheckAddr != nil {
		g.Go(func() error {
			if err := s.startHealthCheck(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}

			<-ctx.Done()

			return nil
		})
	}

	// Wait for shutdown signal
	g.Go(func() error {
		<-ctx.Done()

		// Use a fresh context for cleanup since the current one is canceled
		cleanupCtx := context.Background()

		return s.stop(cleanupCtx)
	})

	return g.Wait()
}

func (s *Server) stop(ctx context.Context) error {
	// Create a timeout context for cleanup
	cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	s.log.Info("Starting graceful shutdown...")

	// Close Redis connection
	if s.redis != nil {
		s.log.Info("Closing Redis connection...")

		if err := s.redis.Close(); err != nil {
			s.log.WithError(err).Error("failed to close redis")
		}
	}

	// Shutdown HTTP servers
	if s.pprofServer != nil {
		if err := s.pprofServer.Shutdown(cleanupCtx); err != nil {
			s.log.WithError(err).Error("failed to shutdown pprof server")
		}
	}

	if s.healthServer != nil {
		if err := s.healthServer.Shutdown(cleanupCtx); err != nil {
			s.log.WithError(err).Error("failed to shutdown health server")
		}
	}

	if s.apiServer != nil {
		if err := s.apiServer.Shutdown(cleanupCtx); err != nil {
			s.log.WithError(err).Error("failed to shutdown API server")
		}
	}

	// Stop metrics server using observability package
	if err := observability.StopMetricsServer(cleanupCtx); err != nil {
		s.log.WithError(err).Error("failed to stop metrics server")
	}

	s.log.Info("Worker stopped gracefully")

	return nil
}

func (s *Server) startPProf() error {
	s.log.WithField("addr", *s.config.PProfAddr).Info("Starting pprof server")

	s.pprofServer = &http.Server{
		Addr:              *s.config.PProfAddr,
		ReadHeaderTimeout: 120 * time.Second,
	}

	return s.pprofServer.ListenAndServe()
}

func (s *Server) startHealthCheck() error {
	s.log.WithField("addr", *s.config.HealthCheckAddr).Info("Starting healthcheck server")

	s.healthServer = &http.Server{
		Addr:              *s.config.HealthCheckAddr,
		ReadHeaderTimeout: 120 * time.Second,
	}

	s.healthServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return s.healthServer.ListenAndServe()
}
