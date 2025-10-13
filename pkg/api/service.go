package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/api/handlers"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/sirupsen/logrus"
)

// Service defines the API service interface
type Service interface {
	Start(ctx context.Context) error
	Stop() error
}

type service struct {
	app             *fiber.App
	server          *http.Server
	config          *Config
	modelsService   models.Service
	adminService    admin.Service
	intervalTypes   handlers.IntervalTypesConfig
	frontendHandler http.Handler
	log             logrus.FieldLogger
}

// NewService creates a new API and frontend service
func NewService(cfg *Config, modelsService models.Service, adminService admin.Service, intervalTypes handlers.IntervalTypesConfig, frontendHandler http.Handler, log logrus.FieldLogger) Service {
	return &service{
		config:          cfg,
		modelsService:   modelsService,
		adminService:    adminService,
		intervalTypes:   intervalTypes,
		frontendHandler: frontendHandler,
		log:             log.WithField("service", "api"),
	}
}

// Start initializes and starts the API server with frontend integration
func (s *service) Start(_ context.Context) error {
	if !s.config.Enabled {
		s.log.Info("API service is disabled")
		return nil
	}

	// Create Fiber app with custom error handler
	s.app = fiber.New(fiber.Config{
		ErrorHandler: errorHandler,
		AppName:      "CBT API",
	})

	// Setup middleware
	setupMiddleware(s.app)

	// Create API handler implementation
	server := handlers.NewServer(s.modelsService, s.adminService, s.intervalTypes, s.log)

	// Create API v1 group
	apiV1 := s.app.Group("/api/v1")

	// Register OpenAPI-generated handlers
	generated.RegisterHandlers(apiV1, server)

	// Register frontend handler as fallback for non-API routes
	if s.frontendHandler != nil {
		s.app.Use(adaptor.HTTPHandler(s.frontendHandler))
	}

	// Create HTTP server with the Fiber app
	fiberHandler := adaptor.FiberApp(s.app)
	s.server = &http.Server{
		Addr:              s.config.Addr,
		Handler:           fiberHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start server in goroutine
	go func() {
		s.log.WithField("addr", s.config.Addr).Info("Starting API and frontend server")
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.WithError(err).Error("Server failed to start")
		}
	}()

	return nil
}

// Stop gracefully shuts down the API server
func (s *service) Stop() error {
	if s.server == nil {
		return nil
	}

	s.log.Info("Stopping API and frontend server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	return nil
}
