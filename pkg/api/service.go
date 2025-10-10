package api

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/api/handlers"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
)

// Service defines the API service interface
type Service interface {
	Start(ctx context.Context) error
	Stop() error
}

type service struct {
	app           *fiber.App
	config        *Config
	modelsService models.Service
	adminService  admin.Service
	intervalTypes handlers.IntervalTypesConfig
	log           logrus.FieldLogger
}

// NewService creates a new API service
func NewService(cfg *Config, modelsService models.Service, adminService admin.Service, intervalTypes handlers.IntervalTypesConfig, log logrus.FieldLogger) Service {
	return &service{
		config:        cfg,
		modelsService: modelsService,
		adminService:  adminService,
		intervalTypes: intervalTypes,
		log:           log.WithField("service", "api"),
	}
}

// Start initializes and starts the API server
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

	// Start server in goroutine
	go func() {
		s.log.WithField("addr", s.config.Addr).Info("Starting API server")
		if err := s.app.Listen(s.config.Addr); err != nil {
			s.log.WithError(err).Error("API server failed to start")
		}
	}()

	return nil
}

// Stop gracefully shuts down the API server
func (s *service) Stop() error {
	if s.app == nil {
		return nil
	}

	s.log.Info("Stopping API server")
	if err := s.app.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown API server: %w", err)
	}

	return nil
}
