// Package frontend provides a static file server for the frontend assets
package frontend

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	static "github.com/ethpandaops/cbt/frontend"
	"github.com/sirupsen/logrus"
)

// Service defines the frontend service interface
type Service interface {
	Start(ctx context.Context) error
	Stop() error
	Handler() http.Handler
}

type service struct {
	log    logrus.FieldLogger
	config *Config

	handler    http.Handler
	filesystem fs.FS
	server     *http.Server
}

// NewService creates a new frontend service
func NewService(cfg *Config, log logrus.FieldLogger) Service {
	return &service{
		log:    log.WithField("service", "frontend"),
		config: cfg,
	}
}

// Start initializes and starts the frontend service
func (s *service) Start(_ context.Context) error {
	if !s.config.Enabled {
		s.log.Info("Frontend service is disabled")
		return nil
	}

	frontendFS, err := fs.Sub(static.FS, "build/frontend")
	if err != nil {
		return fmt.Errorf("failed to load frontend filesystem: %w", err)
	}

	s.filesystem = frontendFS
	s.handler = http.FileServer(http.FS(s.filesystem))

	// Create HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveHTTP)

	s.server = &http.Server{
		Addr:              s.config.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start server in goroutine
	go func() {
		s.log.WithField("addr", s.config.Addr).Info("Starting frontend server")
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.WithError(err).Error("Frontend server failed to start")
		}
	}()

	return nil
}

// Stop gracefully shuts down the frontend server
func (s *service) Stop() error {
	if s.server == nil {
		return nil
	}

	s.log.Info("Stopping frontend server")
	if err := s.server.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("failed to shutdown frontend server: %w", err)
	}

	return nil
}

// Handler returns the frontend HTTP handler
func (s *service) Handler() http.Handler {
	return s.handler
}

// serveHTTP handles frontend requests with SPA fallback support
func (s *service) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if s.handler == nil {
		http.Error(w, "Frontend not initialized", http.StatusInternalServerError)
		return
	}

	// Check if the file exists
	path := strings.TrimPrefix(req.URL.Path, "/")
	if s.fileExists(path) {
		s.handler.ServeHTTP(w, req)
		return
	}

	// Fall back to index.html for SPA routing
	req.URL.Path = "/"
	s.handler.ServeHTTP(w, req)
}

// fileExists checks if a file exists in the frontend filesystem
func (s *service) fileExists(path string) bool {
	_, err := fs.Stat(s.filesystem, path)
	return err == nil
}
