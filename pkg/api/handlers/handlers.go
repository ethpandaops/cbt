// Package handlers implements the API server interface with request handlers for the CBT API.
package handlers

import (
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// Server implements the generated.ServerInterface
type Server struct {
	modelsService models.Service
	adminService  admin.Service
	log           logrus.FieldLogger
}

// NewServer creates a new API server instance
func NewServer(modelsService models.Service, adminService admin.Service, log logrus.FieldLogger) *Server {
	return &Server{
		modelsService: modelsService,
		adminService:  adminService,
		log:           log.WithField("component", "api.handlers"),
	}
}

// Ensure we implement the interface at compile time
var _ generated.ServerInterface = (*Server)(nil)
