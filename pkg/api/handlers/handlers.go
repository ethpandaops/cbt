// Package handlers implements the API server interface with request handlers for the CBT API.
package handlers

import (
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// IntervalTypeTransformation represents a single transformation step for an interval type
type IntervalTypeTransformation struct {
	Name       string `json:"name"`                 // Display name (e.g., "timestamp", "epoch")
	Expression string `json:"expression,omitempty"` // Optional CEL expression (e.g., "math.floor(value / 12)")
	Format     string `json:"format,omitempty"`     // Optional display format hint (e.g., "datetime", "date", "time")
}

// IntervalTypesConfig maps interval type names to their transformation pipelines
type IntervalTypesConfig map[string][]IntervalTypeTransformation

// Server implements the generated.ServerInterface
type Server struct {
	modelsService models.Service
	adminService  admin.Service
	intervalTypes IntervalTypesConfig
	log           logrus.FieldLogger
}

// NewServer creates a new API server instance
func NewServer(modelsService models.Service, adminService admin.Service, intervalTypes IntervalTypesConfig, log logrus.FieldLogger) *Server {
	return &Server{
		modelsService: modelsService,
		adminService:  adminService,
		intervalTypes: intervalTypes,
		log:           log.WithField("component", "api.handlers"),
	}
}

// Ensure we implement the interface at compile time
var _ generated.ServerInterface = (*Server)(nil)
