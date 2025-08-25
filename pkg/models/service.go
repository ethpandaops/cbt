package models

import (
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Service defines the interface for the models service (ethPandaOps pattern)
type Service interface {
	// Lifecycle methods
	Start() error
	Stop() error

	// DAG operations
	GetDAG() DAGReader

	// Rendering operations
	RenderTransformation(model Transformation, position, interval uint64, startTime time.Time) (string, error)
	RenderExternal(model External, cacheState map[string]interface{}) (string, error)
	GetTransformationEnvironmentVariables(model Transformation, position, interval uint64, startTime time.Time) (*[]string, error)
}

// service encapsulates the worker application logic
type service struct {
	config *Config
	log    logrus.FieldLogger

	dag                  *DependencyGraph
	templateEngine       *TemplateEngine
	transformationModels []Transformation
	externalModels       []External
}

// NewService creates a new worker application
func NewService(log logrus.FieldLogger, cfg *Config, _ *redis.Client, clickhouseCfg *clickhouse.Config) (Service, error) {
	dag := NewDependencyGraph()
	templateEngine := NewTemplateEngine(clickhouseCfg, dag)

	return &service{
		config: cfg,
		log:    log.WithField("service", "models"),

		dag:            dag,
		templateEngine: templateEngine,
	}, nil
}

// Start initializes the models service and builds the dependency graph
func (s *service) Start() error {
	if err := s.parseModels(); err != nil {
		return err
	}

	if err := s.buildDAG(); err != nil {
		return err
	}

	s.log.Info("Models service started successfully")

	return nil
}

// Stop gracefully shuts down the models service
func (s *service) Stop() error {
	return nil
}

func (s *service) parseModels() error {
	externalFiles, err := DiscoverPaths(s.config.External.Paths)
	if err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	if len(externalFiles) > 0 {
		for _, file := range externalFiles {
			externalModel, parseErr := NewExternal(file.Content, file.FilePath)
			if parseErr != nil {
				return parseErr
			}

			s.externalModels = append(s.externalModels, externalModel)
		}
	}

	transformationFiles, err := DiscoverPaths(s.config.Transformation.Paths)
	if err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	if len(transformationFiles) > 0 {
		for _, file := range transformationFiles {
			transformationModel, parseErr := NewTransformation(file.Content, file.FilePath)
			if parseErr != nil {
				return parseErr
			}

			s.transformationModels = append(s.transformationModels, transformationModel)
		}
	}

	return nil
}

func (s *service) buildDAG() error {
	return s.dag.BuildGraph(s.transformationModels, s.externalModels)
}

// GetDAG returns the dependency graph
func (s *service) GetDAG() DAGReader {
	return s.dag
}

// RenderTransformation renders a transformation model template with variables
func (s *service) RenderTransformation(model Transformation, position, interval uint64, startTime time.Time) (string, error) {
	return s.templateEngine.RenderTransformation(model, position, interval, startTime)
}

// GetTransformationEnvironmentVariables returns environment variables for a transformation
func (s *service) GetTransformationEnvironmentVariables(model Transformation, position, interval uint64, startTime time.Time) (*[]string, error) {
	return s.templateEngine.GetTransformationEnvironmentVariables(model, position, interval, startTime)
}

// RenderExternal renders an external model template with variables
func (s *service) RenderExternal(model External, cacheState map[string]interface{}) (string, error) {
	return s.templateEngine.RenderExternal(model, cacheState)
}

// Ensure service implements Service interface
var _ Service = (*service)(nil)
