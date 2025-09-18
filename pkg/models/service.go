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

			// Apply default database if not specified
			externalModel.SetDefaultDatabase(s.config.External.DefaultDatabase)

			// Validate that database is set after applying defaults
			config := externalModel.GetConfig()
			if validationErr := config.Validate(); validationErr != nil {
				return fmt.Errorf("model %s validation failed after applying defaults: %w", file.FilePath, validationErr)
			}

			s.externalModels = append(s.externalModels, externalModel)
		}
	}

	transformationFiles, err := DiscoverPaths(s.config.Transformation.Paths)
	if err != nil {
		return fmt.Errorf("failed to discover models: %w", err)
	}

	for _, file := range transformationFiles {
		model, err := s.processTransformationFile(file)
		if err != nil {
			return err
		}
		s.transformationModels = append(s.transformationModels, model)
	}

	// Apply overrides after all models are loaded
	s.applyOverrides()

	return nil
}

// processTransformationFile processes a single transformation file
func (s *service) processTransformationFile(file *ModelFile) (Transformation, error) {
	transformationModel, err := NewTransformation(file.Content, file.FilePath)
	if err != nil {
		return nil, err
	}

	// Apply default database if not specified
	transformationModel.SetDefaultDatabase(s.config.Transformation.DefaultDatabase)

	// Substitute dependency placeholders through handler if it supports it
	s.substitutePlaceholders(transformationModel)

	// Validate that database is set after applying defaults
	if err := transformationModel.GetConfig().Validate(); err != nil {
		return nil, fmt.Errorf("model %s validation failed after applying defaults: %w", file.FilePath, err)
	}

	return transformationModel, nil
}

// substitutePlaceholders handles placeholder substitution in dependencies
func (s *service) substitutePlaceholders(model Transformation) {
	handler := model.GetHandler()
	if handler == nil {
		return
	}

	type placeholderSubstituter interface {
		SubstituteDependencyPlaceholders(externalDefaultDB, transformationDefaultDB string)
	}

	if subProvider, ok := handler.(placeholderSubstituter); ok {
		subProvider.SubstituteDependencyPlaceholders(
			s.config.External.DefaultDatabase,
			s.config.Transformation.DefaultDatabase,
		)
	}
}

func (s *service) applyOverrides() {
	if len(s.config.Overrides) == 0 {
		return
	}

	// Track which overrides were applied
	appliedOverrides := make(map[string]bool)

	// Filter transformations based on overrides - pre-allocate with capacity
	filteredTransformations := make([]Transformation, 0, len(s.transformationModels))
	for _, model := range s.transformationModels {
		config := model.GetConfig()
		modelID := config.GetID()

		// Try to find override using either full ID or table-only format
		override, overrideKey := s.findOverride(modelID, config.Table)

		if override != nil {
			appliedOverrides[overrideKey] = true

			// Check if model is disabled
			if override.IsDisabled() {
				s.log.WithField("model", modelID).Info("Model disabled by override")
				continue // Skip this model
			}

			// Apply configuration overrides to the model
			override.ApplyToTransformation(model)
			s.log.WithField("model", modelID).Debug("Applied configuration override")
		}

		filteredTransformations = append(filteredTransformations, model)
	}

	// Warn about overrides that didn't match any models
	for modelID := range s.config.Overrides {
		if !appliedOverrides[modelID] {
			s.log.WithField("model", modelID).Warn("Override specified for non-existent model")
		}
	}

	s.transformationModels = filteredTransformations
}

// findOverride looks up an override using both full ID and table-only formats
func (s *service) findOverride(fullID, tableName string) (override *ModelOverride, overrideKey string) {
	// First, try with the full model ID (database.table)
	if override, exists := s.config.Overrides[fullID]; exists {
		return override, fullID
	}

	// If the model uses the default database, also try with just the table name
	// This allows more intuitive overrides when using default databases
	if s.config.Transformation.DefaultDatabase != "" {
		// Check if this model is using the default database
		if fullID == fmt.Sprintf("%s.%s", s.config.Transformation.DefaultDatabase, tableName) {
			if override, exists := s.config.Overrides[tableName]; exists {
				return override, tableName
			}
		}
	}

	return nil, ""
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
