package models

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

var (
	// ErrOverrideModelNotFound is returned when an override key doesn't match any loaded model
	ErrOverrideModelNotFound = errors.New("no matching model found for override key")
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

	modelTypes := s.buildModelTypeMapping()
	s.resolveAllOverrides(modelTypes)
	appliedOverrides := s.applyOverridesToModels()
	s.warnUnmatchedOverrides(appliedOverrides)
}

// buildModelTypeMapping creates a map of model IDs to their types
func (s *service) buildModelTypeMapping() map[string]ModelType {
	modelTypes := make(map[string]ModelType, len(s.externalModels)+len(s.transformationModels))

	for _, model := range s.externalModels {
		config := model.GetConfig()
		modelTypes[config.GetID()] = ModelTypeExternal
	}

	for _, model := range s.transformationModels {
		config := model.GetConfig()
		modelTypes[config.GetID()] = ModelTypeTransformation
	}

	return modelTypes
}

// resolveAllOverrides resolves the config for all overrides based on model type lookup
func (s *service) resolveAllOverrides(modelTypes map[string]ModelType) {
	for modelID, override := range s.config.Overrides {
		if modelType, exists := modelTypes[modelID]; exists {
			if err := override.ResolveConfig(modelType); err != nil {
				s.log.WithField("model", modelID).WithError(err).Warn("Failed to resolve override config")
			}
		} else {
			// Try table-only lookup with default databases
			if resolveErr := s.resolveOverrideWithDefaults(modelID, override, modelTypes); resolveErr != nil {
				s.log.WithField("model", modelID).Debug("Could not resolve override (will check during model iteration)")
			}
		}
	}
}

// applyOverridesToModels applies overrides to both external and transformation models
func (s *service) applyOverridesToModels() map[string]bool {
	appliedOverrides := make(map[string]bool, len(s.config.Overrides))

	s.externalModels = s.applyExternalOverrides(appliedOverrides)
	s.transformationModels = s.applyTransformationOverrides(appliedOverrides)

	return appliedOverrides
}

// applyExternalOverrides applies overrides to external models and returns filtered list
func (s *service) applyExternalOverrides(appliedOverrides map[string]bool) []External {
	filtered := make([]External, 0, len(s.externalModels))

	for _, model := range s.externalModels {
		if s.shouldSkipModel(model, ModelTypeExternal, appliedOverrides) {
			continue
		}
		filtered = append(filtered, model)
	}

	return filtered
}

// applyTransformationOverrides applies overrides to transformation models and returns filtered list
func (s *service) applyTransformationOverrides(appliedOverrides map[string]bool) []Transformation {
	filtered := make([]Transformation, 0, len(s.transformationModels))

	for _, model := range s.transformationModels {
		if s.shouldSkipModel(model, ModelTypeTransformation, appliedOverrides) {
			continue
		}
		filtered = append(filtered, model)
	}

	return filtered
}

// shouldSkipModel checks if a model should be skipped and applies overrides if found
func (s *service) shouldSkipModel(model interface{}, modelType ModelType, appliedOverrides map[string]bool) bool {
	var modelID, tableName string
	var override *ModelOverride
	var overrideKey string

	switch modelType {
	case ModelTypeExternal:
		extModel, ok := model.(External)
		if !ok {
			return false
		}
		config := extModel.GetConfig()
		modelID = config.GetID()
		tableName = config.Table
		override, overrideKey = s.findOverride(modelID, tableName, ModelTypeExternal)

		if override != nil {
			appliedOverrides[overrideKey] = true

			if override.IsDisabled() {
				s.log.WithField("model", modelID).Info("External model disabled by override")
				return true
			}

			mutableConfig := extModel.GetConfigMutable()
			override.ApplyToExternal(mutableConfig)
			s.log.WithField("model", modelID).Debug("Applied external configuration override")
		}

	case ModelTypeTransformation:
		transModel, ok := model.(Transformation)
		if !ok {
			return false
		}
		config := transModel.GetConfig()
		modelID = config.GetID()
		tableName = config.Table
		override, overrideKey = s.findOverride(modelID, tableName, ModelTypeTransformation)

		if override != nil {
			appliedOverrides[overrideKey] = true

			if override.IsDisabled() {
				s.log.WithField("model", modelID).Info("Transformation model disabled by override")
				return true
			}

			override.ApplyToTransformation(transModel)
			s.log.WithField("model", modelID).Debug("Applied transformation configuration override")
		}
	}

	return false
}

// warnUnmatchedOverrides logs warnings for overrides that didn't match any models
func (s *service) warnUnmatchedOverrides(appliedOverrides map[string]bool) {
	for modelID := range s.config.Overrides {
		if !appliedOverrides[modelID] {
			s.log.WithField("model", modelID).Warn("Override specified for non-existent model")
		}
	}
}

// resolveOverrideWithDefaults attempts to resolve an override using default database lookup
func (s *service) resolveOverrideWithDefaults(overrideKey string, override *ModelOverride, modelTypes map[string]ModelType) error {
	// Check if this could be a table-only key
	// Try with external default database
	if s.config.External.DefaultDatabase != "" {
		fullID := fmt.Sprintf("%s.%s", s.config.External.DefaultDatabase, overrideKey)
		if modelType, exists := modelTypes[fullID]; exists {
			return override.ResolveConfig(modelType)
		}
	}

	// Try with transformation default database
	if s.config.Transformation.DefaultDatabase != "" {
		fullID := fmt.Sprintf("%s.%s", s.config.Transformation.DefaultDatabase, overrideKey)
		if modelType, exists := modelTypes[fullID]; exists {
			return override.ResolveConfig(modelType)
		}
	}

	return fmt.Errorf("%w: %s", ErrOverrideModelNotFound, overrideKey)
}

// findOverride looks up an override using both full ID and table-only formats
func (s *service) findOverride(fullID, tableName string, modelType ModelType) (override *ModelOverride, overrideKey string) {
	// First, try with the full model ID (database.table)
	if override, exists := s.config.Overrides[fullID]; exists {
		return override, fullID
	}

	// If the model uses the default database, also try with just the table name
	// This allows more intuitive overrides when using default databases
	var defaultDatabase string
	if modelType == ModelTypeExternal {
		defaultDatabase = s.config.External.DefaultDatabase
	} else {
		defaultDatabase = s.config.Transformation.DefaultDatabase
	}

	if defaultDatabase != "" {
		// Check if this model is using the default database
		if fullID == fmt.Sprintf("%s.%s", defaultDatabase, tableName) {
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
