package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/heimdalr/dag"
)

var (
	// ErrMissingDependency indicates a model has a missing dependency
	ErrMissingDependency = errors.New("missing dependency")
	// ErrCircularDependency indicates circular dependencies detected
	ErrCircularDependency = errors.New("circular dependency detected")
	// ErrTableNotFound indicates table does not exist
	ErrTableNotFound = errors.New("table does not exist")
)

// ValidateModels validates all model configurations
func (m *Manager) ValidateModels(ctx context.Context) (*ValidationResult, error) {
	// Discover models
	discovery := models.NewModelDiscovery("./models")
	modelFiles, err := discovery.DiscoverAll()
	if err != nil {
		return nil, fmt.Errorf("failed to discover models: %w", err)
	}

	parser := models.NewModelParser()
	modelConfigs := make(map[string]models.ModelConfig)
	validCount := 0
	errorCount := 0
	validationErrors := make(map[string]error)

	// Parse all models and check basic validation
	for _, file := range modelFiles {
		modelConfig, parseErr := parser.Parse(file)
		if parseErr != nil {
			validationErrors[file.FilePath] = parseErr
			errorCount++
			continue
		}

		modelID := fmt.Sprintf("%s.%s", modelConfig.Database, modelConfig.Table)
		modelConfigs[modelID] = modelConfig

		// Validate external models
		if modelConfig.External {
			if err := m.validateExternalModel(ctx, &modelConfig); err != nil {
				validationErrors[modelID] = err
				errorCount++
				continue
			}
		}

		validCount++
	}

	// Validate dependencies
	depErrors := m.validateDependencies(modelConfigs)
	for modelID, err := range depErrors {
		validationErrors[modelID] = err
	}

	// Convert to interface map for generic use
	genericConfigs := make(map[string]interface{})
	for k := range modelConfigs {
		v := modelConfigs[k]
		genericConfigs[k] = v
	}

	return &ValidationResult{
		ValidCount:   validCount,
		ErrorCount:   errorCount + len(depErrors),
		ModelConfigs: genericConfigs,
	}, nil
}

func (m *Manager) validateExternalModel(ctx context.Context, modelConfig *models.ModelConfig) error {
	tableExists, err := clickhouse.TableExists(ctx, m.chClient, modelConfig.Database, modelConfig.Table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if !tableExists {
		return ErrTableNotFound
	}
	return nil
}

func (m *Manager) validateDependencies(modelConfigs map[string]models.ModelConfig) map[string]error {
	depErrors := make(map[string]error)

	// Check for missing dependencies
	for modelID := range modelConfigs {
		modelConfig := modelConfigs[modelID]
		for _, dep := range modelConfig.Dependencies {
			if _, exists := modelConfigs[dep]; !exists {
				depErrors[modelID] = fmt.Errorf("%w: %s", ErrMissingDependency, dep)
			}
		}
	}

	// Check for circular dependencies using DAG
	d := dag.NewDAG()
	for modelID := range modelConfigs {
		_ = d.AddVertexByID(modelID, modelID)
	}

	for modelID := range modelConfigs {
		modelConfig := modelConfigs[modelID]
		for _, dep := range modelConfig.Dependencies {
			if _, exists := modelConfigs[dep]; exists {
				if err := d.AddEdge(dep, modelID); err != nil {
					depErrors[modelID] = fmt.Errorf("%w: %s -> %s", ErrCircularDependency, dep, modelID)
				}
			}
		}
	}

	return depErrors
}

// GetValidationErrors returns validation errors as a map
func (m *Manager) GetValidationErrors(ctx context.Context) (map[string]error, error) {
	result, err := m.ValidateModels(ctx)
	if err != nil {
		return nil, err
	}

	validationErrs := make(map[string]error)
	// Extract errors from validation result
	// This would need to be enhanced based on actual validation implementation
	if result.ErrorCount > 0 {
		validationErrs["validation"] = fmt.Errorf("%w: count=%d", models.ErrValidationFailed, result.ErrorCount)
	}

	return validationErrs, nil
}
