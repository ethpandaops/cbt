package models

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// LoadAllModels discovers and parses all model configurations from the models directory
func LoadAllModels(logger *logrus.Entry) (map[string]ModelConfig, error) {
	discovery := NewModelDiscovery("./models")
	modelFiles, err := discovery.DiscoverAll()
	if err != nil {
		return nil, fmt.Errorf("failed to discover models: %w", err)
	}

	parser := NewModelParser()
	modelConfigs := make(map[string]ModelConfig)

	for _, file := range modelFiles {
		modelConfig, parseErr := parser.Parse(file)
		if parseErr != nil {
			logger.WithError(parseErr).WithField("file", file.FilePath).Error("Failed to parse model")
			continue
		}
		modelID := fmt.Sprintf("%s.%s", modelConfig.Database, modelConfig.Table)
		modelConfigs[modelID] = modelConfig
	}

	return modelConfigs, nil
}

// LoadModelsForDiscovery is a simplified model loader that doesn't require a logger
func LoadModelsForDiscovery() (map[string]ModelConfig, error) {
	discovery := NewModelDiscovery("./models")
	modelFiles, err := discovery.DiscoverAll()
	if err != nil {
		return nil, fmt.Errorf("failed to discover models: %w", err)
	}

	parser := NewModelParser()
	modelConfigs := make(map[string]ModelConfig)

	for _, file := range modelFiles {
		modelConfig, parseErr := parser.Parse(file)
		if parseErr != nil {
			// Skip invalid models silently
			continue
		}
		modelID := fmt.Sprintf("%s.%s", modelConfig.Database, modelConfig.Table)
		modelConfigs[modelID] = modelConfig
	}

	return modelConfigs, nil
}
