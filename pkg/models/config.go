package models

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	// ErrDatabaseRequired is returned when database field is missing
	ErrDatabaseRequired = errors.New("database is required")
	// ErrTableRequired is returned when table field is missing
	ErrTableRequired = errors.New("table is required")
	// ErrPartitionRequired is returned when partition field is missing
	ErrPartitionRequired = errors.New("partition is required")
	// ErrExternalContentRequired is returned when external model has no content
	ErrExternalContentRequired = errors.New("external models must have SQL content or exec command")
	// ErrExternalNoInterval is returned when external model has interval
	ErrExternalNoInterval = errors.New("external models cannot have interval")
	// ErrExternalNoSchedule is returned when external model has schedule
	ErrExternalNoSchedule = errors.New("external models cannot have schedule")
	// ErrExternalNoDependencies is returned when external model has dependencies
	ErrExternalNoDependencies = errors.New("external models cannot have dependencies")
	// ErrTransformationIntervalRequired is returned when transformation model has no interval
	ErrTransformationIntervalRequired = errors.New("transformation models must have interval")
	// ErrTransformationScheduleRequired is returned when transformation model has no schedule
	ErrTransformationScheduleRequired = errors.New("transformation models must have schedule")
	// ErrTransformationContentRequired is returned when transformation model has no content
	ErrTransformationContentRequired = errors.New("transformation models must have SQL content or exec command")
	// ErrTransformationNoTTL is returned when transformation model has TTL
	ErrTransformationNoTTL = errors.New("transformation models cannot have TTL")
	// ErrTransformationNoLag is returned when transformation model has lag
	ErrTransformationNoLag = errors.New("lag can only be configured on external models")
	// ErrLagTooLarge is returned when lag value is unreasonably large
	ErrLagTooLarge = errors.New("lag value is too large (max: 86400 seconds / 24 hours)")
	// ErrBackfillScheduleRequired is returned when backfill is enabled but no schedule is provided
	ErrBackfillScheduleRequired = errors.New("backfill.schedule is required when backfill.enabled is true")
	// ErrBackfillInvalidSchedule is returned when backfill schedule format is invalid
	ErrBackfillInvalidSchedule = errors.New("backfill.schedule must be a valid cron expression or @every format")
	// ErrBackfillInvalidDuration is returned when @every duration is invalid
	ErrBackfillInvalidDuration = errors.New("invalid @every duration format")
	// ErrBackfillInvalidCron is returned when cron expression is invalid
	ErrBackfillInvalidCron = errors.New("cron expression should have 5 or 6 fields")
)

// ModelParser parses model files and extracts configuration
type ModelParser struct{}

// NewModelParser creates a new model parser
func NewModelParser() *ModelParser {
	return &ModelParser{}
}

// parseFrontmatter parses YAML frontmatter from content
func (p *ModelParser) parseFrontmatter(content []byte, filePath string) (ModelConfig, string, error) {
	parts := bytes.SplitN(content, []byte("\n---\n"), 2)
	if len(parts) != 2 {
		return ModelConfig{}, "", fmt.Errorf("%w in %s", ErrInvalidFrontmatter, filePath)
	}

	var config ModelConfig
	// Parse YAML frontmatter (skip "---\n" prefix)
	if err := yaml.Unmarshal(parts[0][4:], &config); err != nil {
		return ModelConfig{}, "", fmt.Errorf("failed to parse frontmatter in %s: %w", filePath, err)
	}

	return config, string(parts[1]), nil
}

// parseContent extracts config and SQL content from file content
func (p *ModelParser) parseContent(content []byte, filePath string) (ModelConfig, string, error) {
	// Check if file has YAML frontmatter
	if bytes.HasPrefix(content, []byte("---\n")) {
		return p.parseFrontmatter(content, filePath)
	}

	// Pure YAML file
	var config ModelConfig
	if err := yaml.Unmarshal(content, &config); err != nil {
		return ModelConfig{}, "", fmt.Errorf("failed to parse YAML in %s: %w", filePath, err)
	}

	return config, "", nil
}

// Parse parses a model file and returns its configuration
func (p *ModelParser) Parse(file ModelFile) (ModelConfig, error) {
	content, err := os.ReadFile(file.FilePath)
	if err != nil {
		return ModelConfig{}, fmt.Errorf("failed to read file %s: %w", file.FilePath, err)
	}

	config, sqlContent, err := p.parseContent(content, file.FilePath)
	if err != nil {
		return ModelConfig{}, err
	}

	// Set model type based on directory
	config.External = file.IsExternal

	// Store content for execution
	if config.Exec != "" {
		config.Content = config.Exec // Exec command
	} else {
		config.Content = strings.TrimSpace(sqlContent) // SQL content
	}

	// Validate configuration
	if err := p.validateConfig(&config); err != nil {
		return ModelConfig{}, fmt.Errorf("invalid configuration in %s: %w", file.FilePath, err)
	}

	return config, nil
}

func (p *ModelParser) validateConfig(config *ModelConfig) error {
	if config.Database == "" {
		return ErrDatabaseRequired
	}
	if config.Table == "" {
		return ErrTableRequired
	}
	if config.Partition == "" {
		return ErrPartitionRequired
	}

	if config.External {
		return p.validateExternalModel(config)
	}
	return p.validateTransformationModel(config)
}

// validateExternalModel validates an external model configuration
func (p *ModelParser) validateExternalModel(config *ModelConfig) error {
	if config.Content == "" && config.Exec == "" {
		return ErrExternalContentRequired
	}
	if config.Interval != 0 {
		return ErrExternalNoInterval
	}
	if config.Schedule != "" {
		return ErrExternalNoSchedule
	}
	if len(config.Dependencies) > 0 {
		return ErrExternalNoDependencies
	}
	// Validate lag if configured
	if config.Lag > 86400 { // Max 24 hours
		return ErrLagTooLarge
	}
	return nil
}

// validateTransformationModel validates a transformation model configuration
func (p *ModelParser) validateTransformationModel(config *ModelConfig) error {
	if config.Interval == 0 {
		return ErrTransformationIntervalRequired
	}
	if config.Schedule == "" {
		return ErrTransformationScheduleRequired
	}
	if config.Content == "" && config.Exec == "" {
		return ErrTransformationContentRequired
	}
	if config.TTL != 0 {
		return ErrTransformationNoTTL
	}
	if config.Lag != 0 {
		return ErrTransformationNoLag
	}

	// Validate backfill configuration if present
	if config.Backfill != nil {
		if err := p.validateBackfillConfig(config.Backfill); err != nil {
			return err
		}
	}

	return nil
}

// validateBackfillConfig validates a backfill configuration
func (p *ModelParser) validateBackfillConfig(backfill *BackfillConfig) error {
	if backfill == nil || !backfill.Enabled {
		// If nil or not enabled, no further validation needed
		return nil
	}

	// Schedule is required when enabled
	if backfill.Schedule == "" {
		return ErrBackfillScheduleRequired
	}

	// Validate schedule format
	if err := p.validateScheduleFormat(backfill.Schedule); err != nil {
		return fmt.Errorf("%w: %w", ErrBackfillInvalidSchedule, err)
	}

	return nil
}

// validateScheduleFormat validates that a schedule string is in valid format
func (p *ModelParser) validateScheduleFormat(schedule string) error {
	// Support @every format
	if strings.HasPrefix(schedule, "@every ") {
		duration := strings.TrimPrefix(schedule, "@every ")
		_, err := time.ParseDuration(duration)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBackfillInvalidDuration, err)
		}
		return nil
	}

	// Support other @ formats
	validAtFormats := []string{"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly"}
	for _, format := range validAtFormats {
		if schedule == format {
			return nil
		}
	}

	// For now, we'll assume other formats are cron expressions
	// Full cron validation would require a cron parser library
	// Basic check: should have 5 or 6 space-separated fields
	fields := strings.Fields(schedule)
	if len(fields) != 5 && len(fields) != 6 {
		return ErrBackfillInvalidCron
	}

	return nil
}

// GetModelID returns the model ID for a configuration
func (p *ModelParser) GetModelID(config *ModelConfig) string {
	return fmt.Sprintf("%s.%s", config.Database, config.Table)
}
