// Package validation provides dependency validation for CBT models
package validation

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// ExternalModelExecutorImpl implements the ExternalModelExecutor interface
type ExternalModelExecutorImpl struct {
	chClient clickhouse.ClientInterface
	logger   *logrus.Logger
}

// NewExternalModelExecutor creates a new external model executor
func NewExternalModelExecutor(chClient clickhouse.ClientInterface, logger *logrus.Logger) *ExternalModelExecutorImpl {
	return &ExternalModelExecutorImpl{
		chClient: chClient,
		logger:   logger,
	}
}

// GetMinMax retrieves the min and max position values for an external model
func (e *ExternalModelExecutorImpl) GetMinMax(ctx context.Context, modelConfig *models.ModelConfig) (minPos, maxPos uint64, err error) {
	// Build the query to get min/max from the external table
	// Convert DateTime to Unix timestamp if needed
	query := fmt.Sprintf(`
		SELECT 
			coalesce(toUnixTimestamp(min(%s)), 0) as min_pos,
			coalesce(toUnixTimestamp(max(%s)), 0) as max_pos
		FROM %s.%s
	`, modelConfig.Partition, modelConfig.Partition, modelConfig.Database, modelConfig.Table)

	var result struct {
		MinPos uint64 `json:"min_pos"`
		MaxPos uint64 `json:"max_pos"`
	}

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		return 0, 0, fmt.Errorf("failed to get min/max for external model %s.%s: %w",
			modelConfig.Database, modelConfig.Table, err)
	}

	e.logger.WithFields(logrus.Fields{
		"model":   fmt.Sprintf("%s.%s", modelConfig.Database, modelConfig.Table),
		"min_pos": result.MinPos,
		"max_pos": result.MaxPos,
	}).Debug("Retrieved external model bounds")

	return result.MinPos, result.MaxPos, nil
}

// HasDataInRange checks if an external model has any data in the specified range
func (e *ExternalModelExecutorImpl) HasDataInRange(ctx context.Context, modelConfig *models.ModelConfig, startPos, endPos uint64) (bool, error) {
	// Check if ANY data exists in the specified range
	query := fmt.Sprintf(`
		SELECT count() > 0 as has_data
		FROM %s.%s
		WHERE toUnixTimestamp(%s) >= %d
		  AND toUnixTimestamp(%s) < %d
		LIMIT 1
	`, modelConfig.Database, modelConfig.Table,
		modelConfig.Partition, startPos,
		modelConfig.Partition, endPos)

	var result struct {
		HasData int `json:"has_data"`
	}

	if err := e.chClient.QueryOne(ctx, query, &result); err != nil {
		return false, fmt.Errorf("failed to check data existence: %w", err)
	}

	return result.HasData > 0, nil
}
