// Package operations provides write operations for models including rerun functionality
package operations

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// Manager handles rerun operations
type Manager struct {
	chClient     clickhouse.ClientInterface
	adminManager *clickhouse.AdminTableManager
	modelConfigs map[string]models.ModelConfig
	logger       *logrus.Entry
}

// Options contains options for rerun operations
type Options struct {
	ModelID string
	Start   uint64
	End     uint64
}

// NewManager creates a new rerun manager
func NewManager(chConfig *clickhouse.Config, logger *logrus.Logger) (*Manager, error) {
	log := logger.WithField("component", "rerun-manager")

	// Initialize ClickHouse client with admin manager
	chClient, adminManager, err := clickhouse.SetupClientWithAdmin(chConfig, logger)
	if err != nil {
		return nil, err
	}

	// Load model configurations
	modelConfigs, err := models.LoadAllModels(log)
	if err != nil {
		_ = chClient.Stop()
		return nil, err
	}

	return &Manager{
		chClient:     chClient,
		adminManager: adminManager,
		modelConfigs: modelConfigs,
		logger:       log,
	}, nil
}

// Execute performs the rerun operation by invalidating completion records
func (m *Manager) Execute(ctx context.Context, opts *Options) error {
	// Validate model exists
	modelCfg, exists := m.modelConfigs[opts.ModelID]
	if !exists {
		return fmt.Errorf("%w: %s", models.ErrModelNotFound, opts.ModelID)
	}

	m.logger.WithFields(logrus.Fields{
		"model": opts.ModelID,
		"start": opts.Start,
		"end":   opts.End,
	}).Info("Starting rerun invalidation")

	// Build list of models to invalidate - always includes dependents
	modelsToInvalidate := []string{opts.ModelID}
	dependents := FindDependentModels(opts.ModelID, m.modelConfigs)
	modelsToInvalidate = append(modelsToInvalidate, dependents...)

	m.logger.WithField("models", modelsToInvalidate).Info("Invalidating completion records")

	// Delete completion records for each affected model
	for _, modelID := range modelsToInvalidate {
		err := m.adminManager.DeleteRange(ctx, modelID, opts.Start, opts.End)
		if err != nil {
			m.logger.WithError(err).WithField("model", modelID).Error("Failed to delete completion records")
			// Continue with other models even if one fails
		}
		m.logger.WithField("model", modelID).Info("Invalidated completion records")
	}

	// Provide clear feedback about what happens next
	backfillMsg := "Rerun invalidation complete. Gaps will be detected and filled by backfill processes"
	if modelCfg.Backfill != nil && modelCfg.Backfill.Enabled {
		backfillMsg = fmt.Sprintf("Rerun invalidation complete. Gaps will be filled by backfill (schedule: %s)", modelCfg.Backfill.Schedule)
	}
	m.logger.Info(backfillMsg)

	return nil
}

// Close cleanly shuts down the manager
func (m *Manager) Close() error {
	chErr := m.chClient.Stop()
	if chErr != nil {
		m.logger.WithError(chErr).Error("Failed to stop ClickHouse client")
		return fmt.Errorf("failed to stop ClickHouse client: %w", chErr)
	}
	return nil
}
