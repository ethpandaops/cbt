package manager

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/sirupsen/logrus"
)

// Manager handles model management operations
type Manager struct {
	chClient     clickhouse.ClientInterface
	adminManager *clickhouse.AdminTableManager
	pathConfig   *models.PathConfig
	logger       *logrus.Entry
}

// NewManager creates a new models manager
func NewManager(chConfig *clickhouse.Config, pathConfig *models.PathConfig, logger *logrus.Logger) (*Manager, error) {
	log := logger.WithField("component", "models-manager")

	// Initialize ClickHouse client with admin manager
	chClient, adminManager, err := clickhouse.SetupClientWithAdmin(chConfig, logger)
	if err != nil {
		return nil, err
	}

	return &Manager{
		chClient:     chClient,
		adminManager: adminManager,
		pathConfig:   pathConfig,
		logger:       log,
	}, nil
}

// Close cleanly shuts down the manager
func (m *Manager) Close() error {
	if err := m.chClient.Stop(); err != nil {
		m.logger.WithError(err).Error("Failed to stop ClickHouse client")
		return fmt.Errorf("failed to stop ClickHouse client: %w", err)
	}
	return nil
}

// GetClickHouseClient returns the ClickHouse client
func (m *Manager) GetClickHouseClient() clickhouse.ClientInterface {
	return m.chClient
}

// GetAdminManager returns the admin table manager
func (m *Manager) GetAdminManager() *clickhouse.AdminTableManager {
	return m.adminManager
}

// GetContext returns a context for operations
func (m *Manager) GetContext() context.Context {
	return context.Background()
}
