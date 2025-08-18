package clickhouse

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// SetupClientWithAdmin creates and starts a ClickHouse client with admin table manager
func SetupClientWithAdmin(chConfig *Config, logger *logrus.Logger) (ClientInterface, *AdminTableManager, error) {
	// Create ClickHouse client
	chClient, err := New(&Config{
		URL: chConfig.URL,
	}, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// Start the client
	if startErr := chClient.Start(); startErr != nil {
		return nil, nil, fmt.Errorf("failed to start ClickHouse client: %w", startErr)
	}

	// Create admin table manager
	adminManager := NewAdminTableManager(chClient, chConfig.Cluster)

	return chClient, adminManager, nil
}

// SetupClient creates and starts a ClickHouse client (without admin manager)
func SetupClient(chConfig *Config, logger *logrus.Logger) (ClientInterface, error) {
	chClient, err := New(&Config{
		URL: chConfig.URL,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	if startErr := chClient.Start(); startErr != nil {
		return nil, fmt.Errorf("failed to start ClickHouse client: %w", startErr)
	}

	return chClient, nil
}
