//go:build integration

package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

// ClickHouseConnection holds connection details for test containers.
type ClickHouseConnection struct {
	URL      string // clickhouse://default@host:port/test_db
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

// NewClickHouseContainer starts a ClickHouse container and returns connection details.
// The container is automatically terminated when the test completes.
func NewClickHouseContainer(t *testing.T) *ClickHouseConnection {
	t.Helper()

	ctx := context.Background()

	container, err := clickhouse.Run(ctx,
		"clickhouse/clickhouse-server:25.5.10",
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword("test_password"),
		clickhouse.WithDatabase("test_db"),
	)
	if err != nil {
		t.Fatalf("failed to start ClickHouse container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate ClickHouse container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	return &ClickHouseConnection{
		URL:      fmt.Sprintf("clickhouse://default:test_password@%s:%s/test_db", host, port.Port()),
		Host:     host,
		Port:     port.Port(),
		Database: "test_db",
		Username: "default",
		Password: "test_password",
	}
}
