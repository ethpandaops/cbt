//go:build integration

package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisConnection holds connection details for test containers.
type RedisConnection struct {
	Client        *redis.Client
	Options       *redis.Options
	ConnectionURL string // redis://host:port
}

// NewRedisContainer starts a Redis container and returns connection details.
// The container is automatically terminated when the test completes.
func NewRedisContainer(t *testing.T) *RedisConnection {
	t.Helper()

	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate Redis container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	connURL := fmt.Sprintf("redis://%s:%s", host, port.Port())

	opts := &redis.Options{
		Addr: fmt.Sprintf("%s:%s", host, port.Port()),
	}

	client := redis.NewClient(opts)

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close Redis client: %v", err)
		}
	})

	return &RedisConnection{
		Client:        client,
		Options:       opts,
		ConnectionURL: connURL,
	}
}
