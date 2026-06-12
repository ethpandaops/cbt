package testutil

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// NewMiniredisClient returns both a miniredis server and a connected client.
// Miniredis provides an in-memory Redis for unit tests (no Docker needed).
// Both are automatically closed when the test completes.
func NewMiniredisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close miniredis client: %v", err)
		}
	})

	return mr, client
}
