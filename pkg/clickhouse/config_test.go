package clickhouse

import (
	"os"
	"testing"
)

func TestMapDatabase(t *testing.T) {
	c := &Config{}

	// Test without prefix
	result := c.MapDatabase("mainnet")
	if result != "mainnet" {
		t.Errorf("MapDatabase without prefix: expected 'mainnet', got '%s'", result)
	}

	// Test with prefix
	os.Setenv("CBT_DATABASE_PREFIX", "test_123_")
	defer os.Unsetenv("CBT_DATABASE_PREFIX")

	result = c.MapDatabase("mainnet")
	if result != "test_123_mainnet" {
		t.Errorf("MapDatabase with prefix: expected 'test_123_mainnet', got '%s'", result)
	}

	// Test admin database mapping
	result = c.MapDatabase("admin")
	if result != "test_123_admin" {
		t.Errorf("MapDatabase with prefix for admin: expected 'test_123_admin', got '%s'", result)
	}

	// Test empty database name
	result = c.MapDatabase("")
	if result != "test_123_" {
		t.Errorf("MapDatabase with empty name: expected 'test_123_', got '%s'", result)
	}

	// Clear prefix and test again
	os.Unsetenv("CBT_DATABASE_PREFIX")
	result = c.MapDatabase("mainnet")
	if result != "mainnet" {
		t.Errorf("MapDatabase after clearing prefix: expected 'mainnet', got '%s'", result)
	}
}
