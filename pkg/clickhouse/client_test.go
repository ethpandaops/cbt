package clickhouse

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config with native URL",
			config: Config{
				URL: "clickhouse://localhost:9000",
			},
			expectError: false,
		},
		{
			name: "valid config with HTTP URL",
			config: Config{
				URL: "http://localhost:8123",
			},
			expectError: false,
		},
		{
			name: "valid config with HTTPS URL",
			config: Config{
				URL: "https://localhost:8443",
			},
			expectError: false,
		},
		{
			name:        "missing URL",
			config:      Config{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_SetDefaults(t *testing.T) {
	config := Config{
		URL: "clickhouse://localhost:9000",
	}

	config.SetDefaults()

	assert.Equal(t, 30*time.Second, config.QueryTimeout)
	assert.Equal(t, 5*time.Minute, config.InsertTimeout)
}

func TestCreateClickHouseOptions_NativeProtocol(t *testing.T) {
	cfg := &Config{
		URL:          "clickhouse://user:pass@localhost:9000/testdb",
		QueryTimeout: 30 * time.Second,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:9000"}, options.Addr)
	assert.Equal(t, clickhouse.Native, options.Protocol)
	assert.Equal(t, "user", options.Auth.Username)
	assert.Equal(t, "pass", options.Auth.Password)
	assert.Equal(t, "testdb", options.Auth.Database)
	assert.Nil(t, options.TLS)
}

func TestCreateClickHouseOptions_HTTPProtocol(t *testing.T) {
	cfg := &Config{
		URL:          "http://user:pass@localhost:8123/testdb",
		QueryTimeout: 30 * time.Second,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:8123"}, options.Addr)
	assert.Equal(t, clickhouse.HTTP, options.Protocol)
	assert.Equal(t, "user", options.Auth.Username)
	assert.Equal(t, "pass", options.Auth.Password)
	assert.Equal(t, "testdb", options.Auth.Database)
}

func TestCreateClickHouseOptions_HTTPSProtocol(t *testing.T) {
	cfg := &Config{
		URL:                "https://user:pass@localhost:8443/testdb",
		QueryTimeout:       30 * time.Second,
		InsecureSkipVerify: true,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	assert.Equal(t, clickhouse.HTTP, options.Protocol)
	assert.NotNil(t, options.TLS)
	assert.True(t, options.TLS.InsecureSkipVerify)
}

func TestCreateClickHouseOptions_NativeTLSPort(t *testing.T) {
	cfg := &Config{
		URL:                "clickhouse://user:pass@localhost:9440/testdb",
		QueryTimeout:       30 * time.Second,
		InsecureSkipVerify: false,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	assert.Equal(t, clickhouse.Native, options.Protocol)
	assert.NotNil(t, options.TLS)
	assert.False(t, options.TLS.InsecureSkipVerify)
}

func TestCreateClickHouseOptions_DatabaseFromConfig(t *testing.T) {
	cfg := &Config{
		URL:          "clickhouse://localhost:9000",
		Database:     "mydb",
		QueryTimeout: 30 * time.Second,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	assert.Equal(t, "mydb", options.Auth.Database)
}

func TestCreateClickHouseOptions_DatabaseFromURL(t *testing.T) {
	cfg := &Config{
		URL:          "clickhouse://localhost:9000/urldb",
		Database:     "configdb", // URL should take precedence
		QueryTimeout: 30 * time.Second,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	// URL database takes precedence
	assert.Equal(t, "urldb", options.Auth.Database)
}

func TestCreateClickHouseOptions_ConnectionPool(t *testing.T) {
	cfg := &Config{
		URL:             "clickhouse://localhost:9000",
		QueryTimeout:    30 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 1 * time.Hour,
	}

	options, err := createClickHouseOptions(cfg)
	require.NoError(t, err)

	assert.Equal(t, 10, options.MaxOpenConns)
	assert.Equal(t, 5, options.MaxIdleConns)
	assert.Equal(t, 1*time.Hour, options.ConnMaxLifetime)
}

func TestCreateClickHouseOptions_InvalidURL(t *testing.T) {
	cfg := &Config{
		URL: "://invalid",
	}

	_, err := createClickHouseOptions(cfg)
	assert.Error(t, err)
}

func TestTruncateQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "short query unchanged",
			query:    "SELECT 1",
			expected: "SELECT 1",
		},
		{
			name:     "exactly 500 chars unchanged",
			query:    string(make([]byte, 500)),
			expected: string(make([]byte, 500)),
		},
		{
			name:     "long query truncated",
			query:    string(make([]byte, 600)),
			expected: string(make([]byte, 500)) + "...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Integration tests require a running ClickHouse instance.
// Run with: go test -tags=integration -run=Integration
// These tests are skipped by default.

func TestIntegration_NewClient(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This test requires CLICKHOUSE_URL environment variable
	// Example: CLICKHOUSE_URL=clickhouse://localhost:9000 go test -run Integration
	t.Skip("Integration test - requires running ClickHouse instance")
}
