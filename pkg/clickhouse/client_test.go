package clickhouse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClient_QueryOne(t *testing.T) {
	type testResult struct {
		Count int `json:"count"`
	}

	tests := []struct {
		name           string
		query          string
		serverResponse string
		serverStatus   int
		expectedResult *testResult
		expectedError  bool
	}{
		{
			name:  "successful query with result",
			query: "SELECT count FROM test_table",
			serverResponse: `{
				"data": [{"count": 42}],
				"meta": [{"name": "count", "type": "UInt64"}],
				"rows": 1,
				"rows_read": 1
			}`,
			serverStatus:   http.StatusOK,
			expectedResult: &testResult{Count: 42},
			expectedError:  false,
		},
		{
			name:  "query with no results",
			query: "SELECT count FROM test_table WHERE id = 999",
			serverResponse: `{
				"data": [],
				"meta": [{"name": "count", "type": "UInt64"}],
				"rows": 0,
				"rows_read": 0
			}`,
			serverStatus:   http.StatusOK,
			expectedResult: nil,
			expectedError:  false,
		},
		{
			name:           "server error",
			query:          "SELECT invalid",
			serverResponse: `{"exception": "DB::Exception: Unknown identifier: invalid"}`,
			serverStatus:   http.StatusBadRequest,
			expectedResult: nil,
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "POST", r.Method)
				assert.Contains(t, r.Header.Get("Content-Type"), "text/plain")

				w.WriteHeader(tt.serverStatus)
				_, _ = w.Write([]byte(tt.serverResponse))
			}))
			defer server.Close()

			// Create client
			logger := logrus.New()
			logger.SetLevel(logrus.ErrorLevel)
			client, err := NewClient(logger, &Config{
				URL: server.URL,
			})
			require.NoError(t, err)

			// Execute query
			var result testResult
			err = client.QueryOne(context.Background(), tt.query, &result)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expectedResult != nil {
					assert.Equal(t, *tt.expectedResult, result)
				}
			}
		})
	}
}

func TestHTTPClient_QueryMany(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := `{
			"data": [
				{"id": 1, "name": "first"},
				{"id": 2, "name": "second"},
				{"id": 3, "name": "third"}
			],
			"meta": [
				{"name": "id", "type": "UInt64"},
				{"name": "name", "type": "String"}
			],
			"rows": 3,
			"rows_read": 3
		}`
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(response))
	}))
	defer server.Close()

	// Create client
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	client, err := NewClient(logger, &Config{
		URL: server.URL,
	})
	require.NoError(t, err)

	// Execute query
	type testRow struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var results []testRow
	err = client.QueryMany(context.Background(), "SELECT id, name FROM test_table", &results)
	require.NoError(t, err)

	// Verify results
	assert.Len(t, results, 3)
	assert.Equal(t, 1, results[0].ID)
	assert.Equal(t, "first", results[0].Name)
	assert.Equal(t, 2, results[1].ID)
	assert.Equal(t, "second", results[1].Name)
}

func TestHTTPClient_BulkInsert(t *testing.T) {
	var receivedBody string

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)

		// Read body
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		receivedBody = string(body)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	}))
	defer server.Close()

	// Create client
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	client, err := NewClient(logger, &Config{
		URL: server.URL,
	})
	require.NoError(t, err)

	// Test data
	type testData struct {
		ID        int    `json:"id"`
		Name      string `json:"name"`
		Timestamp int64  `json:"timestamp"`
	}

	data := []testData{
		{ID: 1, Name: "first", Timestamp: 1000},
		{ID: 2, Name: "second", Timestamp: 2000},
		{ID: 3, Name: "third", Timestamp: 3000},
	}

	// Execute bulk insert
	err = client.BulkInsert(context.Background(), "test_table", data)
	require.NoError(t, err)

	// Verify the request body
	assert.Contains(t, receivedBody, "INSERT INTO test_table FORMAT JSONEachRow")
	assert.Contains(t, receivedBody, `{"id":1,"name":"first","timestamp":1000}`)
	assert.Contains(t, receivedBody, `{"id":2,"name":"second","timestamp":2000}`)
	assert.Contains(t, receivedBody, `{"id":3,"name":"third","timestamp":3000}`)
}

func TestHTTPClient_Execute(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	}))
	defer server.Close()

	// Create client
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	client, err := NewClient(logger, &Config{
		URL: server.URL,
	})
	require.NoError(t, err)

	// Execute command
	err = client.Execute(context.Background(), "CREATE TABLE test_table (id UInt64) ENGINE = Memory")
	require.NoError(t, err)
}

func TestHTTPClient_Timeouts(t *testing.T) {
	// Create a slow server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with short timeout
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	client, err := NewClient(logger, &Config{
		URL:          server.URL,
		QueryTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	// Execute query - should timeout
	ctx := context.Background()
	var result struct{}
	err = client.QueryOne(ctx, "SELECT 1", &result)
	assert.Error(t, err)
	// The timeout causes the request to fail - we just need to ensure an error occurred
}

func TestHTTPClient_StartStop(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok."))
	}))
	defer server.Close()

	// Create client
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	client, err := NewClient(logger, &Config{
		URL: server.URL,
	})
	require.NoError(t, err)

	// Start should succeed
	err = client.Start()
	assert.NoError(t, err)

	// Stop should succeed
	err = client.Stop()
	assert.NoError(t, err)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config",
			config: Config{
				URL: "http://localhost:8123",
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
		URL: "http://localhost:8123",
	}

	config.SetDefaults()

	assert.Equal(t, 30*time.Second, config.QueryTimeout)
	assert.Equal(t, 5*time.Minute, config.InsertTimeout)
	assert.Equal(t, 30*time.Second, config.KeepAlive)
}

func TestHTTPClient_DebugLogging(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := `{"data": [{"value": 1}], "meta": [], "rows": 1}`
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(response))
	}))
	defer server.Close()

	// Create client with debug enabled
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	client, err := NewClient(logger, &Config{
		URL:   server.URL,
		Debug: true,
	})
	require.NoError(t, err)

	// Execute query - debug logging should not cause errors
	var result struct {
		Value int `json:"value"`
	}
	err = client.QueryOne(context.Background(), "SELECT 1 as value", &result)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.Value)
}
