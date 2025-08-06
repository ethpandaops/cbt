package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Define static errors
var (
	ErrDestMustBePointerToSlice = errors.New("dest must be a pointer to a slice")
	ErrDataMustBeSlice          = errors.New("data must be a slice")
	ErrClickHouseResponse       = errors.New("clickhouse error")
)

const (
	statusSuccess = "success"
	statusFailed  = "failed"
)

// client implements the ClientInterface using HTTP
type client struct {
	log        logrus.FieldLogger
	httpClient *http.Client
	baseURL    string
	debug      bool
	lock       sync.RWMutex
}

// New creates a new HTTP-based ClickHouse client
func New(cfg *Config) (ClientInterface, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Set defaults
	cfg.SetDefaults()

	// Create HTTP client with keep-alive settings
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     cfg.KeepAlive,
		DisableKeepAlives:   false,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   0, // We'll set per-request timeouts
	}

	c := &client{
		log:        logrus.WithField("component", "clickhouse-http"),
		httpClient: httpClient,
		baseURL:    strings.TrimRight(cfg.URL, "/"),
		debug:      cfg.Debug,
	}

	return c, nil
}

func (c *client) Start() error {
	// Test connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Execute(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	c.log.Info("Connected to ClickHouse HTTP interface")

	return nil
}

func (c *client) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}

	c.log.Info("Closed ClickHouse HTTP client")

	return nil
}

func (c *client) QueryOne(ctx context.Context, query string, dest interface{}) error {
	// Add FORMAT JSON to query
	formattedQuery := query + " FORMAT JSON"

	resp, err := c.executeHTTPRequest(ctx, formattedQuery, c.getTimeout(ctx, "query"))
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Parse response
	var result struct {
		Data []json.RawMessage `json:"data"`
		Meta []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"meta"`
		Rows     int `json:"rows"`
		RowsRead int `json:"rows_read"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Data) == 0 {
		// No rows found, return without error but don't unmarshal
		return nil
	}

	// Unmarshal the first row into dest
	if err := json.Unmarshal(result.Data[0], dest); err != nil {
		return fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return nil
}

func (c *client) QueryMany(ctx context.Context, query string, dest interface{}) error {
	// Validate that dest is a pointer to a slice
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
		return ErrDestMustBePointerToSlice
	}

	// Add FORMAT JSON to query
	formattedQuery := query + " FORMAT JSON"

	resp, err := c.executeHTTPRequest(ctx, formattedQuery, c.getTimeout(ctx, "query"))
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Parse response
	var result struct {
		Data []json.RawMessage `json:"data"`
		Meta []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"meta"`
		Rows     int `json:"rows"`
		RowsRead int `json:"rows_read"`
	}

	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Create a slice of the appropriate type
	sliceType := destValue.Elem().Type()
	elemType := sliceType.Elem()
	newSlice := reflect.MakeSlice(sliceType, len(result.Data), len(result.Data))

	// Unmarshal each row
	for i, data := range result.Data {
		elem := reflect.New(elemType)
		if err := json.Unmarshal(data, elem.Interface()); err != nil {
			return fmt.Errorf("failed to unmarshal row %d: %w", i, err)
		}

		newSlice.Index(i).Set(elem.Elem())
	}

	// Set the result
	destValue.Elem().Set(newSlice)

	return nil
}

func (c *client) Execute(ctx context.Context, query string) error {
	_, err := c.executeHTTPRequest(ctx, query, c.getTimeout(ctx, "query"))
	if err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}

	return nil
}

func (c *client) BulkInsert(ctx context.Context, table string, data interface{}) error {
	// Convert data to slice via reflection
	dataValue := reflect.ValueOf(data)
	if dataValue.Kind() != reflect.Slice {
		return ErrDataMustBeSlice
	}

	if dataValue.Len() == 0 {
		return nil // Nothing to insert
	}

	// Build INSERT query with JSONEachRow format
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow\n", table))

	// Marshal each item as JSON
	for i := 0; i < dataValue.Len(); i++ {
		item := dataValue.Index(i).Interface()

		jsonData, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to marshal row %d: %w", i, err)
		}

		buf.Write(jsonData)
		buf.WriteByte('\n')
	}

	// Execute the insert
	_, err := c.executeHTTPRequest(ctx, buf.String(), c.getTimeout(ctx, "insert"))
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}

	return nil
}

func (c *client) executeHTTPRequest(ctx context.Context, query string, timeout time.Duration) ([]byte, error) {
	// Create request with timeout
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "POST", c.baseURL, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClickHouse-Format", "JSON")

	// Debug logging
	if c.debug {
		// For large inserts, truncate the query
		logQuery := query
		if len(query) > 1000 && strings.Contains(query, "INSERT") {
			logQuery = query[:1000] + "... (truncated)"
		}

		c.log.WithField("query", logQuery).Debug("Executing ClickHouse query")
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.log.WithError(closeErr).Debug("Failed to close response body")
		}
	}()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		// Try to parse error message
		var errorResp struct {
			Exception string `json:"exception"`
		}

		if jsonErr := json.Unmarshal(body, &errorResp); jsonErr == nil && errorResp.Exception != "" {
			return nil, fmt.Errorf("%w (status %d): %s", ErrClickHouseResponse, resp.StatusCode, errorResp.Exception)
		}

		return nil, fmt.Errorf("%w (status %d): %s", ErrClickHouseResponse, resp.StatusCode, string(body))
	}

	// Debug logging
	if c.debug && len(body) < 1000 {
		c.log.WithField("response", string(body)).Debug("ClickHouse response")
	}

	return body, nil
}

func (c *client) getTimeout(ctx context.Context, operation string) time.Duration {
	// Check if context already has a deadline
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}

	// Use default timeouts based on operation type
	switch operation {
	case "insert":
		return 5 * time.Minute
	case "query":
		return 30 * time.Second
	default:
		return 30 * time.Second
	}
}

func (c *client) IsStorageEmpty(ctx context.Context, table string, conditions map[string]interface{}) (bool, error) {
	// Build the query
	query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s FINAL", table)

	if len(conditions) > 0 {
		query += " WHERE "

		var conditionParts []string

		for key, value := range conditions {
			// Handle different value types
			switch v := value.(type) {
			case string:
				conditionParts = append(conditionParts, fmt.Sprintf("%s = '%s'", key, v))
			case int, int64, uint64:
				conditionParts = append(conditionParts, fmt.Sprintf("%s = %v", key, v))
			default:
				conditionParts = append(conditionParts, fmt.Sprintf("%s = '%v'", key, v))
			}
		}

		query += strings.Join(conditionParts, " AND ")
	}

	var result struct {
		Count uint64 `json:"count"`
	}

	if err := c.QueryOne(ctx, query, &result); err != nil {
		return false, fmt.Errorf("failed to check if table is empty: %w", err)
	}

	return result.Count == 0, nil
}

// extractTableName attempts to extract the table name from various SQL query patterns
func extractTableName(query string) string {
	// Work with original query to preserve case
	trimmedQuery := strings.TrimSpace(query)
	upperQuery := strings.ToUpper(trimmedQuery)

	// Handle INSERT INTO queries
	if strings.HasPrefix(upperQuery, "INSERT INTO") {
		parts := strings.Fields(trimmedQuery)
		if len(parts) >= 3 {
			// Return the third part which should be the table name
			// Handle cases where table name might have backticks or quotes
			return strings.Trim(parts[2], "`'\"")
		}
	}

	// Handle SELECT ... FROM queries
	if idx := strings.Index(upperQuery, "FROM"); idx != -1 {
		// Get the substring after FROM from the original query
		afterFrom := strings.TrimSpace(trimmedQuery[idx+4:])
		parts := strings.Fields(afterFrom)

		if len(parts) > 0 {
			// Return the first part which should be the table name
			// Remove any trailing keywords like WHERE, ORDER BY, etc.
			tableName := parts[0]
			// Handle FINAL keyword (e.g., "FROM table FINAL")
			if !strings.EqualFold(tableName, "FINAL") {
				return strings.Trim(tableName, "`'\"")
			}
		}
	}

	// Handle CREATE TABLE queries
	if strings.HasPrefix(upperQuery, "CREATE TABLE") {
		parts := strings.Fields(trimmedQuery)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "`'\"")
		}
	}

	// Handle DROP TABLE queries
	if strings.HasPrefix(upperQuery, "DROP TABLE") {
		parts := strings.Fields(trimmedQuery)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "`'\"")
		}
	}

	return ""
}
