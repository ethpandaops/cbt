package clickhouse

import "context"

// ClientInterface defines the methods for interacting with ClickHouse
type ClientInterface interface {
	// QueryOne executes a query and returns a single result
	QueryOne(ctx context.Context, query string, dest interface{}) error
	// QueryMany executes a query and returns multiple results
	QueryMany(ctx context.Context, query string, dest interface{}) error
	// Execute runs a query without expecting results
	Execute(ctx context.Context, query string) error
	// BulkInsert performs a bulk insert operation
	BulkInsert(ctx context.Context, table string, data interface{}) error
	// IsStorageEmpty checks if a table has any records matching the given conditions
	IsStorageEmpty(ctx context.Context, table string, conditions map[string]interface{}) (bool, error)
	// Start initializes the client
	Start() error
	// Stop closes the client
	Stop() error
}
