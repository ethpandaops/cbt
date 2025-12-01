package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"
)

// Protocol constants for URL parsing.
const (
	SchemeClickHouse        = "clickhouse"
	SchemeHTTPS             = "https"
	SchemeHTTP              = "http"
	PortClickHouseNative    = "9000"
	PortClickHouseNativeTLS = "9440"
)

// Define static errors.
var (
	ErrDestMustBePointerToSlice = errors.New("dest must be a pointer to a slice")
	ErrDestMustBePointer        = errors.New("dest must be a pointer")
	ErrDataMustBeSlice          = errors.New("data must be a slice")
	ErrClickHouseResponse       = errors.New("clickhouse error")
)

// ClientInterface defines the methods for interacting with ClickHouse.
type ClientInterface interface {
	// QueryOne executes a query and returns a single result
	QueryOne(ctx context.Context, query string, dest any) error
	// QueryMany executes a query and returns multiple results
	QueryMany(ctx context.Context, query string, dest any) error
	// Execute runs a query without returning results (INSERT, ALTER, etc.)
	Execute(ctx context.Context, query string) error
	// BulkInsert performs a bulk insert operation
	BulkInsert(ctx context.Context, table string, data any) error
	// Start initializes the client
	Start() error
	// Stop closes the client
	Stop() error
}

// client implements the ClientInterface using the official ClickHouse driver.
type client struct {
	log           logrus.FieldLogger
	conn          driver.Conn
	debug         bool
	queryTimeout  time.Duration
	insertTimeout time.Duration
}

// NewClient creates a new ClickHouse client using the official Go driver.
func NewClient(logger *logrus.Logger, cfg *Config) (ClientInterface, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	cfg.SetDefaults()

	options, err := createClickHouseOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create options: %w", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	c := &client{
		log:           logger.WithField("component", "clickhouse"),
		conn:          conn,
		debug:         cfg.Debug,
		queryTimeout:  cfg.QueryTimeout,
		insertTimeout: cfg.InsertTimeout,
	}

	return c, nil
}

func (c *client) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	c.log.Info("Connected to ClickHouse")

	return nil
}

func (c *client) Stop() error {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			c.log.WithError(err).Warn("Failed to close ClickHouse connection")

			return err
		}
	}

	c.log.Info("Closed ClickHouse connection")

	return nil
}

func (c *client) QueryOne(ctx context.Context, query string, dest any) error {
	ctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	if c.debug {
		c.log.WithField("query", truncateQuery(query)).Debug("Executing query")
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return ErrDestMustBePointer
	}

	// Create a slice of the destination type to use with Select
	elemType := destVal.Elem().Type()
	sliceType := reflect.SliceOf(elemType)
	slicePtr := reflect.New(sliceType)

	if err := c.conn.Select(ctx, slicePtr.Interface(), query); err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	resultSlice := slicePtr.Elem()
	if resultSlice.Len() > 0 {
		destVal.Elem().Set(resultSlice.Index(0))
	}

	return nil
}

func (c *client) QueryMany(ctx context.Context, query string, dest any) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
		return ErrDestMustBePointerToSlice
	}

	ctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	if c.debug {
		c.log.WithField("query", truncateQuery(query)).Debug("Executing query")
	}

	if err := c.conn.Select(ctx, dest, query); err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	return nil
}

func (c *client) Execute(ctx context.Context, query string) error {
	ctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	if c.debug {
		c.log.WithField("query", truncateQuery(query)).Debug("Executing query")
	}

	if err := c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}

	return nil
}

func (c *client) BulkInsert(ctx context.Context, table string, data any) error {
	dataVal := reflect.ValueOf(data)
	if dataVal.Kind() != reflect.Slice {
		return ErrDataMustBeSlice
	}

	if dataVal.Len() == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, c.insertTimeout)
	defer cancel()

	if c.debug {
		c.log.WithFields(logrus.Fields{
			"table": table,
			"rows":  dataVal.Len(),
		}).Debug("Executing bulk insert")
	}

	batch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for i := 0; i < dataVal.Len(); i++ {
		item := dataVal.Index(i).Interface()
		if err := batch.AppendStruct(item); err != nil {
			return fmt.Errorf("failed to append row %d: %w", i, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// createClickHouseOptions builds connection options from config.
func createClickHouseOptions(cfg *Config) (*clickhouse.Options, error) {
	parsedURL, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Determine protocol based on scheme
	protocol := clickhouse.Native
	if parsedURL.Scheme == SchemeHTTP || parsedURL.Scheme == SchemeHTTPS {
		protocol = clickhouse.HTTP
	}

	// Extract auth from URL
	auth := clickhouse.Auth{}
	if parsedURL.User != nil {
		auth.Username = parsedURL.User.Username()
		if pw, ok := parsedURL.User.Password(); ok {
			auth.Password = pw
		}
	}

	// Database from URL path (optional, CBT uses fully-qualified table names)
	auth.Database = strings.TrimPrefix(parsedURL.Path, "/")

	options := &clickhouse.Options{
		Addr:     []string{parsedURL.Host},
		Auth:     auth,
		Protocol: protocol,
		Settings: clickhouse.Settings{
			"max_execution_time": int(cfg.QueryTimeout.Seconds()),
		},
		DialTimeout: 10 * time.Second,
		ReadTimeout: cfg.QueryTimeout,
	}

	// Connection pool settings
	if cfg.MaxOpenConns > 0 {
		options.MaxOpenConns = cfg.MaxOpenConns
	}

	if cfg.MaxIdleConns > 0 {
		options.MaxIdleConns = cfg.MaxIdleConns
	}

	if cfg.ConnMaxLifetime > 0 {
		options.ConnMaxLifetime = cfg.ConnMaxLifetime
	}

	// TLS for HTTPS or native TLS port (9440)
	if parsedURL.Scheme == SchemeHTTPS || parsedURL.Port() == PortClickHouseNativeTLS {
		options.TLS = &tls.Config{
			InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // configurable for dev environments
		}
	}

	return options, nil
}

// truncateQuery truncates long queries for logging.
func truncateQuery(query string) string {
	const maxLen = 500
	if len(query) > maxLen {
		return query[:maxLen] + "..."
	}

	return query
}
