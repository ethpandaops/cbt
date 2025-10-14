package admin

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test service creation (ethPandaOps requirement)
func TestNewService(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	mockClient := &mockClickhouseClient{}
	cluster := "test_cluster"
	localSuffix := "_local"
	adminDatabase := "admin_db"
	adminTable := "admin_table"
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	config := TableConfig{
		IncrementalDatabase: adminDatabase,
		IncrementalTable:    adminTable,
		ScheduledDatabase:   adminDatabase,
		ScheduledTable:      "cbt_scheduled",
	}
	svc := NewService(log, mockClient, cluster, localSuffix, config, redisClient)
	assert.NotNil(t, svc)

	// Verify it implements the Service interface
	var _ = svc

	// Verify admin database and table are set
	assert.Equal(t, adminDatabase, svc.GetIncrementalAdminDatabase())
	assert.Equal(t, adminTable, svc.GetIncrementalAdminTable())
}

// Test RecordCompletion
func TestRecordCompletion(t *testing.T) {
	tests := []struct {
		name     string
		modelID  string
		position uint64
		interval uint64
		wantErr  bool
		errMatch error
	}{
		{
			name:     "valid model ID",
			modelID:  "database.table",
			position: 1000,
			interval: 100,
			wantErr:  false,
		},
		{
			name:     "invalid model ID - no dot",
			modelID:  "invalidmodel",
			position: 1000,
			interval: 100,
			wantErr:  true,
			errMatch: ErrInvalidModelID,
		},
		{
			name:     "invalid model ID - multiple dots",
			modelID:  "database.schema.table",
			position: 1000,
			interval: 100,
			wantErr:  true,
			errMatch: ErrInvalidModelID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			err := svc.RecordCompletion(ctx, tt.modelID, tt.position, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMatch != nil {
					assert.ErrorIs(t, err, tt.errMatch)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test GetLastProcessedEndPosition
func TestGetLastProcessedEndPosition(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		queryResult uint64
		queryError  error
		expectedPos uint64
		wantErr     bool
	}{
		{
			name:        "valid model with data",
			modelID:     "database.table",
			queryResult: 5000,
			expectedPos: 5000,
			wantErr:     false,
		},
		{
			name:        "valid model no data",
			modelID:     "database.empty",
			queryResult: 0,
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			expectedPos: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				queryResult: tt.queryResult,
				queryError:  tt.queryError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			pos, err := svc.GetLastProcessedEndPosition(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// Test new position tracking functions
func TestPositionTrackingFunctions(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		queryResult uint64
		queryError  error
		expectedPos uint64
		wantErr     bool
	}{
		{
			name:        "valid model with data",
			modelID:     "database.table",
			queryResult: 10000,
			expectedPos: 10000,
			wantErr:     false,
		},
		{
			name:        "valid model no data",
			modelID:     "database.empty",
			queryResult: 0,
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			expectedPos: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				queryResult: tt.queryResult,
				queryError:  tt.queryError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()

			// Test GetLastProcessedEndPosition
			endPos, err := svc.GetLastProcessedEndPosition(ctx, tt.modelID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, endPos)
			}

			// Test GetNextUnprocessedPosition (should be same as GetLastProcessedEndPosition)
			nextPos, err := svc.GetNextUnprocessedPosition(ctx, tt.modelID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, nextPos)
			}

			// Test GetLastProcessedPosition (returns max(position) not max(position+interval))
			lastPos, err := svc.GetLastProcessedPosition(ctx, tt.modelID)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// For this mock, it will return the same value since we're using a simple mock
				assert.Equal(t, tt.expectedPos, lastPos)
			}
		})
	}
}

// Test GetFirstPosition
func TestGetFirstPosition(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		queryResult uint64
		queryError  error
		expectedPos uint64
		wantErr     bool
	}{
		{
			name:        "valid model with data",
			modelID:     "database.table",
			queryResult: 100,
			expectedPos: 100,
			wantErr:     false,
		},
		{
			name:        "valid model no data",
			modelID:     "database.empty",
			queryResult: 0,
			expectedPos: 0,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			expectedPos: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				queryResult: tt.queryResult,
				queryError:  tt.queryError,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			pos, err := svc.GetFirstPosition(ctx, tt.modelID)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPos, pos)
			}
		})
	}
}

// Test FindGaps
func TestFindGaps(t *testing.T) {
	tests := []struct {
		name         string
		modelID      string
		minPos       uint64
		maxPos       uint64
		interval     uint64
		mockGaps     []GapInfo
		expectedGaps int
		wantErr      bool
	}{
		{
			name:     "finds large gaps",
			modelID:  "database.table",
			minPos:   0,
			maxPos:   1000,
			interval: 384,
			mockGaps: []GapInfo{
				{StartPos: 200, EndPos: 300},
				{StartPos: 500, EndPos: 600},
			},
			expectedGaps: 2,
			wantErr:      false,
		},
		{
			name:     "finds micro-gaps (12s)",
			modelID:  "database.table",
			minPos:   0,
			maxPos:   1000,
			interval: 384,
			mockGaps: []GapInfo{
				{StartPos: 100, EndPos: 112}, // 12s gap
				{StartPos: 200, EndPos: 224}, // 24s gap
				{StartPos: 300, EndPos: 336}, // 36s gap
			},
			expectedGaps: 3,
			wantErr:      false,
		},
		{
			name:     "finds all gap sizes",
			modelID:  "database.table",
			minPos:   0,
			maxPos:   2000,
			interval: 384,
			mockGaps: []GapInfo{
				{StartPos: 100, EndPos: 112},   // 12s
				{StartPos: 200, EndPos: 224},   // 24s
				{StartPos: 300, EndPos: 360},   // 60s
				{StartPos: 400, EndPos: 520},   // 120s
				{StartPos: 1000, EndPos: 1384}, // 384s (full interval)
			},
			expectedGaps: 5,
			wantErr:      false,
		},
		{
			name:         "no gaps found",
			modelID:      "database.table",
			minPos:       0,
			maxPos:       1000,
			interval:     384,
			mockGaps:     []GapInfo{},
			expectedGaps: 0,
			wantErr:      false,
		},
		{
			name:         "invalid model ID",
			modelID:      "invalid",
			minPos:       0,
			maxPos:       1000,
			interval:     384,
			mockGaps:     []GapInfo{},
			expectedGaps: 0,
			wantErr:      true,
		},
		{
			name:     "no false positive when scanning range overlapping with existing interval",
			modelID:  "database.table",
			minPos:   1760414678,
			maxPos:   1760414690,
			interval: 60,
			mockGaps: []GapInfo{
				// Existing interval: position=1760414666, interval=24, end=1760414690
				// This covers the scan range [1760414678, 1760414690]
				// Should NOT report a false positive gap
			},
			expectedGaps: 0,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				gaps: tt.mockGaps,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			gaps, err := svc.FindGaps(ctx, tt.modelID, tt.minPos, tt.maxPos, tt.interval)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, gaps, tt.expectedGaps)
				if tt.expectedGaps > 0 {
					// Verify first gap matches expected
					assert.Equal(t, tt.mockGaps[0].StartPos, gaps[0].StartPos)
					assert.Equal(t, tt.mockGaps[0].EndPos, gaps[0].EndPos)
				}
			}
		})
	}
}

// Test GetCoverage
func TestGetCoverage(t *testing.T) {
	tests := []struct {
		name        string
		modelID     string
		startPos    uint64
		endPos      uint64
		queryResult bool
		expectedCov bool
		wantErr     bool
	}{
		{
			name:        "fully covered range",
			modelID:     "database.table",
			startPos:    100,
			endPos:      200,
			queryResult: true,
			expectedCov: true,
			wantErr:     false,
		},
		{
			name:        "not covered range",
			modelID:     "database.table",
			startPos:    100,
			endPos:      200,
			queryResult: false,
			expectedCov: false,
			wantErr:     false,
		},
		{
			name:        "invalid model ID",
			modelID:     "invalid",
			startPos:    100,
			endPos:      200,
			expectedCov: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockClickhouseClient{
				coverageResult: tt.queryResult,
			}
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			config := TableConfig{
				IncrementalDatabase: "admin",
				IncrementalTable:    "tracking",
				ScheduledDatabase:   "admin",
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)

			ctx := context.Background()
			covered, err := svc.GetCoverage(ctx, tt.modelID, tt.startPos, tt.endPos)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCov, covered)
			}
		})
	}
}

// Mock ClickHouse client for testing
type mockClickhouseClient struct {
	queryResult    uint64
	queryError     error
	gaps           []GapInfo
	coverageResult bool
	executeError   error
}

func (m *mockClickhouseClient) Execute(_ context.Context, _ string) ([]byte, error) {
	if m.executeError != nil {
		return nil, m.executeError
	}
	// Return a mock response with 1 row written
	return []byte(`{"written_rows":"1"}`), nil
}

func (m *mockClickhouseClient) QueryOne(_ context.Context, _ string, result interface{}) error {
	if m.queryError != nil {
		return m.queryError
	}

	// Handle different result types based on what's being queried
	switch v := result.(type) {
	case *struct {
		LastPos uint64 `json:"last_pos,string"`
	}:
		v.LastPos = m.queryResult
	case *struct {
		LastEndPos uint64 `json:"last_end_pos,string"`
	}:
		v.LastEndPos = m.queryResult
	case *struct {
		FirstPos uint64 `json:"first_pos,string"`
	}:
		v.FirstPos = m.queryResult
	case *struct {
		FullyCovered int `json:"fully_covered"`
	}:
		if m.coverageResult {
			v.FullyCovered = 1
		} else {
			v.FullyCovered = 0
		}
	}
	return nil
}

func (m *mockClickhouseClient) QueryMany(_ context.Context, _ string, result interface{}) error {
	if m.queryError != nil {
		return m.queryError
	}

	// Handle gap results
	if gaps, ok := result.(*[]struct {
		GapStart uint64 `json:"gap_start,string"`
		GapEnd   uint64 `json:"gap_end,string"`
	}); ok {
		for _, gap := range m.gaps {
			*gaps = append(*gaps, struct {
				GapStart uint64 `json:"gap_start,string"`
				GapEnd   uint64 `json:"gap_end,string"`
			}{
				GapStart: gap.StartPos,
				GapEnd:   gap.EndPos,
			})
		}
	}
	return nil
}

func (m *mockClickhouseClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *mockClickhouseClient) Start() error {
	return nil
}

func (m *mockClickhouseClient) Stop() error {
	return nil
}

// Ensure mock implements the interface
var _ clickhouse.ClientInterface = (*mockClickhouseClient)(nil)

// TestHyphenatedDatabaseNamesInQueries tests that SQL queries properly escape hyphenated database names
func TestHyphenatedDatabaseNamesInQueries(t *testing.T) {
	tests := []struct {
		name          string
		adminDatabase string
		adminTable    string
		modelID       string
		position      uint64
		interval      uint64
		expectedInSQL []string // Strings that should appear in the SQL
	}{
		{
			name:          "simple hyphenated database",
			adminDatabase: "admin-db",
			adminTable:    "cbt",
			modelID:       "source-database.events",
			position:      1000,
			interval:      100,
			expectedInSQL: []string{
				"`admin-db`.`cbt`",
				"'source-database'",
				"'events'",
			},
		},
		{
			name:          "hyphenated admin and model databases",
			adminDatabase: "fusake-devnet-3",
			adminTable:    "admin_cbt",
			modelID:       "analytics-db.aggregates",
			position:      2000,
			interval:      200,
			expectedInSQL: []string{
				"`fusake-devnet-3`.`admin_cbt`",
				"'analytics-db'",
				"'aggregates'",
			},
		},
		{
			name:          "multiple hyphens in names",
			adminDatabase: "my-super-admin-db",
			adminTable:    "tracking-table",
			modelID:       "data-lake-2024.user-events",
			position:      3000,
			interval:      300,
			expectedInSQL: []string{
				"`my-super-admin-db`.`tracking-table`",
				"'data-lake-2024'",
				"'user-events'",
			},
		},
		{
			name:          "numeric with hyphens",
			adminDatabase: "admin-2024-01",
			adminTable:    "cbt-v2",
			modelID:       "db-2024-01.metrics",
			position:      4000,
			interval:      400,
			expectedInSQL: []string{
				"`admin-2024-01`.`cbt-v2`",
				"'db-2024-01'",
				"'metrics'",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock client that captures queries
			mockClient := &queryCapturingClient{
				queries: []string{},
			}

			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			// Create service with hyphenated admin database/table
			config := TableConfig{
				IncrementalDatabase: tt.adminDatabase,
				IncrementalTable:    tt.adminTable,
				ScheduledDatabase:   tt.adminDatabase,
				ScheduledTable:      "cbt_scheduled",
			}
			svc := NewService(log, mockClient, "", "", config, nil)
			ctx := context.Background()

			// Test RecordCompletion (INSERT query)
			err := svc.RecordCompletion(ctx, tt.modelID, tt.position, tt.interval)
			require.NoError(t, err)

			// Check that the INSERT query has proper escaping
			require.Len(t, mockClient.queries, 1, "Expected one INSERT query")
			insertQuery := mockClient.queries[0]

			for _, expected := range tt.expectedInSQL {
				assert.Contains(t, insertQuery, expected,
					"INSERT query should contain %s", expected)
			}

			// Reset queries
			mockClient.queries = []string{}

			// Test GetFirstPosition (SELECT query)
			_, err = svc.GetFirstPosition(ctx, tt.modelID)
			require.NoError(t, err)

			require.Len(t, mockClient.queries, 1, "Expected one SELECT query")
			selectQuery := mockClient.queries[0]

			// Check for proper escaping in SELECT
			assert.Contains(t, selectQuery, fmt.Sprintf("`%s`.`%s`", tt.adminDatabase, tt.adminTable),
				"SELECT query should have escaped admin table reference")

			// Check SELECT query has proper WHERE clause
			parts := strings.Split(tt.modelID, ".")
			assert.Contains(t, selectQuery, fmt.Sprintf("database = '%s'", parts[0]),
				"SELECT query should have database in WHERE clause")
			assert.Contains(t, selectQuery, fmt.Sprintf("table = '%s'", parts[1]),
				"SELECT query should have table in WHERE clause")

			// Reset queries
			mockClient.queries = []string{}

			// Test GetLastProcessedEndPosition
			_, err = svc.GetLastProcessedEndPosition(ctx, tt.modelID)
			require.NoError(t, err)

			require.Len(t, mockClient.queries, 1, "Expected one SELECT query")
			selectQuery2 := mockClient.queries[0]

			assert.Contains(t, selectQuery2, fmt.Sprintf("`%s`.`%s`", tt.adminDatabase, tt.adminTable),
				"SELECT query should have escaped admin table reference")
			assert.Contains(t, selectQuery2, fmt.Sprintf("database = '%s'", parts[0]),
				"SELECT query should have database in WHERE clause")
			assert.Contains(t, selectQuery2, fmt.Sprintf("table = '%s'", parts[1]),
				"SELECT query should have table in WHERE clause")
		})
	}
}

// queryCapturingClient is a mock ClickHouse client that captures queries for inspection
type queryCapturingClient struct {
	queries     []string
	mockResults []interface{}
	resultIndex int
}

func (m *queryCapturingClient) Execute(_ context.Context, query string) ([]byte, error) {
	m.queries = append(m.queries, query)
	return []byte(`{"written_rows":"1"}`), nil
}

func (m *queryCapturingClient) QueryOne(_ context.Context, query string, result interface{}) error {
	m.queries = append(m.queries, query)

	// Return mock results if available
	if m.resultIndex < len(m.mockResults) {
		// Simple type assertion - in real tests you'd handle this better
		switch v := result.(type) {
		case *struct {
			FirstPos uint64 `json:"first_pos,string"`
		}:
			v.FirstPos = 1000 // Default value
		case *struct {
			LastEndPos uint64 `json:"last_end_pos,string"`
		}:
			v.LastEndPos = 2000 // Default value
		case *struct {
			LastPos uint64 `json:"last_pos,string"`
		}:
			v.LastPos = 1500 // Default value
		case *struct {
			StartPos uint64 `json:"start_pos,string"`
			EndPos   uint64 `json:"end_pos,string"`
			RowCount int    `json:"row_count,string"`
		}:
			// For consolidation query
			v.StartPos = 1000
			v.EndPos = 2000
			v.RowCount = 5
		}
		m.resultIndex++
	}

	return nil
}

func (m *queryCapturingClient) QueryMany(_ context.Context, query string, result interface{}) error {
	m.queries = append(m.queries, query)

	if m.resultIndex < len(m.mockResults) {
		// Return the mock result based on the actual type expected
		switch v := result.(type) {
		case *[]struct {
			GapStart uint64 `json:"gap_start,string"`
			GapEnd   uint64 `json:"gap_end,string"`
		}:
			// For FindGaps query
			*v = []struct {
				GapStart uint64 `json:"gap_start,string"`
				GapEnd   uint64 `json:"gap_end,string"`
			}{
				{GapStart: 1000, GapEnd: 1100},
				{GapStart: 2000, GapEnd: 2200},
			}
		case *[]GapInfo:
			// For other queries that might use GapInfo directly
			if mockGaps, ok := m.mockResults[m.resultIndex].([]GapInfo); ok {
				*v = mockGaps
			}
		}
		m.resultIndex++
	}

	return nil
}

func (m *queryCapturingClient) BulkInsert(_ context.Context, _ string, _ interface{}) error {
	return nil
}

func (m *queryCapturingClient) Start() error {
	return nil
}

func (m *queryCapturingClient) Stop() error {
	return nil
}
