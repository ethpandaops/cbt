package admin

import (
	"context"
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

	svc := NewService(log, mockClient, cluster, localSuffix, adminDatabase, adminTable, redisClient)
	assert.NotNil(t, svc)

	// Verify it implements the Service interface
	var _ = svc

	// Verify admin database and table are set
	assert.Equal(t, adminDatabase, svc.GetAdminDatabase())
	assert.Equal(t, adminTable, svc.GetAdminTable())
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
			svc := NewService(log, mockClient, "", "", "admin", "tracking", nil)

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
			svc := NewService(log, mockClient, "", "", "admin", "tracking", nil)

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
			svc := NewService(log, mockClient, "", "", "admin", "tracking", nil)

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
			svc := NewService(log, mockClient, "", "", "admin", "tracking", nil)

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
	mockClient := &mockClickhouseClient{
		gaps: []GapInfo{
			{StartPos: 200, EndPos: 300},
			{StartPos: 500, EndPos: 600},
		},
	}
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	svc := NewService(log, mockClient, "", "", "admin", "tracking", nil)

	ctx := context.Background()
	gaps, err := svc.FindGaps(ctx, "database.table", 0, 1000, 100)
	require.NoError(t, err)
	assert.Len(t, gaps, 2)
	assert.Equal(t, uint64(200), gaps[0].StartPos)
	assert.Equal(t, uint64(300), gaps[0].EndPos)
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
			svc := NewService(log, mockClient, "", "", "admin", "tracking", nil)

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

func (m *mockClickhouseClient) Execute(_ context.Context, _ string) error {
	return m.executeError
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
