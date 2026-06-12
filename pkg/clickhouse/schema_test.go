package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaFakeClient is a minimal ClientInterface fake for TableExists tests.
// It records the query passed to QueryOne and populates the Count field.
type schemaFakeClient struct {
	count   uint64
	err     error
	gotQry  string
	gotCall bool
}

func (s *schemaFakeClient) QueryOne(_ context.Context, query string, dest any) error {
	s.gotCall = true
	s.gotQry = query

	if s.err != nil {
		return s.err
	}

	if r, ok := dest.(*struct {
		Count uint64 `ch:"count"`
	}); ok {
		r.Count = s.count
	}

	return nil
}

func (s *schemaFakeClient) QueryMany(_ context.Context, _ string, _ any) error { return nil }
func (s *schemaFakeClient) Execute(_ context.Context, _ string) error          { return nil }
func (s *schemaFakeClient) BulkInsert(_ context.Context, _ string, _ any) error {
	return nil
}
func (s *schemaFakeClient) Start() error { return nil }
func (s *schemaFakeClient) Stop() error  { return nil }

var _ ClientInterface = (*schemaFakeClient)(nil)

// errQuery is a sentinel used to assert error propagation from QueryOne.
var errQuery = errors.New("query boom")

func TestTableExists(t *testing.T) {
	tests := []struct {
		name      string
		count     uint64
		queryErr  error
		wantExist bool
		wantErr   bool
	}{
		{name: "table exists", count: 1, wantExist: true},
		{name: "table does not exist", count: 0, wantExist: false},
		{name: "query error", queryErr: errQuery, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &schemaFakeClient{count: tt.count, err: tt.queryErr}
			exists, err := TableExists(context.Background(), fake, "mydb", "mytable")

			if tt.wantErr {
				require.ErrorIs(t, err, tt.queryErr)
				assert.False(t, exists)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantExist, exists)
			assert.True(t, fake.gotCall)
			assert.Contains(t, fake.gotQry, "database = 'mydb'")
			assert.Contains(t, fake.gotQry, "name = 'mytable'")
		})
	}
}
