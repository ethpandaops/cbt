package admin

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordScheduledCompletion covers the success, invalid-model, and Execute
// error branches.
func TestRecordScheduledCompletion(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("success", func(t *testing.T) {
		client := &scriptedClient{}
		svc := newScriptedService(client)
		require.NoError(t, svc.RecordScheduledCompletion(context.Background(), "db.table", start))
		require.Len(t, client.queries, 1)
		assert.Contains(t, client.queries[0], "INSERT INTO `admin`.`cbt_scheduled`")
		assert.Contains(t, client.queries[0], "'2024-01-02 03:04:05.000'")
	})

	t.Run("invalid model ID", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		err := svc.RecordScheduledCompletion(context.Background(), "invalid", start)
		require.Error(t, err)
		require.ErrorIs(t, err, modelid.ErrInvalid)
	})

	t.Run("execute error", func(t *testing.T) {
		client := &scriptedClient{executeErrs: []error{errInjected}}
		svc := newScriptedService(client)
		err := svc.RecordScheduledCompletion(context.Background(), "db.table", start)
		require.Error(t, err)
		require.ErrorIs(t, err, errInjected)
		assert.Contains(t, err.Error(), "failed to insert scheduled admin record")
	})
}

// TestGetLastScheduledExecution covers all branches: invalid model, query error,
// nil/zero results, and a valid timestamp returned in UTC.
func TestGetLastScheduledExecution(t *testing.T) {
	t.Run("invalid model ID", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		_, err := svc.GetLastScheduledExecution(context.Background(), "invalid")
		require.Error(t, err)
		require.ErrorIs(t, err, modelid.ErrInvalid)
	})

	t.Run("query error", func(t *testing.T) {
		client := &scriptedClient{
			queryOneFns: []func(dest any) error{
				func(_ any) error { return errInjected },
			},
		}
		svc := newScriptedService(client)
		_, err := svc.GetLastScheduledExecution(context.Background(), "db.table")
		require.Error(t, err)
		require.ErrorIs(t, err, errInjected)
		assert.Contains(t, err.Error(), "failed to query last scheduled execution")
	})

	t.Run("nil result", func(t *testing.T) {
		client := &scriptedClient{
			queryOneFns: []func(dest any) error{
				func(_ any) error { return nil }, // leaves LastExecution nil
			},
		}
		svc := newScriptedService(client)
		got, err := svc.GetLastScheduledExecution(context.Background(), "db.table")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("epoch result treated as nil", func(t *testing.T) {
		client := &scriptedClient{
			queryOneFns: []func(dest any) error{
				func(dest any) error {
					setScheduledTime(dest, time.Unix(0, 0).UTC())
					return nil
				},
			},
		}
		svc := newScriptedService(client)
		got, err := svc.GetLastScheduledExecution(context.Background(), "db.table")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("valid result in UTC", func(t *testing.T) {
		// Provide the time in a non-UTC location to confirm conversion to UTC.
		loc := time.FixedZone("test", 5*3600)
		want := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
		client := &scriptedClient{
			queryOneFns: []func(dest any) error{
				func(dest any) error {
					setScheduledTime(dest, want.In(loc))
					return nil
				},
			},
		}
		svc := newScriptedService(client)
		got, err := svc.GetLastScheduledExecution(context.Background(), "db.table")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.True(t, got.Equal(want))
		assert.Equal(t, time.UTC, got.Location())
	})
}

// TestGetAllLastScheduledExecutions covers empty input, invalid model, query error,
// and the grouping path including the epoch-skip branch.
func TestGetAllLastScheduledExecutions(t *testing.T) {
	t.Run("empty model IDs", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		got, err := svc.GetAllLastScheduledExecutions(context.Background(), nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("invalid model ID", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		_, err := svc.GetAllLastScheduledExecutions(context.Background(), []string{"invalid"})
		require.Error(t, err)
		require.ErrorIs(t, err, modelid.ErrInvalid)
	})

	t.Run("query error", func(t *testing.T) {
		client := &scriptedClient{
			queryManyFns: []func(dest any) error{
				func(_ any) error { return errInjected },
			},
		}
		svc := newScriptedService(client)
		_, err := svc.GetAllLastScheduledExecutions(context.Background(), []string{"db.table"})
		require.Error(t, err)
		require.ErrorIs(t, err, errInjected)
		assert.Contains(t, err.Error(), "failed to batch query scheduled executions")
	})

	t.Run("groups and skips epoch rows", func(t *testing.T) {
		valid := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
		client := &scriptedClient{
			queryManyFns: []func(dest any) error{
				func(dest any) error {
					rows, ok := dest.(*[]struct {
						Database      string    `ch:"database"`
						Table         string    `ch:"table"`
						LastExecution time.Time `ch:"last_execution"`
					})
					require.True(t, ok)
					*rows = append(*rows,
						struct {
							Database      string    `ch:"database"`
							Table         string    `ch:"table"`
							LastExecution time.Time `ch:"last_execution"`
						}{Database: "db", Table: "a", LastExecution: valid},
						// Epoch row must be skipped.
						struct {
							Database      string    `ch:"database"`
							Table         string    `ch:"table"`
							LastExecution time.Time `ch:"last_execution"`
						}{Database: "db", Table: "b", LastExecution: time.Unix(0, 0).UTC()},
					)
					return nil
				},
			},
		}
		svc := newScriptedService(client)
		got, err := svc.GetAllLastScheduledExecutions(context.Background(), []string{"db.a", "db.b"})
		require.NoError(t, err)

		require.Len(t, got, 1)
		require.Contains(t, got, "db.a")
		require.NotContains(t, got, "db.b")
		assert.True(t, got["db.a"].Equal(valid))
	})
}

// TestScheduledAdminGetters covers the scheduled admin database/table accessors.
func TestScheduledAdminGetters(t *testing.T) {
	svc := newScriptedService(&scriptedClient{})

	assert.Equal(t, "admin", svc.GetScheduledAdminDatabase())
	assert.Equal(t, "cbt_scheduled", svc.GetScheduledAdminTable())
}

// setScheduledTime sets the *time.Time LastExecution field on the result struct.
func setScheduledTime(dest any, ts time.Time) {
	v := reflect.ValueOf(dest).Elem()
	f := v.FieldByName("LastExecution")
	if f.IsValid() && f.CanSet() {
		f.Set(reflect.ValueOf(&ts))
	}
}
