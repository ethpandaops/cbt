package admin

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.New("injected failure")

// scriptedClient is a flexible ClickHouse fake that drives each query method via
// per-call callbacks. It lets a single test exercise the multi-step incremental
// flows (range scan, member fetch, insert, delete) and inject failures at any step.
type scriptedClient struct {
	queries []string

	// queryOneFns is consumed one entry per QueryOne call (in order). A nil entry
	// (or running past the end) is a no-op success.
	queryOneFns []func(dest any) error
	queryOneIdx int

	// queryManyFns is consumed one entry per QueryMany call (in order).
	queryManyFns []func(dest any) error
	queryManyIdx int

	// executeErrs is consumed one entry per Execute call (in order); nil means success.
	executeErrs []error
	executeIdx  int
}

func (m *scriptedClient) Execute(_ context.Context, query string) error {
	m.queries = append(m.queries, query)

	var err error
	if m.executeIdx < len(m.executeErrs) {
		err = m.executeErrs[m.executeIdx]
	}

	m.executeIdx++

	return err
}

func (m *scriptedClient) QueryOne(_ context.Context, query string, dest any) error {
	m.queries = append(m.queries, query)

	defer func() { m.queryOneIdx++ }()

	if m.queryOneIdx < len(m.queryOneFns) {
		if fn := m.queryOneFns[m.queryOneIdx]; fn != nil {
			return fn(dest)
		}
	}

	return nil
}

func (m *scriptedClient) QueryMany(_ context.Context, query string, dest any) error {
	m.queries = append(m.queries, query)

	defer func() { m.queryManyIdx++ }()

	if m.queryManyIdx < len(m.queryManyFns) {
		if fn := m.queryManyFns[m.queryManyIdx]; fn != nil {
			return fn(dest)
		}
	}

	return nil
}

func (m *scriptedClient) BulkInsert(_ context.Context, _ string, _ any) error { return nil }
func (m *scriptedClient) Start() error                                        { return nil }
func (m *scriptedClient) Stop() error                                         { return nil }

var _ clickhouse.ClientInterface = (*scriptedClient)(nil)

// newScriptedService builds an admin service backed by the given scripted client.
func newScriptedService(client clickhouse.ClientInterface) Service {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	config := TableConfig{
		IncrementalDatabase: "admin",
		IncrementalTable:    "cbt_incremental",
		ScheduledDatabase:   "admin",
		ScheduledTable:      "cbt_scheduled",
	}

	return NewService(log, client, "", "", config, nil)
}

// setUint sets a uint64 field by name on a struct pointed to by dest.
func setUint(dest any, field string, value uint64) {
	v := reflect.ValueOf(dest).Elem()
	f := v.FieldByName(field)
	if f.IsValid() && f.CanSet() {
		f.SetUint(value)
	}
}

// TestRecordCompletion_ExecuteError covers the Execute failure branch.
func TestRecordCompletion_ExecuteError(t *testing.T) {
	client := &scriptedClient{executeErrs: []error{errInjected}}
	svc := newScriptedService(client)

	err := svc.RecordCompletion(context.Background(), "db.table", 100, 50)
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to insert admin record")
}

// TestPositionQueries_QueryError covers the QueryOne error branches in the
// single-position lookups.
func TestPositionQueries_QueryError(t *testing.T) {
	tests := []struct {
		name string
		call func(svc Service) error
	}{
		{
			name: "GetFirstPosition",
			call: func(svc Service) error {
				_, err := svc.GetFirstPosition(context.Background(), "db.table")
				return err
			},
		},
		{
			name: "GetNextUnprocessedPosition",
			call: func(svc Service) error {
				_, err := svc.GetNextUnprocessedPosition(context.Background(), "db.table")
				return err
			},
		},
		{
			name: "GetLastProcessedPosition",
			call: func(svc Service) error {
				_, err := svc.GetLastProcessedPosition(context.Background(), "db.table")
				return err
			},
		},
		{
			name: "GetCoverage",
			call: func(svc Service) error {
				_, err := svc.GetCoverage(context.Background(), "db.table", 0, 100)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &scriptedClient{
				queryOneFns: []func(dest any) error{
					func(_ any) error { return errInjected },
				},
			}
			svc := newScriptedService(client)

			err := tt.call(svc)
			require.Error(t, err)
			require.ErrorIs(t, err, errInjected)
		})
	}
}

// TestFindGaps_QueryError covers the QueryMany error branch in FindGaps.
func TestFindGaps_QueryError(t *testing.T) {
	client := &scriptedClient{
		queryManyFns: []func(dest any) error{
			func(_ any) error { return errInjected },
		},
	}
	svc := newScriptedService(client)

	_, err := svc.FindGaps(context.Background(), "db.table", 0, 1000, 100)
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to query gaps")
}

// TestFindGaps_FirstPositionQueryError covers the second QueryOne error branch in
// FindGaps (the begin-of-range gap check).
func TestFindGaps_FirstPositionQueryError(t *testing.T) {
	client := &scriptedClient{
		// First call: QueryMany for gaps (success, no gaps).
		queryManyFns: []func(dest any) error{nil},
		// Second call: QueryOne for first position (fails).
		queryOneFns: []func(dest any) error{
			func(_ any) error { return errInjected },
		},
	}
	svc := newScriptedService(client)

	_, err := svc.FindGaps(context.Background(), "db.table", 0, 1000, 100)
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to query first position for gaps")
}

// TestFindGaps_GapAtBeginning covers the branch that appends a synthetic gap when
// the first stored position is past minPos.
func TestFindGaps_GapAtBeginning(t *testing.T) {
	const firstPos uint64 = 500

	client := &scriptedClient{
		// QueryMany: window-function gaps query returns one explicit gap.
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]struct {
					GapStart uint64 `ch:"gap_start"`
					GapEnd   uint64 `ch:"gap_end"`
				})
				require.True(t, ok)
				*rows = append(*rows, struct {
					GapStart uint64 `ch:"gap_start"`
					GapEnd   uint64 `ch:"gap_end"`
				}{GapStart: 600, GapEnd: 700})
				return nil
			},
		},
		// QueryOne: first-position lookup returns a non-nil pointer > minPos.
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				v := reflect.ValueOf(dest).Elem()
				f := v.FieldByName("FirstPos")
				require.True(t, f.IsValid())
				fp := firstPos
				f.Set(reflect.ValueOf(&fp))
				return nil
			},
		},
	}
	svc := newScriptedService(client)

	gaps, err := svc.FindGaps(context.Background(), "db.table", 0, 1000, 100)
	require.NoError(t, err)

	// Expect the explicit gap plus the synthesized begin-of-range gap [0, 500).
	require.Len(t, gaps, 2)
	assert.Equal(t, GapInfo{StartPos: 600, EndPos: 700}, gaps[0])
	assert.Equal(t, GapInfo{StartPos: 0, EndPos: firstPos}, gaps[1])
}

// TestGetProcessedRanges covers the success and error paths.
func TestGetProcessedRanges(t *testing.T) {
	t.Run("invalid model ID", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		_, err := svc.GetProcessedRanges(context.Background(), "invalid")
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
		_, err := svc.GetProcessedRanges(context.Background(), "db.table")
		require.Error(t, err)
		require.ErrorIs(t, err, errInjected)
		assert.Contains(t, err.Error(), "failed to query processed ranges")
	})

	t.Run("success", func(t *testing.T) {
		want := []ProcessedRange{{Position: 200, Interval: 100}, {Position: 100, Interval: 100}}
		client := &scriptedClient{
			queryManyFns: []func(dest any) error{
				func(dest any) error {
					rows, ok := dest.(*[]ProcessedRange)
					require.True(t, ok)
					*rows = want
					return nil
				},
			},
		}
		svc := newScriptedService(client)
		got, err := svc.GetProcessedRanges(context.Background(), "db.table")
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

// TestGetAllProcessedRanges covers all branches: empty input, invalid model,
// query error, and the success grouping path.
func TestGetAllProcessedRanges(t *testing.T) {
	t.Run("empty model IDs", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		got, err := svc.GetAllProcessedRanges(context.Background(), nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("invalid model ID", func(t *testing.T) {
		svc := newScriptedService(&scriptedClient{})
		_, err := svc.GetAllProcessedRanges(context.Background(), []string{"invalid"})
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
		_, err := svc.GetAllProcessedRanges(context.Background(), []string{"db.table"})
		require.Error(t, err)
		require.ErrorIs(t, err, errInjected)
		assert.Contains(t, err.Error(), "failed to batch query processed ranges")
	})

	t.Run("success groups by model", func(t *testing.T) {
		client := &scriptedClient{
			queryManyFns: []func(dest any) error{
				func(dest any) error {
					rows, ok := dest.(*[]struct {
						Database string `ch:"database"`
						Table    string `ch:"table"`
						Position uint64 `ch:"position"`
						Interval uint64 `ch:"interval"`
					})
					require.True(t, ok)
					*rows = append(*rows,
						struct {
							Database string `ch:"database"`
							Table    string `ch:"table"`
							Position uint64 `ch:"position"`
							Interval uint64 `ch:"interval"`
						}{Database: "db", Table: "a", Position: 200, Interval: 100},
						struct {
							Database string `ch:"database"`
							Table    string `ch:"table"`
							Position uint64 `ch:"position"`
							Interval uint64 `ch:"interval"`
						}{Database: "db", Table: "a", Position: 100, Interval: 100},
						struct {
							Database string `ch:"database"`
							Table    string `ch:"table"`
							Position uint64 `ch:"position"`
							Interval uint64 `ch:"interval"`
						}{Database: "db", Table: "b", Position: 0, Interval: 50},
					)
					return nil
				},
			},
		}
		svc := newScriptedService(client)
		got, err := svc.GetAllProcessedRanges(context.Background(), []string{"db.a", "db.b"})
		require.NoError(t, err)

		require.Len(t, got, 2)
		assert.Equal(t, []ProcessedRange{
			{Position: 200, Interval: 100},
			{Position: 100, Interval: 100},
		}, got["db.a"])
		assert.Equal(t, []ProcessedRange{{Position: 0, Interval: 50}}, got["db.b"])
	})
}

// TestConsolidateHistoricalData_RangeQueryError covers the QueryOne error branch.
func TestConsolidateHistoricalData_RangeQueryError(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(_ any) error { return errInjected },
		},
	}
	svc := newScriptedService(client)

	_, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to find contiguous ranges")
}

// TestConsolidateHistoricalData_MemberFetchError covers the QueryMany error branch
// for fetching island members.
func TestConsolidateHistoricalData_MemberFetchError(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				setUint(dest, "StartPos", 0)
				setUint(dest, "EndPos", 400)
				setUint(dest, "RowCount", 4)
				return nil
			},
		},
		queryManyFns: []func(dest any) error{
			func(_ any) error { return errInjected },
		},
	}
	svc := newScriptedService(client)

	_, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to fetch island members")
}

// TestConsolidateHistoricalData_MembersShrunk covers the guard where the island
// shrinks below 2 rows between scans, returning (0, nil) without writing.
func TestConsolidateHistoricalData_MembersShrunk(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				setUint(dest, "StartPos", 0)
				setUint(dest, "EndPos", 400)
				setUint(dest, "RowCount", 4)
				return nil
			},
		},
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				*rows = []ProcessedRange{{Position: 0, Interval: 100}} // only 1 row
				return nil
			},
		},
	}
	svc := newScriptedService(client)

	deleted, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.NoError(t, err)
	assert.Zero(t, deleted)
}

// TestConsolidateHistoricalData_InsertError covers the writeConsolidatedRows insert
// failure branch.
func TestConsolidateHistoricalData_InsertError(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				setUint(dest, "StartPos", 0)
				setUint(dest, "EndPos", 400)
				setUint(dest, "RowCount", 2)
				return nil
			},
		},
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				*rows = []ProcessedRange{
					{Position: 0, Interval: 200},
					{Position: 200, Interval: 200},
				}
				return nil
			},
		},
		executeErrs: []error{errInjected}, // INSERT fails
	}
	svc := newScriptedService(client)

	_, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to insert consolidated row")
}

// TestConsolidateHistoricalData_DeleteError covers the deleteMemberBatches error
// branch: the consolidated INSERT succeeds but the keyed DELETE fails.
func TestConsolidateHistoricalData_DeleteError(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				setUint(dest, "StartPos", 0)
				setUint(dest, "EndPos", 400)
				setUint(dest, "RowCount", 2)
				return nil
			},
		},
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				*rows = []ProcessedRange{
					{Position: 0, Interval: 200},
					{Position: 200, Interval: 200},
				}
				return nil
			},
		},
		executeErrs: []error{nil, errInjected}, // INSERT ok, DELETE fails
	}
	svc := newScriptedService(client)

	// writeConsolidatedRows returns members count even on delete error.
	got, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to delete old rows")
	assert.Equal(t, uint64(2), got)
}

// TestConsolidateHistoricalData_AllMembersMatchConsolidatedKey covers the
// len(tuples)==0 short-circuit in writeConsolidatedRows: when every member already
// equals the consolidated key, there is nothing to delete.
func TestConsolidateHistoricalData_AllMembersMatchConsolidatedKey(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				setUint(dest, "StartPos", 0)
				setUint(dest, "EndPos", 400)
				setUint(dest, "RowCount", 2)
				return nil
			},
		},
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				// Both members share the consolidated key (start=0, span=400) so the
				// delete tuple list ends up empty.
				*rows = []ProcessedRange{
					{Position: 0, Interval: 400},
					{Position: 0, Interval: 400},
				}
				return nil
			},
		},
	}
	svc := newScriptedService(client)

	got, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), got)

	// Only the range scan, member fetch, and INSERT should have run; no DELETE.
	deletes := 0
	for _, q := range client.queries {
		if strings.Contains(q, "DELETE FROM") {
			deletes++
		}
	}
	assert.Zero(t, deletes, "no DELETE should be issued when tuple list is empty")
}

// TestConsolidateHistoricalData_MembersOutOfOrder covers the startPos-update branch
// in writeConsolidatedRows where a later member has a smaller position than the
// first member, so the consolidated span must extend leftward.
func TestConsolidateHistoricalData_MembersOutOfOrder(t *testing.T) {
	client := &scriptedClient{
		queryOneFns: []func(dest any) error{
			func(dest any) error {
				setUint(dest, "StartPos", 0)
				setUint(dest, "EndPos", 400)
				setUint(dest, "RowCount", 2)
				return nil
			},
		},
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				// First member has the larger position; the second is smaller, forcing
				// startPos to be lowered to 0 and the consolidated span to be [0, 400).
				*rows = []ProcessedRange{
					{Position: 200, Interval: 200}, // [200, 400)
					{Position: 0, Interval: 200},   // [0, 200)
				}
				return nil
			},
		},
	}
	svc := newScriptedService(client)

	got, err := svc.ConsolidateHistoricalData(context.Background(), "db.table")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), got)

	// The INSERT (3rd query) must carry the leftward-extended consolidated span.
	require.Len(t, client.queries, 4)
	insertQuery := client.queries[2]
	assert.Contains(t, insertQuery, "INSERT INTO")
	assert.Contains(t, insertQuery, "0, 400", "consolidated row spans [0, 400)")
}

// TestDeletePeriod_SelectError covers the QueryMany error branch.
func TestDeletePeriod_SelectError(t *testing.T) {
	client := &scriptedClient{
		queryManyFns: []func(dest any) error{
			func(_ any) error { return errInjected },
		},
	}
	svc := newScriptedService(client)

	_, err := svc.DeletePeriod(context.Background(), "db.table", 100, 200)
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to query overlapping rows")
}

// TestDeletePeriod_DeleteError covers the Execute error branch on the DELETE step.
func TestDeletePeriod_DeleteError(t *testing.T) {
	client := &scriptedClient{
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				*rows = []ProcessedRange{{Position: 100, Interval: 100}}
				return nil
			},
		},
		executeErrs: []error{errInjected}, // DELETE fails
	}
	svc := newScriptedService(client)

	_, err := svc.DeletePeriod(context.Background(), "db.table", 130, 170)
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to delete overlapping rows")
}

// TestDeletePeriod_InsertRemainderError covers the Execute error branch on a
// remainder INSERT, which returns the deleted count alongside the error.
func TestDeletePeriod_InsertRemainderError(t *testing.T) {
	client := &scriptedClient{
		queryManyFns: []func(dest any) error{
			func(dest any) error {
				rows, ok := dest.(*[]ProcessedRange)
				require.True(t, ok)
				*rows = []ProcessedRange{{Position: 100, Interval: 100}} // [100,200)
				return nil
			},
		},
		// DELETE ok, first remainder INSERT fails.
		executeErrs: []error{nil, errInjected},
	}
	svc := newScriptedService(client)

	got, err := svc.DeletePeriod(context.Background(), "db.table", 130, 170)
	require.Error(t, err)
	require.ErrorIs(t, err, errInjected)
	assert.Contains(t, err.Error(), "failed to insert remainder")
	assert.Equal(t, uint64(1), got)
}
