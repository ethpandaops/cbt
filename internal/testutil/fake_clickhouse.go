package testutil

import (
	"context"
	"reflect"
	"strings"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
)

// FakeClickHouseClient is a configurable fake implementing clickhouse.ClientInterface.
//
// The zero value returns no error from every method and writes nothing into query
// destinations. QueryOne uses reflection to populate result structs so callers do not
// depend on ch struct tags:
//   - For queries that mention "system.tables" it sets the "Count" field to 1 when
//     TableExists is true, otherwise 0.
//   - For all other queries it sets the "Min" and "Max" fields from BoundsMin / BoundsMax.
//
// Inject failures via the *Err fields.
type FakeClickHouseClient struct {
	TableExists bool
	BoundsMin   uint64
	BoundsMax   uint64

	QueryOneErr   error
	QueryManyErr  error
	ExecuteErr    error
	BulkInsertErr error

	// QueryOneFn fully overrides QueryOne when set.
	QueryOneFn func(ctx context.Context, query string, dest any) error
}

// QueryOne populates dest via reflection based on the query shape (see type docs).
func (f *FakeClickHouseClient) QueryOne(ctx context.Context, query string, dest any) error {
	if f.QueryOneFn != nil {
		return f.QueryOneFn(ctx, query, dest)
	}

	if f.QueryOneErr != nil {
		return f.QueryOneErr
	}

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return nil
	}

	elem := v.Elem()

	if strings.Contains(query, "system.tables") {
		var count uint64
		if f.TableExists {
			count = 1
		}

		setUintField(elem, "Count", count)

		return nil
	}

	setUintField(elem, "Min", f.BoundsMin)
	setUintField(elem, "Max", f.BoundsMax)

	return nil
}

// QueryMany returns QueryManyErr and writes nothing.
func (f *FakeClickHouseClient) QueryMany(_ context.Context, _ string, _ any) error {
	return f.QueryManyErr
}

// Execute returns ExecuteErr.
func (f *FakeClickHouseClient) Execute(_ context.Context, _ string) error {
	return f.ExecuteErr
}

// BulkInsert returns BulkInsertErr.
func (f *FakeClickHouseClient) BulkInsert(_ context.Context, _ string, _ any) error {
	return f.BulkInsertErr
}

// Start is a no-op.
func (f *FakeClickHouseClient) Start() error { return nil }

// Stop is a no-op.
func (f *FakeClickHouseClient) Stop() error { return nil }

// setUintField sets a uint struct field by name if it exists and is settable.
func setUintField(v reflect.Value, fieldName string, value uint64) {
	if v.Kind() != reflect.Struct {
		return
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() || !field.CanSet() {
		return
	}

	field.SetUint(value)
}

var _ clickhouse.ClientInterface = (*FakeClickHouseClient)(nil)
