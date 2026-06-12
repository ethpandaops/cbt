package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errFake is a sentinel error used to assert error propagation in the fake conn.
var errFake = errors.New("fake conn failure")

// fakeConn is a configurable fake implementing driver.Conn for unit testing the
// real *client methods without a network connection.
type fakeConn struct {
	selectErr  error
	execErr    error
	pingErr    error
	closeErr   error
	prepareErr error

	// selectFn, when set, fully handles Select so tests can populate dest.
	selectFn func(ctx context.Context, dest any, query string, args ...any) error

	// batch is returned by PrepareBatch when prepareErr is nil.
	batch *fakeBatch

	// recorded inputs for assertions.
	execQuery    string
	selectQuery  string
	prepareQuery string
	pingCalled   bool
	closeCalled  bool
}

func (f *fakeConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	f.selectQuery = query

	if f.selectFn != nil {
		return f.selectFn(ctx, dest, query, args...)
	}

	return f.selectErr
}

func (f *fakeConn) Exec(_ context.Context, query string, _ ...any) error {
	f.execQuery = query

	return f.execErr
}

func (f *fakeConn) PrepareBatch(
	_ context.Context, query string, _ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	f.prepareQuery = query

	if f.prepareErr != nil {
		return nil, f.prepareErr
	}

	if f.batch == nil {
		f.batch = &fakeBatch{}
	}

	return f.batch, nil
}

func (f *fakeConn) Ping(_ context.Context) error {
	f.pingCalled = true

	return f.pingErr
}

func (f *fakeConn) Close() error {
	f.closeCalled = true

	return f.closeErr
}

// Unused interface methods.
func (f *fakeConn) Contributors() []string { return nil }
func (f *fakeConn) ServerVersion() (*driver.ServerVersion, error) {
	return nil, nil //nolint:nilnil // unused in tests
}

func (f *fakeConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return nil, nil //nolint:nilnil // unused in tests
}
func (f *fakeConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row { return nil }
func (f *fakeConn) AsyncInsert(_ context.Context, _ string, _ bool, _ ...any) error {
	return nil
}
func (f *fakeConn) Stats() driver.Stats { return driver.Stats{} }

var _ driver.Conn = (*fakeConn)(nil)

// fakeBatch is a configurable fake implementing driver.Batch.
type fakeBatch struct {
	appendErr error
	sendErr   error

	appendCount int
	sendCalled  bool
}

func (b *fakeBatch) AppendStruct(_ any) error {
	b.appendCount++

	return b.appendErr
}

func (b *fakeBatch) Send() error {
	b.sendCalled = true

	return b.sendErr
}

// Unused interface methods.
func (b *fakeBatch) Abort() error                    { return nil }
func (b *fakeBatch) Append(_ ...any) error           { return nil }
func (b *fakeBatch) Column(_ int) driver.BatchColumn { return nil }
func (b *fakeBatch) Flush() error                    { return nil }
func (b *fakeBatch) IsSent() bool                    { return b.sendCalled }
func (b *fakeBatch) Rows() int                       { return b.appendCount }
func (b *fakeBatch) Columns() []column.Interface     { return nil }
func (b *fakeBatch) Close() error                    { return nil }

var _ driver.Batch = (*fakeBatch)(nil)

// newTestClient builds a *client wired to the supplied fake conn.
func newTestClient(conn driver.Conn, debug bool) *client {
	return &client{
		log:           logrus.New().WithField("component", "clickhouse-test"),
		conn:          conn,
		debug:         debug,
		queryTimeout:  time.Second,
		insertTimeout: time.Second,
	}
}

func TestNewClient(t *testing.T) {
	t.Run("invalid config", func(t *testing.T) {
		_, err := NewClient(logrus.New(), &Config{})
		require.ErrorIs(t, err, ErrURLRequired)
	})

	t.Run("invalid URL", func(t *testing.T) {
		_, err := NewClient(logrus.New(), &Config{URL: "://invalid"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create options")
	})

	t.Run("open connection error", func(t *testing.T) {
		original := openConn
		t.Cleanup(func() { openConn = original })

		openConn = func(*clickhouse.Options) (driver.Conn, error) {
			return nil, errFake
		}

		_, err := NewClient(logrus.New(), &Config{URL: "clickhouse://localhost:9000"})
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "failed to create connection")
	})

	t.Run("success", func(t *testing.T) {
		c, err := NewClient(logrus.New(), &Config{URL: "clickhouse://localhost:9000", Debug: true})
		require.NoError(t, err)
		require.NotNil(t, c)
		// Defaults applied through NewClient.
		impl, ok := c.(*client)
		require.True(t, ok)
		assert.Equal(t, 30*time.Second, impl.queryTimeout)
		assert.True(t, impl.debug)
	})
}

func TestClient_Start(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		conn := &fakeConn{}
		c := newTestClient(conn, false)
		require.NoError(t, c.Start())
		assert.True(t, conn.pingCalled)
	})

	t.Run("ping error", func(t *testing.T) {
		conn := &fakeConn{pingErr: errFake}
		c := newTestClient(conn, false)
		err := c.Start()
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "failed to connect to ClickHouse")
	})
}

func TestClient_Stop(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		conn := &fakeConn{}
		c := newTestClient(conn, false)
		require.NoError(t, c.Stop())
		assert.True(t, conn.closeCalled)
	})

	t.Run("close error", func(t *testing.T) {
		conn := &fakeConn{closeErr: errFake}
		c := newTestClient(conn, false)
		err := c.Stop()
		require.ErrorIs(t, err, errFake)
	})

	t.Run("nil conn", func(t *testing.T) {
		c := newTestClient(nil, false)
		// conn is nil; Stop must not panic and returns nil.
		c.conn = nil
		require.NoError(t, c.Stop())
	})
}

func TestClient_QueryOne(t *testing.T) {
	type row struct {
		Count uint64 `ch:"count"`
	}

	t.Run("not a pointer", func(t *testing.T) {
		c := newTestClient(&fakeConn{}, false)
		err := c.QueryOne(context.Background(), "SELECT 1", row{})
		require.ErrorIs(t, err, ErrDestMustBePointer)
	})

	t.Run("select error", func(t *testing.T) {
		c := newTestClient(&fakeConn{selectErr: errFake}, true)
		var dest row
		err := c.QueryOne(context.Background(), "SELECT 1", &dest)
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "query failed")
	})

	t.Run("empty result leaves zero value", func(t *testing.T) {
		// selectErr nil and no population => result slice empty.
		c := newTestClient(&fakeConn{}, false)
		dest := row{Count: 99}
		err := c.QueryOne(context.Background(), "SELECT 1", &dest)
		require.NoError(t, err)
		// dest unchanged because result slice was empty.
		assert.Equal(t, uint64(99), dest.Count)
	})

	t.Run("populates first result", func(t *testing.T) {
		conn := &fakeConn{
			selectFn: func(_ context.Context, dest any, _ string, _ ...any) error {
				// dest is *[]row; append one element.
				slicePtr := reflect.ValueOf(dest)
				slice := slicePtr.Elem()
				elem := reflect.ValueOf(row{Count: 7})
				slice.Set(reflect.Append(slice, elem))

				return nil
			},
		}
		c := newTestClient(conn, false)
		var dest row
		err := c.QueryOne(context.Background(), "SELECT count() as count", &dest)
		require.NoError(t, err)
		assert.Equal(t, uint64(7), dest.Count)
	})
}

func TestClient_QueryMany(t *testing.T) {
	type row struct {
		ID uint64 `ch:"id"`
	}

	t.Run("not a pointer to slice", func(t *testing.T) {
		c := newTestClient(&fakeConn{}, false)
		var notSlice row
		err := c.QueryMany(context.Background(), "SELECT 1", &notSlice)
		require.ErrorIs(t, err, ErrDestMustBePointerToSlice)
	})

	t.Run("select error", func(t *testing.T) {
		c := newTestClient(&fakeConn{selectErr: errFake}, true)
		var dest []row
		err := c.QueryMany(context.Background(), "SELECT 1", &dest)
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "query failed")
	})

	t.Run("success", func(t *testing.T) {
		conn := &fakeConn{
			selectFn: func(_ context.Context, dest any, _ string, _ ...any) error {
				slice := reflect.ValueOf(dest).Elem()
				slice.Set(reflect.Append(slice, reflect.ValueOf(row{ID: 1})))

				return nil
			},
		}
		c := newTestClient(conn, false)
		var dest []row
		err := c.QueryMany(context.Background(), "SELECT id FROM t", &dest)
		require.NoError(t, err)
		require.Len(t, dest, 1)
		assert.Equal(t, uint64(1), dest[0].ID)
		assert.Equal(t, "SELECT id FROM t", conn.selectQuery)
	})
}

func TestClient_Execute(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		conn := &fakeConn{}
		c := newTestClient(conn, true)
		require.NoError(t, c.Execute(context.Background(), "ALTER TABLE t"))
		assert.Equal(t, "ALTER TABLE t", conn.execQuery)
	})

	t.Run("exec error", func(t *testing.T) {
		c := newTestClient(&fakeConn{execErr: errFake}, false)
		err := c.Execute(context.Background(), "ALTER TABLE t")
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "execution failed")
	})
}

func TestClient_BulkInsert(t *testing.T) {
	type row struct {
		ID uint64 `ch:"id"`
	}

	t.Run("not a slice", func(t *testing.T) {
		c := newTestClient(&fakeConn{}, false)
		err := c.BulkInsert(context.Background(), "t", row{})
		require.ErrorIs(t, err, ErrDataMustBeSlice)
	})

	t.Run("empty slice returns nil", func(t *testing.T) {
		conn := &fakeConn{}
		c := newTestClient(conn, false)
		require.NoError(t, c.BulkInsert(context.Background(), "t", []row{}))
		assert.Empty(t, conn.prepareQuery)
	})

	t.Run("prepare batch error", func(t *testing.T) {
		c := newTestClient(&fakeConn{prepareErr: errFake}, true)
		err := c.BulkInsert(context.Background(), "t", []row{{ID: 1}})
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "failed to prepare batch")
	})

	t.Run("append error", func(t *testing.T) {
		conn := &fakeConn{batch: &fakeBatch{appendErr: errFake}}
		c := newTestClient(conn, false)
		err := c.BulkInsert(context.Background(), "t", []row{{ID: 1}})
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "failed to append row 0")
	})

	t.Run("send error", func(t *testing.T) {
		conn := &fakeConn{batch: &fakeBatch{sendErr: errFake}}
		c := newTestClient(conn, false)
		err := c.BulkInsert(context.Background(), "t", []row{{ID: 1}})
		require.ErrorIs(t, err, errFake)
		assert.Contains(t, err.Error(), "failed to send batch")
	})

	t.Run("success", func(t *testing.T) {
		conn := &fakeConn{batch: &fakeBatch{}}
		c := newTestClient(conn, true)
		err := c.BulkInsert(context.Background(), "mydb.t", []row{{ID: 1}, {ID: 2}})
		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO mydb.t", conn.prepareQuery)
		assert.Equal(t, 2, conn.batch.appendCount)
		assert.True(t, conn.batch.sendCalled)
	})
}
