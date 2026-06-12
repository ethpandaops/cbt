package management

import (
	"context"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNewSessionStore(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)

	store := NewSessionStore(client, time.Hour)
	require.NotNil(t, store)
	require.Equal(t, time.Hour, store.ttl)
	require.Same(t, client, store.client)
}

func TestSessionStoreCreateAndGet(t *testing.T) {
	t.Parallel()

	mr, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	id, err := store.Create(context.Background(), &SessionData{
		Username: "octocat",
		AuthMode: "github",
	})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// The stored key carries the session prefix.
	require.True(t, mr.Exists(sessionKeyPrefix+id))

	data, err := store.Get(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, "octocat", data.Username)
	require.Equal(t, "github", data.AuthMode)
	require.NotZero(t, data.CreatedAt)
}

func TestSessionStoreGetMissingReturnsNil(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	data, err := store.Get(context.Background(), "does-not-exist")
	require.NoError(t, err)
	require.Nil(t, data)
}

func TestSessionStoreGetUnmarshalError(t *testing.T) {
	t.Parallel()

	mr, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	// Store invalid JSON directly so Get hits the unmarshal error branch.
	require.NoError(t, mr.Set(sessionKeyPrefix+"badjson", "{not-json"))

	data, err := store.Get(context.Background(), "badjson")
	require.Error(t, err)
	require.Nil(t, data)
	require.Contains(t, err.Error(), "unmarshal session data")
}

func TestSessionStoreGetClientError(t *testing.T) {
	t.Parallel()

	mr, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	// Close the backing server to force a non-redis.Nil client error.
	mr.Close()

	data, err := store.Get(context.Background(), "anything")
	require.Error(t, err)
	require.Nil(t, data)
	require.NotErrorIs(t, err, redis.Nil)
	require.Contains(t, err.Error(), "get session")
}

func TestSessionStoreCreateClientError(t *testing.T) {
	t.Parallel()

	mr, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	mr.Close()

	id, err := store.Create(context.Background(), &SessionData{Username: "u"})
	require.Error(t, err)
	require.Empty(t, id)
	require.Contains(t, err.Error(), "store session")
}

func TestSessionStoreDelete(t *testing.T) {
	t.Parallel()

	mr, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	id, err := store.Create(context.Background(), &SessionData{Username: "octocat"})
	require.NoError(t, err)
	require.True(t, mr.Exists(sessionKeyPrefix+id))

	require.NoError(t, store.Delete(context.Background(), id))
	require.False(t, mr.Exists(sessionKeyPrefix+id))
}

func TestSessionStoreDeleteClientError(t *testing.T) {
	t.Parallel()

	mr, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	mr.Close()

	err := store.Delete(context.Background(), "anything")
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete session")
}

func TestSessionStoreCreateSessionIDError(t *testing.T) {
	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	withRandRead(t, failingReader)

	id, err := store.Create(context.Background(), &SessionData{Username: "u"})
	require.Error(t, err)
	require.Empty(t, id)
	require.Contains(t, err.Error(), "generate session id")
}

func TestSessionStoreCreateMarshalError(t *testing.T) {
	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	withJSONMarshal(t, failingMarshal)

	id, err := store.Create(context.Background(), &SessionData{Username: "u"})
	require.Error(t, err)
	require.Empty(t, id)
	require.Contains(t, err.Error(), "marshal session data")
}

func TestGenerateSessionID(t *testing.T) {
	t.Parallel()

	id1, err := generateSessionID()
	require.NoError(t, err)
	require.Len(t, id1, 64) // 32 bytes hex-encoded.

	id2, err := generateSessionID()
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)
}
