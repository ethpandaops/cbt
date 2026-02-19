package management

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const sessionKeyPrefix = "cbt:session:"

// SessionData holds the data associated with an authenticated session.
type SessionData struct {
	Username  string `json:"username"`
	AuthMode  string `json:"auth_mode"`
	CreatedAt int64  `json:"created_at"`
}

// SessionStore manages sessions backed by Redis.
type SessionStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewSessionStore creates a new Redis-backed session store.
func NewSessionStore(client *redis.Client, ttl time.Duration) *SessionStore {
	return &SessionStore{
		client: client,
		ttl:    ttl,
	}
}

// Create generates a new session ID, stores the data in Redis, and returns
// the session ID.
func (s *SessionStore) Create(
	ctx context.Context,
	data *SessionData,
) (string, error) {
	id, err := generateSessionID()
	if err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}

	data.CreatedAt = time.Now().Unix()

	raw, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("marshal session data: %w", err)
	}

	key := sessionKeyPrefix + id
	if err := s.client.Set(ctx, key, raw, s.ttl).Err(); err != nil {
		return "", fmt.Errorf("store session: %w", err)
	}

	return id, nil
}

// Get retrieves session data for the given session ID. Returns nil if the
// session does not exist or has expired.
func (s *SessionStore) Get(
	ctx context.Context,
	id string,
) (*SessionData, error) {
	key := sessionKeyPrefix + id

	raw, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}

		return nil, fmt.Errorf("get session: %w", err)
	}

	var data SessionData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal session data: %w", err)
	}

	return &data, nil
}

// Delete removes a session from Redis.
func (s *SessionStore) Delete(ctx context.Context, id string) error {
	key := sessionKeyPrefix + id

	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("delete session: %w", err)
	}

	return nil
}

func generateSessionID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}

	return hex.EncodeToString(b), nil
}
