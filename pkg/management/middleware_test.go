package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newBrokenRedisClient returns a redis client that fails fast on every command,
// without retry storms, so error branches that depend on a Redis failure are
// deterministic and quick under -race.
func newBrokenRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		// Port 1 is reserved and refuses connections immediately.
		Addr:        "127.0.0.1:1",
		MaxRetries:  -1,
		DialTimeout: 50 * time.Millisecond,
	})
}

// newSessionCookieFor creates a session in the store and returns its ID.
func newSessionCookieFor(t *testing.T, store *SessionStore, username string) string {
	t.Helper()

	id, err := store.Create(context.Background(), &SessionData{
		Username: username,
		AuthMode: "github",
	})
	require.NoError(t, err)

	return id
}

func TestPasswordAuthMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{name: "missing header", authHeader: "", wantStatus: fiber.StatusUnauthorized},
		{name: "wrong scheme", authHeader: "Basic abc", wantStatus: fiber.StatusUnauthorized},
		{name: "wrong token", authHeader: "Bearer nope", wantStatus: fiber.StatusUnauthorized},
		{name: "valid token", authHeader: "Bearer secret", wantStatus: fiber.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app := fiber.New()
			app.Use(PasswordAuthMiddleware("secret"))
			app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			require.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

func TestSessionAuthMiddlewareMissingCookie(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	app := fiber.New()
	app.Use(SessionAuthMiddleware(store))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

func TestSessionAuthMiddlewareValidSession(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)
	id := newSessionCookieFor(t, store, "octocat")

	app := fiber.New()
	app.Use(SessionAuthMiddleware(store))
	app.Get("/", func(c fiber.Ctx) error {
		data, ok := c.Locals("session").(*SessionData)
		require.True(t, ok)
		require.Equal(t, "octocat", data.Username)

		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: id})

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestSessionAuthMiddlewareExpiredSession(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	app := fiber.New()
	app.Use(SessionAuthMiddleware(store))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "missing-id"})

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}

func TestSessionAuthMiddlewareLookupError(t *testing.T) {
	t.Parallel()

	store := NewSessionStore(newBrokenRedisClient(), time.Hour)

	app := fiber.New()
	app.Use(SessionAuthMiddleware(store))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "any"})

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestCombinedAuthMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authHeader string
		withCookie bool
		cookieVal  string
		wantStatus int
	}{
		{
			name:       "valid bearer",
			authHeader: "Bearer secret",
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "bearer wrong scheme falls through to no cookie",
			authHeader: "Basic secret",
			wantStatus: fiber.StatusUnauthorized,
		},
		{
			name:       "wrong bearer then valid cookie",
			authHeader: "Bearer wrong",
			withCookie: true,
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "valid cookie only",
			withCookie: true,
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "invalid cookie only",
			withCookie: true,
			cookieVal:  "bad-id",
			wantStatus: fiber.StatusUnauthorized,
		},
		{
			name:       "nothing provided",
			wantStatus: fiber.StatusUnauthorized,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := testutil.NewMiniredisClient(t)
			store := NewSessionStore(client, time.Hour)
			validID := newSessionCookieFor(t, store, "octocat")

			app := fiber.New()
			app.Use(CombinedAuthMiddleware("secret", store))
			app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			if tc.withCookie {
				val := tc.cookieVal
				if val == "" {
					val = validID
				}

				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: val})
			}

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			require.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

func TestCombinedAuthMiddlewareStoreError(t *testing.T) {
	t.Parallel()

	store := NewSessionStore(newBrokenRedisClient(), time.Hour)

	app := fiber.New()
	app.Use(CombinedAuthMiddleware("secret", store))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "any"})

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// store.Get errors -> neither path authenticates -> 401.
	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)
}
