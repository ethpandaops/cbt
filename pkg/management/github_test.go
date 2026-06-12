package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	errRand    = errors.New("rand failure")
	errMarshal = errors.New("marshal failure")
)

// failingReader simulates a crypto/rand failure when wired into randRead.
func failingReader(_ []byte) (int, error) { return 0, errRand }

// failingMarshal simulates a json.Marshal failure when wired into jsonMarshal.
func failingMarshal(_ any) ([]byte, error) { return nil, errMarshal }

// withRandRead temporarily replaces randRead, restoring it on cleanup.
func withRandRead(t *testing.T, fn func([]byte) (int, error)) {
	t.Helper()

	orig := randRead
	randRead = fn

	t.Cleanup(func() { randRead = orig })
}

// withJSONMarshal temporarily replaces jsonMarshal, restoring it on cleanup.
func withJSONMarshal(t *testing.T, fn func(any) ([]byte, error)) {
	t.Helper()

	orig := jsonMarshal
	jsonMarshal = fn

	t.Cleanup(func() { jsonMarshal = orig })
}

// withGitHubEndpoints points the package-level GitHub endpoints at base for the
// duration of the test.
func withGitHubEndpoints(t *testing.T, base string) {
	t.Helper()

	origAPI, origExchange, origAuth := githubAPIBase, githubExchangeURL, githubAuthURL

	githubAPIBase = base
	githubExchangeURL = base + "/login/oauth/access_token"
	githubAuthURL = base + "/login/oauth/authorize"

	t.Cleanup(func() {
		githubAPIBase = origAPI
		githubExchangeURL = origExchange
		githubAuthURL = origAuth
	})
}

// githubMockConfig configures the behavior of the GitHub mock server.
type githubMockConfig struct {
	tokenStatus  int
	tokenBody    string
	userStatus   int
	userBody     string
	memberStatus int
}

// newGitHubMock starts an httptest server emulating the GitHub OAuth + API
// endpoints used by the handler.
func newGitHubMock(t *testing.T, cfg githubMockConfig) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/login/oauth/access_token", func(w http.ResponseWriter, _ *http.Request) {
		status := cfg.tokenStatus
		if status == 0 {
			status = http.StatusOK
		}

		body := cfg.tokenBody
		if body == "" {
			body = `{"access_token":"gho_token","token_type":"bearer"}`
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})

	mux.HandleFunc("/user", func(w http.ResponseWriter, _ *http.Request) {
		status := cfg.userStatus
		if status == 0 {
			status = http.StatusOK
		}

		body := cfg.userBody
		if body == "" {
			body = `{"login":"octocat"}`
		}

		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})

	mux.HandleFunc("/orgs/", func(w http.ResponseWriter, _ *http.Request) {
		status := cfg.memberStatus
		if status == 0 {
			status = http.StatusNoContent
		}

		w.WriteHeader(status)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

// newTestGitHubHandler builds a handler wired to the test server and redis client.
func newTestGitHubHandler(
	t *testing.T,
	cfg *GitHubConfig,
	client *redis.Client,
	httpClient *http.Client,
) *GitHubHandler {
	t.Helper()

	store := NewSessionStore(client, time.Hour)
	h := NewGitHubHandler(cfg, client, store, logrus.New())
	h.httpClient = httpClient

	return h
}

func defaultGitHubConfig() *GitHubConfig {
	return &GitHubConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		CallbackURL:  "http://localhost:8080/cb",
		Org:          "ethpandaops",
		SessionTTL:   time.Hour,
	}
}

func TestGeneratePKCEChallenge(t *testing.T) {
	t.Parallel()

	// Known vector: challenge is the base64url-encoded SHA-256 of the verifier.
	got := generatePKCEChallenge("abc")
	require.NotEmpty(t, got)
	require.NotContains(t, got, "=")
	// Deterministic for the same input.
	require.Equal(t, got, generatePKCEChallenge("abc"))
	require.NotEqual(t, got, generatePKCEChallenge("xyz"))
}

func TestGenerateRandomHexError(t *testing.T) {
	withRandRead(t, failingReader)

	_, err := generateRandomHex(16)
	require.ErrorIs(t, err, errRand)
}

func TestGeneratePKCEVerifierError(t *testing.T) {
	withRandRead(t, failingReader)

	_, err := generatePKCEVerifier()
	require.ErrorIs(t, err, errRand)
}

func TestGenerateSessionIDError(t *testing.T) {
	withRandRead(t, failingReader)

	_, err := generateSessionID()
	require.ErrorIs(t, err, errRand)
}

func TestNewGitHubHandlerScopes(t *testing.T) {
	_, client := testutil.NewMiniredisClient(t)
	store := NewSessionStore(client, time.Hour)

	withOrg := NewGitHubHandler(defaultGitHubConfig(), client, store, logrus.New())
	require.Contains(t, withOrg.oauth2Config.Scopes, "read:org")
	require.Same(t, http.DefaultClient, withOrg.httpClient)

	noOrg := &GitHubConfig{
		ClientID:     "id",
		ClientSecret: "secret",
		CallbackURL:  "http://localhost/cb",
		AllowedUsers: []string{"octocat"},
	}
	without := NewGitHubHandler(noOrg, client, store, logrus.New())
	require.NotContains(t, without.oauth2Config.Scopes, "read:org")
}

func TestHandleLogin(t *testing.T) {
	mr, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

	app := fiber.New()
	app.Get("/login", h.HandleLogin)

	req := httptest.NewRequest(http.MethodGet, "/login", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusSeeOther, resp.StatusCode)

	loc := resp.Header.Get("Location")
	require.Contains(t, loc, "code_challenge")
	require.Contains(t, loc, "code_challenge_method=S256")

	// State must have been persisted in redis.
	parsed, perr := url.Parse(loc)
	require.NoError(t, perr)
	state := parsed.Query().Get("state")
	require.NotEmpty(t, state)
	require.True(t, mr.Exists(oauthStatePrefix+state))
}

func TestHandleLoginStateGenError(t *testing.T) {
	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

	withRandRead(t, failingReader)

	app := fiber.New()
	app.Get("/login", h.HandleLogin)

	req := httptest.NewRequest(http.MethodGet, "/login", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestHandleLoginVerifierError(t *testing.T) {
	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

	// Fail only on the second randRead call (PKCE verifier), succeeding for state.
	var calls int
	withRandRead(t, func(b []byte) (int, error) {
		calls++
		if calls >= 2 {
			return 0, errRand
		}

		return len(b), nil
	})

	app := fiber.New()
	app.Get("/login", h.HandleLogin)

	req := httptest.NewRequest(http.MethodGet, "/login", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestHandleLoginRedisError(t *testing.T) {
	h := newTestGitHubHandler(t, defaultGitHubConfig(), newBrokenRedisClient(), http.DefaultClient)

	app := fiber.New()
	app.Get("/login", h.HandleLogin)

	req := httptest.NewRequest(http.MethodGet, "/login", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestHandleLoginMarshalError(t *testing.T) {
	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

	withJSONMarshal(t, failingMarshal)

	app := fiber.New()
	app.Get("/login", h.HandleLogin)

	req := httptest.NewRequest(http.MethodGet, "/login", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

func TestHandleLogout(t *testing.T) {
	t.Run("with valid cookie", func(t *testing.T) {
		mr, client := testutil.NewMiniredisClient(t)
		h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

		id, err := h.sessionStore.Create(context.Background(), &SessionData{Username: "octocat"})
		require.NoError(t, err)
		require.True(t, mr.Exists(sessionKeyPrefix+id))

		app := fiber.New()
		app.Post("/logout", h.HandleLogout)

		req := httptest.NewRequest(http.MethodPost, "/logout", http.NoBody)
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: id})

		resp, terr := app.Test(req)
		require.NoError(t, terr)
		defer func() { _ = resp.Body.Close() }()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
		require.False(t, mr.Exists(sessionKeyPrefix+id))
	})

	t.Run("without cookie", func(t *testing.T) {
		_, client := testutil.NewMiniredisClient(t)
		h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

		app := fiber.New()
		app.Post("/logout", h.HandleLogout)

		req := httptest.NewRequest(http.MethodPost, "/logout", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})

	t.Run("delete error logged", func(t *testing.T) {
		h := newTestGitHubHandler(t, defaultGitHubConfig(), newBrokenRedisClient(), http.DefaultClient)

		app := fiber.New()
		app.Post("/logout", h.HandleLogout)

		req := httptest.NewRequest(http.MethodPost, "/logout", http.NoBody)
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "some-id"})

		resp, err := app.Test(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		// Delete failure is logged but the response is still 200.
		require.Equal(t, fiber.StatusOK, resp.StatusCode)
	})
}

func TestHandleCallback(t *testing.T) {
	type seed struct {
		state    string
		rawValue string
	}

	tests := []struct {
		name          string
		query         string
		seed          *seed
		mock          githubMockConfig
		cfg           *GitHubConfig
		wantStatus    int
		wantLocation  string
		wantLocPrefix string
	}{
		{
			name:          "oauth error param",
			query:         "?error=access_denied&error_description=nope",
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:          "missing state and code",
			query:         "?state=&code=",
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:          "invalid state in redis",
			query:         "?state=missing&code=abc",
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:          "corrupt state json",
			query:         "?state=corrupt&code=abc",
			seed:          &seed{state: "corrupt", rawValue: "{not-json"},
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:          "token exchange failure",
			query:         "?state=good&code=abc",
			seed:          &seed{state: "good", rawValue: `{"verifier":"v"}`},
			mock:          githubMockConfig{tokenStatus: http.StatusUnauthorized, tokenBody: `{"error":"bad"}`},
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:          "get username failure",
			query:         "?state=good&code=abc",
			seed:          &seed{state: "good", rawValue: `{"verifier":"v"}`},
			mock:          githubMockConfig{userStatus: http.StatusInternalServerError},
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:          "user not authorized",
			query:         "?state=good&code=abc",
			seed:          &seed{state: "good", rawValue: `{"verifier":"v"}`},
			mock:          githubMockConfig{memberStatus: http.StatusNotFound},
			cfg:           defaultGitHubConfig(),
			wantStatus:    fiber.StatusSeeOther,
			wantLocPrefix: "/?",
		},
		{
			name:         "success http callback no secure cookie",
			query:        "?state=good&code=abc",
			seed:         &seed{state: "good", rawValue: `{"verifier":"v"}`},
			mock:         githubMockConfig{memberStatus: http.StatusNoContent},
			cfg:          defaultGitHubConfig(),
			wantStatus:   fiber.StatusSeeOther,
			wantLocation: "/",
		},
		{
			name:  "success https callback secure cookie",
			query: "?state=good&code=abc",
			seed:  &seed{state: "good", rawValue: `{"verifier":"v"}`},
			mock:  githubMockConfig{memberStatus: http.StatusNoContent},
			cfg: &GitHubConfig{
				ClientID:     "id",
				ClientSecret: "secret",
				CallbackURL:  "https://example.com/cb",
				AllowedUsers: []string{"octocat"},
				SessionTTL:   time.Hour,
			},
			wantStatus:   fiber.StatusSeeOther,
			wantLocation: "/",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newGitHubMock(t, tc.mock)
			withGitHubEndpoints(t, srv.URL)

			mr, client := testutil.NewMiniredisClient(t)
			h := newTestGitHubHandler(t, tc.cfg, client, srv.Client())

			if tc.seed != nil {
				require.NoError(t, mr.Set(oauthStatePrefix+tc.seed.state, tc.seed.rawValue))
			}

			app := fiber.New()
			app.Get("/cb", h.HandleCallback)

			req := httptest.NewRequest(http.MethodGet, "/cb"+tc.query, http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			require.Equal(t, tc.wantStatus, resp.StatusCode)

			loc := resp.Header.Get("Location")
			if tc.wantLocation != "" {
				require.Equal(t, tc.wantLocation, loc)
			}

			if tc.wantLocPrefix != "" {
				require.True(t, strings.HasPrefix(loc, tc.wantLocPrefix), "location=%s", loc)
			}
		})
	}
}

func TestHandleCallbackSessionCreateError(t *testing.T) {
	srv := newGitHubMock(t, githubMockConfig{memberStatus: http.StatusNoContent})
	withGitHubEndpoints(t, srv.URL)

	// Seed state into a working redis, but back the session store with a broken
	// client so the final session Create fails.
	mr, client := testutil.NewMiniredisClient(t)
	require.NoError(t, mr.Set(oauthStatePrefix+"good", `{"verifier":"v"}`))

	store := NewSessionStore(newBrokenRedisClient(), time.Hour)
	h := NewGitHubHandler(defaultGitHubConfig(), client, store, logrus.New())
	h.httpClient = srv.Client()

	app := fiber.New()
	app.Get("/cb", h.HandleCallback)

	req := httptest.NewRequest(http.MethodGet, "/cb?state=good&code=abc", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, fiber.StatusSeeOther, resp.StatusCode)
	require.True(t, strings.HasPrefix(resp.Header.Get("Location"), "/?"))
}

func TestRedirectToFrontendAuthError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/with-desc", func(c fiber.Ctx) error {
		return redirectToFrontendAuthError(c, "access_denied", "denied")
	})
	app.Get("/no-desc", func(c fiber.Ctx) error {
		return redirectToFrontendAuthError(c, "server_error", "")
	})

	req := httptest.NewRequest(http.MethodGet, "/with-desc", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	loc := resp.Header.Get("Location")
	require.Contains(t, loc, "error=access_denied")
	require.Contains(t, loc, "error_description=denied")

	req2 := httptest.NewRequest(http.MethodGet, "/no-desc", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	loc2 := resp2.Header.Get("Location")
	require.Contains(t, loc2, "error=server_error")
	require.NotContains(t, loc2, "error_description")
}

func TestGetGitHubUsername(t *testing.T) {
	tests := []struct {
		name      string
		mock      githubMockConfig
		wantUser  string
		wantErrIs error
		wantErr   bool
	}{
		{
			name:     "success",
			mock:     githubMockConfig{userBody: `{"login":"octocat"}`},
			wantUser: "octocat",
		},
		{
			name:      "non-200 status",
			mock:      githubMockConfig{userStatus: http.StatusForbidden},
			wantErrIs: ErrUnexpectedGitHubStatus,
		},
		{
			name:      "empty login",
			mock:      githubMockConfig{userBody: `{"login":""}`},
			wantErrIs: ErrEmptyGitHubLogin,
		},
		{
			name:    "invalid json",
			mock:    githubMockConfig{userBody: `{not-json`},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newGitHubMock(t, tc.mock)
			withGitHubEndpoints(t, srv.URL)

			_, client := testutil.NewMiniredisClient(t)
			h := newTestGitHubHandler(t, defaultGitHubConfig(), client, srv.Client())

			user, err := h.getGitHubUsername(context.Background(), "token")
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)

				return
			}

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantUser, user)
		})
	}
}

func TestGithubAPIGetRequestError(t *testing.T) {
	// Point the API base at an unreachable host so http.Do fails.
	origAPI := githubAPIBase
	githubAPIBase = "http://127.0.0.1:1"

	t.Cleanup(func() { githubAPIBase = origAPI })

	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, &http.Client{
		Timeout: 100 * time.Millisecond,
	})

	_, err := h.fetchGitHubUser(context.Background(), "token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "do request")
}

func TestValidateUser(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *GitHubConfig
		username  string
		mock      githubMockConfig
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "allowlist match case insensitive",
			cfg: &GitHubConfig{
				AllowedUsers: []string{"OctoCat"},
			},
			username: "octocat",
		},
		{
			name: "org member",
			cfg: &GitHubConfig{
				Org: "ethpandaops",
			},
			username: "octocat",
			mock:     githubMockConfig{memberStatus: http.StatusNoContent},
		},
		{
			name: "not authorized",
			cfg: &GitHubConfig{
				Org: "ethpandaops",
			},
			username:  "octocat",
			mock:      githubMockConfig{memberStatus: http.StatusNotFound},
			wantErrIs: ErrUserNotAuthorized,
		},
		{
			name: "allowlist miss then org member",
			cfg: &GitHubConfig{
				AllowedUsers: []string{"someone-else"},
				Org:          "ethpandaops",
			},
			username: "octocat",
			mock:     githubMockConfig{memberStatus: http.StatusNoContent},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newGitHubMock(t, tc.mock)
			withGitHubEndpoints(t, srv.URL)

			_, client := testutil.NewMiniredisClient(t)
			h := newTestGitHubHandler(t, tc.cfg, client, srv.Client())

			err := h.validateUser(context.Background(), "token", tc.username)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)

				return
			}

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestValidateUserOrgCheckError(t *testing.T) {
	origAPI := githubAPIBase
	githubAPIBase = "http://127.0.0.1:1"

	t.Cleanup(func() { githubAPIBase = origAPI })

	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, &GitHubConfig{Org: "ethpandaops"}, client, &http.Client{
		Timeout: 100 * time.Millisecond,
	})

	err := h.validateUser(context.Background(), "token", "octocat")
	require.Error(t, err)
	require.Contains(t, err.Error(), "check org membership")
}

func TestCheckOrgMembership(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantMember bool
	}{
		{name: "member", status: http.StatusNoContent, wantMember: true},
		{name: "not member", status: http.StatusNotFound, wantMember: false},
		{name: "redirect not member", status: http.StatusFound, wantMember: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newGitHubMock(t, githubMockConfig{memberStatus: tc.status})
			withGitHubEndpoints(t, srv.URL)

			_, client := testutil.NewMiniredisClient(t)
			h := newTestGitHubHandler(t, defaultGitHubConfig(), client, srv.Client())

			member, err := h.checkOrgMembership(
				context.Background(), "token", "ethpandaops", "octocat",
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantMember, member)
		})
	}
}

func TestCheckOrgMembershipRequestError(t *testing.T) {
	origAPI := githubAPIBase
	githubAPIBase = "http://127.0.0.1:1"

	t.Cleanup(func() { githubAPIBase = origAPI })

	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, &http.Client{
		Timeout: 100 * time.Millisecond,
	})

	_, err := h.checkOrgMembership(context.Background(), "token", "org", "user")
	require.Error(t, err)
	require.Contains(t, err.Error(), "do request")
}

func TestGithubAPIGetReadError(t *testing.T) {
	// Server claims a larger Content-Length than it sends, then closes the
	// connection, forcing io.ReadAll to fail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		if hj, ok := w.(http.Hijacker); ok {
			conn, _, herr := hj.Hijack()
			if herr == nil {
				_ = conn.Close()
			}
		}
	}))
	t.Cleanup(srv.Close)

	origAPI := githubAPIBase
	githubAPIBase = srv.URL

	t.Cleanup(func() { githubAPIBase = origAPI })

	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, srv.Client())

	_, err := h.fetchGitHubUser(context.Background(), "token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "read body")
}

func TestGithubAPIGetCreateRequestError(t *testing.T) {
	// A control character in the URL makes http.NewRequestWithContext fail.
	origAPI := githubAPIBase
	githubAPIBase = "http://invalid\x7fhost"

	t.Cleanup(func() { githubAPIBase = origAPI })

	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

	_, err := h.fetchGitHubUser(context.Background(), "token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "create request")
}

func TestCheckOrgMembershipCreateRequestError(t *testing.T) {
	origAPI := githubAPIBase
	githubAPIBase = "http://invalid\x7fhost"

	t.Cleanup(func() { githubAPIBase = origAPI })

	_, client := testutil.NewMiniredisClient(t)
	h := newTestGitHubHandler(t, defaultGitHubConfig(), client, http.DefaultClient)

	_, err := h.checkOrgMembership(context.Background(), "token", "org", "user")
	require.Error(t, err)
	require.Contains(t, err.Error(), "create request")
}
