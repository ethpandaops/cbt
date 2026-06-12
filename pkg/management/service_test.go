package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func githubCfg() *GitHubConfig {
	return &GitHubConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		CallbackURL:  "http://localhost:8080/cb",
		Org:          "ethpandaops",
		SessionTTL:   time.Hour,
	}
}

func newSvc(t *testing.T, cfg *Config, client *redis.Client) Service {
	t.Helper()

	return NewService(
		cfg,
		&adminfake.FakeAdminService{},
		&testutil.FakeModelsService{},
		&coordinatorfake.FakeCoordinator{},
		client,
		logrus.New(),
	)
}

func TestNewServiceWithoutGitHub(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true, Auth: AuthConfig{Password: "pw"}}
	s := newSvc(t, cfg, nil)
	require.NotNil(t, s)

	concrete, ok := s.(*svc)
	require.True(t, ok)
	require.Nil(t, concrete.sessionStore)
	require.Nil(t, concrete.githubHandler)
}

func TestNewServiceWithGitHub(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)
	cfg := &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}}
	s := newSvc(t, cfg, client)

	concrete, ok := s.(*svc)
	require.True(t, ok)
	require.NotNil(t, concrete.sessionStore)
	require.NotNil(t, concrete.githubHandler)
}

func TestServiceSetBaseConfigProvider(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true}
	s := newSvc(t, cfg, nil)

	provider := &fakeBaseConfigProvider{}
	s.SetBaseConfigProvider(provider)

	concrete, ok := s.(*svc)
	require.True(t, ok)
	require.Equal(t, BaseConfigProvider(provider), concrete.handlers.baseConfigProvider)
}

func TestGetFrontendConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         *Config
		wantEnabled bool
		wantMethods []string
	}{
		{
			name:        "disabled no auth",
			cfg:         &Config{Enabled: false},
			wantEnabled: false,
			wantMethods: []string{},
		},
		{
			name:        "password only",
			cfg:         &Config{Enabled: true, Auth: AuthConfig{Password: "pw"}},
			wantEnabled: true,
			wantMethods: []string{"password"},
		},
		{
			name:        "github only",
			cfg:         &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}},
			wantEnabled: true,
			wantMethods: []string{"github"},
		},
		{
			name: "both",
			cfg: &Config{Enabled: true, Auth: AuthConfig{
				Password: "pw",
				GitHub:   githubCfg(),
			}},
			wantEnabled: true,
			wantMethods: []string{"password", "github"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var client *redis.Client
			if tc.cfg.Auth.GitHubEnabled() {
				_, client = testutil.NewMiniredisClient(t)
			}

			s := newSvc(t, tc.cfg, client)
			fc := s.GetFrontendConfig()
			require.Equal(t, tc.wantEnabled, fc.ManagementEnabled)
			require.Equal(t, tc.wantMethods, fc.AuthMethods)
		})
	}
}

func TestRegisterRoutesPublicEndpoints(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true}
	s := newSvc(t, cfg, nil)

	app := fiber.New()
	s.RegisterRoutes(app)

	// Auth session is public and reachable.
	req := httptest.NewRequest(http.MethodGet, "/auth/session", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	// Public model config-override read is reachable (invalid id -> 400).
	req2 := httptest.NewRequest(http.MethodGet, "/models/bad/config-override", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, fiber.StatusBadRequest, resp2.StatusCode)
}

func TestRegisterRoutesGitHubEnabled(t *testing.T) {
	t.Parallel()

	_, client := testutil.NewMiniredisClient(t)
	cfg := &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}}
	s := newSvc(t, cfg, client)

	app := fiber.New()
	s.RegisterRoutes(app)

	// GitHub login route exists and redirects.
	req := httptest.NewRequest(http.MethodGet, "/auth/github/login", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, fiber.StatusSeeOther, resp.StatusCode)

	// Admin route requires session auth -> 401 without cookie.
	req2 := httptest.NewRequest(http.MethodPost, "/admin/models/db.table/consolidate", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, fiber.StatusUnauthorized, resp2.StatusCode)
}

func TestRegisterRoutesPasswordProtectsAdmin(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true, Auth: AuthConfig{Password: "secret"}}
	s := newSvc(t, cfg, nil)

	app := fiber.New()
	s.RegisterRoutes(app)

	// Without auth header -> 401.
	req := httptest.NewRequest(http.MethodGet, "/admin/config-overrides", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, fiber.StatusUnauthorized, resp.StatusCode)

	// With valid bearer -> 200.
	req2 := httptest.NewRequest(http.MethodGet, "/admin/config-overrides", http.NoBody)
	req2.Header.Set("Authorization", "Bearer secret")
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, fiber.StatusOK, resp2.StatusCode)
}

func TestRegisterRoutesNoAuthAllowsAdmin(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true}
	s := newSvc(t, cfg, nil)

	app := fiber.New()
	s.RegisterRoutes(app)

	// No auth configured: admin route reachable without credentials.
	req := httptest.NewRequest(http.MethodGet, "/admin/config-overrides", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestHandleAuthSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfg           *Config
		authHeader    string
		withCookie    bool
		seedSession   bool
		wantAuth      bool
		wantUsername  string
		expectMethods []string
	}{
		{
			name:          "no auth always authenticated",
			cfg:           &Config{Enabled: true},
			wantAuth:      true,
			expectMethods: []string{},
		},
		{
			name:          "password valid",
			cfg:           &Config{Enabled: true, Auth: AuthConfig{Password: "pw"}},
			authHeader:    "Bearer pw",
			wantAuth:      true,
			expectMethods: []string{"password"},
		},
		{
			name:          "password missing",
			cfg:           &Config{Enabled: true, Auth: AuthConfig{Password: "pw"}},
			wantAuth:      false,
			expectMethods: []string{"password"},
		},
		{
			name:          "github session valid",
			cfg:           &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}},
			withCookie:    true,
			seedSession:   true,
			wantAuth:      true,
			wantUsername:  "octocat",
			expectMethods: []string{"github"},
		},
		{
			name:          "github no cookie",
			cfg:           &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}},
			wantAuth:      false,
			expectMethods: []string{"github"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var client *redis.Client
			if tc.cfg.Auth.GitHubEnabled() {
				_, client = testutil.NewMiniredisClient(t)
			}

			s := newSvc(t, tc.cfg, client)

			var cookieVal string
			if tc.seedSession {
				concrete, ok := s.(*svc)
				require.True(t, ok)
				id, err := concrete.sessionStore.Create(context.Background(), &SessionData{
					Username: "octocat",
					AuthMode: "github",
				})
				require.NoError(t, err)
				cookieVal = id
			}

			app := fiber.New()
			s.RegisterRoutes(app)

			req := httptest.NewRequest(http.MethodGet, "/auth/session", http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			if tc.withCookie {
				val := cookieVal
				if val == "" {
					val = "missing"
				}

				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: val})
			}

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, fiber.StatusOK, resp.StatusCode)

			var body map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

			require.Equal(t, true, body["management_enabled"])
			require.Equal(t, tc.wantAuth, body["authenticated"])

			if tc.wantUsername != "" {
				require.Equal(t, tc.wantUsername, body["username"])
			}
		})
	}
}

func TestCheckSessionAuthStoreError(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}}
	s := NewService(
		cfg,
		&adminfake.FakeAdminService{},
		&testutil.FakeModelsService{},
		&coordinatorfake.FakeCoordinator{},
		newBrokenRedisClient(),
		logrus.New(),
	)

	app := fiber.New()
	s.RegisterRoutes(app)

	req := httptest.NewRequest(http.MethodGet, "/auth/session", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "any"})

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	// Store error -> not authenticated.
	require.Equal(t, false, body["authenticated"])
}

func TestCheckPasswordAuthBranches(t *testing.T) {
	t.Parallel()

	cfg := &Config{Enabled: true, Auth: AuthConfig{Password: "pw"}}
	s := newSvc(t, cfg, nil)

	app := fiber.New()
	s.RegisterRoutes(app)

	tests := []struct {
		name       string
		authHeader string
		wantAuth   bool
	}{
		{name: "no header", wantAuth: false},
		{name: "wrong scheme", authHeader: "Basic pw", wantAuth: false},
		{name: "wrong token", authHeader: "Bearer nope", wantAuth: false},
		{name: "valid", authHeader: "Bearer pw", wantAuth: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/auth/session", http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			resp, err := app.Test(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			var body map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			require.Equal(t, tc.wantAuth, body["authenticated"])
		})
	}
}

func TestBuildAuthMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     *Config
		wantNil bool
		github  bool
	}{
		{
			name:    "no auth nil middleware",
			cfg:     &Config{Enabled: true},
			wantNil: true,
		},
		{
			name: "password only",
			cfg:  &Config{Enabled: true, Auth: AuthConfig{Password: "pw"}},
		},
		{
			name:   "github only",
			cfg:    &Config{Enabled: true, Auth: AuthConfig{GitHub: githubCfg()}},
			github: true,
		},
		{
			name: "combined",
			cfg: &Config{Enabled: true, Auth: AuthConfig{
				Password: "pw",
				GitHub:   githubCfg(),
			}},
			github: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var client *redis.Client
			if tc.github {
				_, client = testutil.NewMiniredisClient(t)
			}

			s := newSvc(t, tc.cfg, client)
			concrete, ok := s.(*svc)
			require.True(t, ok)

			mw := concrete.buildAuthMiddleware()
			if tc.wantNil {
				require.Nil(t, mw)

				return
			}

			require.NotNil(t, mw)
		})
	}
}
