package management

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

const (
	oauthStatePrefix  = "cbt:oauth_state:"
	oauthStateTTL     = 10 * time.Minute
	githubAuthURL     = "https://github.com/login/oauth/authorize"
	githubExchangeURL = "https://github.com/login/oauth/access_token" //nolint:gosec // OAuth endpoint URL, not a credential
	githubAPIBase     = "https://api.github.com"
	sessionCookieName = "cbt_session"
)

var (
	// ErrEmptyGitHubLogin is returned when the GitHub API returns an empty login.
	ErrEmptyGitHubLogin = errors.New("empty login in GitHub API response")
	// ErrUserNotAuthorized is returned when a user is not in the allowed
	// list or org.
	ErrUserNotAuthorized = errors.New("user is not authorized")
	// ErrUnexpectedGitHubStatus is returned when the GitHub API returns an
	// unexpected status code.
	ErrUnexpectedGitHubStatus = errors.New("unexpected GitHub API status")
)

// oauthStateData stores the CSRF state and PKCE verifier for an OAuth flow.
type oauthStateData struct {
	Verifier string `json:"verifier"`
}

// GitHubHandler manages the GitHub OAuth login flow.
type GitHubHandler struct {
	config       *GitHubConfig
	oauth2Config *oauth2.Config
	redisClient  *redis.Client
	sessionStore *SessionStore
	log          logrus.FieldLogger
}

// NewGitHubHandler creates a new GitHub OAuth handler.
func NewGitHubHandler(
	cfg *GitHubConfig,
	redisClient *redis.Client,
	sessionStore *SessionStore,
	log logrus.FieldLogger,
) *GitHubHandler {
	scopes := []string{"read:user"}
	if cfg.Org != "" {
		scopes = append(scopes, "read:org")
	}

	oauth2Cfg := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  cfg.CallbackURL,
		Scopes:       scopes,
		Endpoint: oauth2.Endpoint{
			AuthURL:  githubAuthURL,
			TokenURL: githubExchangeURL,
		},
	}

	return &GitHubHandler{
		config:       cfg,
		oauth2Config: oauth2Cfg,
		redisClient:  redisClient,
		sessionStore: sessionStore,
		log:          log,
	}
}

// HandleLogin initiates the GitHub OAuth flow by generating CSRF state and
// PKCE verifier, storing them in Redis, and redirecting to GitHub.
func (h *GitHubHandler) HandleLogin(c fiber.Ctx) error {
	state, err := generateRandomHex(16)
	if err != nil {
		return fiber.NewError(
			fiber.StatusInternalServerError, "failed to generate state",
		)
	}

	verifier, err := generatePKCEVerifier()
	if err != nil {
		return fiber.NewError(
			fiber.StatusInternalServerError, "failed to generate PKCE verifier",
		)
	}

	stateData := &oauthStateData{Verifier: verifier}

	raw, err := json.Marshal(stateData)
	if err != nil {
		return fiber.NewError(
			fiber.StatusInternalServerError, "failed to marshal state",
		)
	}

	key := oauthStatePrefix + state
	if setErr := h.redisClient.Set(
		c.Context(), key, raw, oauthStateTTL,
	).Err(); setErr != nil {
		return fiber.NewError(
			fiber.StatusInternalServerError, "failed to store state",
		)
	}

	challenge := generatePKCEChallenge(verifier)

	authURL := h.oauth2Config.AuthCodeURL(
		state,
		oauth2.SetAuthURLParam("code_challenge", challenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)

	return c.Redirect().To(authURL)
}

// HandleCallback processes the GitHub OAuth callback, exchanges the code for
// a token, validates the user, creates a session, and redirects to the root.
func (h *GitHubHandler) HandleCallback(c fiber.Ctx) error {
	if oauthErr := c.Query("error"); oauthErr != "" {
		return redirectToFrontendAuthError(
			c, oauthErr, c.Query("error_description"),
		)
	}

	state := c.Query("state")
	code := c.Query("code")

	if state == "" || code == "" {
		return redirectToFrontendAuthError(
			c, "invalid_request", "missing state or code",
		)
	}

	// Retrieve and delete state from Redis.
	key := oauthStatePrefix + state

	raw, err := h.redisClient.GetDel(c.Context(), key).Bytes()
	if err != nil {
		return redirectToFrontendAuthError(
			c, "invalid_grant", "invalid or expired oauth state",
		)
	}

	var stateData oauthStateData
	if unmarshalErr := json.Unmarshal(raw, &stateData); unmarshalErr != nil {
		return redirectToFrontendAuthError(
			c, "server_error", "failed to parse oauth state",
		)
	}

	// Exchange code for token with PKCE verifier.
	token, err := h.oauth2Config.Exchange(
		c.Context(),
		code,
		oauth2.SetAuthURLParam("code_verifier", stateData.Verifier),
	)
	if err != nil {
		h.log.WithError(err).Error("OAuth token exchange failed")

		return redirectToFrontendAuthError(
			c, "access_denied", "token exchange failed",
		)
	}

	// Get the authenticated user's login.
	username, err := h.getGitHubUsername(c.Context(), token.AccessToken)
	if err != nil {
		h.log.WithError(err).Error("Failed to get GitHub username")

		return redirectToFrontendAuthError(
			c, "server_error", "failed to get user info",
		)
	}

	// Validate authorization.
	if validateErr := h.validateUser(
		c.Context(), token.AccessToken, username,
	); validateErr != nil {
		h.log.WithField("username", username).
			WithError(validateErr).
			Warn("GitHub user authorization denied")

		return redirectToFrontendAuthError(
			c, "access_denied", "access denied",
		)
	}

	// Create session.
	sessionID, err := h.sessionStore.Create(c.Context(), &SessionData{
		Username: username,
		AuthMode: "github",
	})
	if err != nil {
		return redirectToFrontendAuthError(
			c, "server_error", "failed to create session",
		)
	}

	// Set session cookie.
	secure := strings.HasPrefix(h.config.CallbackURL, "https://")

	c.Cookie(&fiber.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HTTPOnly: true,
		Secure:   secure,
		SameSite: fiber.CookieSameSiteLaxMode,
		MaxAge:   int(h.config.SessionTTL.Seconds()),
	})

	return c.Redirect().To("/")
}

func redirectToFrontendAuthError(
	c fiber.Ctx,
	code, description string,
) error {
	query := url.Values{}
	query.Set("error", code)
	if description != "" {
		query.Set("error_description", description)
	}

	return c.Redirect().To("/?" + query.Encode())
}

// HandleLogout deletes the session and clears the cookie.
func (h *GitHubHandler) HandleLogout(c fiber.Ctx) error {
	sessionID := c.Cookies(sessionCookieName)
	if sessionID != "" {
		if err := h.sessionStore.Delete(c.Context(), sessionID); err != nil {
			h.log.WithError(err).Warn("Failed to delete session from Redis")
		}
	}

	c.Cookie(&fiber.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HTTPOnly: true,
		MaxAge:   -1,
	})

	return c.JSON(fiber.Map{"ok": true})
}

// getGitHubUsername fetches the authenticated user's login from the GitHub API.
func (h *GitHubHandler) getGitHubUsername(
	ctx context.Context,
	accessToken string,
) (string, error) {
	body, err := h.githubAPIGet(ctx, accessToken, "/user")
	if err != nil {
		return "", fmt.Errorf("get user: %w", err)
	}

	var user struct {
		Login string `json:"login"`
	}

	if err := json.Unmarshal(body, &user); err != nil {
		return "", fmt.Errorf("unmarshal user: %w", err)
	}

	if user.Login == "" {
		return "", ErrEmptyGitHubLogin
	}

	return user.Login, nil
}

// validateUser checks if the user is authorized by org membership and/or
// username allowlist.
func (h *GitHubHandler) validateUser(
	ctx context.Context,
	accessToken string,
	username string,
) error {
	// Check allowlist first (cheaper, no API call).
	if len(h.config.AllowedUsers) > 0 {
		for _, allowed := range h.config.AllowedUsers {
			if strings.EqualFold(allowed, username) {
				return nil
			}
		}
	}

	// Check org membership.
	if h.config.Org != "" {
		isMember, err := h.checkOrgMembership(
			ctx, accessToken, h.config.Org, username,
		)
		if err != nil {
			return fmt.Errorf("check org membership: %w", err)
		}

		if isMember {
			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrUserNotAuthorized, username)
}

// checkOrgMembership checks if a user is a member of the given GitHub org.
func (h *GitHubHandler) checkOrgMembership(
	ctx context.Context,
	accessToken, org, username string,
) (bool, error) {
	endpoint := fmt.Sprintf(
		"%s/orgs/%s/members/%s", githubAPIBase, org, username,
	)

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, endpoint, http.NoBody,
	)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // request host is fixed to githubAPIBase.
	if err != nil {
		return false, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 204 = member, 404 = not member, 302 = requester is not org member.
	return resp.StatusCode == http.StatusNoContent, nil
}

// githubAPIGet performs a GET request against the GitHub API.
func (h *GitHubHandler) githubAPIGet(
	ctx context.Context,
	accessToken, urlPath string,
) ([]byte, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, githubAPIBase+urlPath, http.NoBody,
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // request host is fixed to githubAPIBase.
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"%w: %d", ErrUnexpectedGitHubStatus, resp.StatusCode,
		)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}

func generateRandomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

func generatePKCEVerifier() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

func generatePKCEChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))

	return base64.RawURLEncoding.EncodeToString(h[:])
}
