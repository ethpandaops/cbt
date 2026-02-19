package management

import (
	"crypto/subtle"
	"strings"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// FrontendConfig holds configuration injected into the frontend.
type FrontendConfig struct {
	ManagementEnabled bool     `json:"managementEnabled"`
	AuthMethods       []string `json:"authMethods"`
}

// Service defines the management service interface.
type Service interface {
	// RegisterRoutes registers management routes on the given router.
	RegisterRoutes(router fiber.Router)
	// GetFrontendConfig returns the configuration to inject into the frontend.
	GetFrontendConfig() FrontendConfig
}

type svc struct {
	config        *Config
	handlers      *Handlers
	sessionStore  *SessionStore
	githubHandler *GitHubHandler
	log           logrus.FieldLogger
}

// NewService creates a new management Service.
func NewService(
	cfg *Config,
	adminService admin.Service,
	modelsService models.Service,
	coord coordinator.Service,
	redisClient *redis.Client,
	log logrus.FieldLogger,
) Service {
	l := log.WithField("component", "management")
	handlers := NewHandlers(adminService, modelsService, coord, l)

	s := &svc{
		config:   cfg,
		handlers: handlers,
		log:      l,
	}

	// Set up GitHub OAuth if configured.
	if cfg.Auth.GitHubEnabled() {
		s.sessionStore = NewSessionStore(
			redisClient, cfg.Auth.GitHub.SessionTTL,
		)
		s.githubHandler = NewGitHubHandler(
			cfg.Auth.GitHub, redisClient, s.sessionStore, l,
		)
	}

	return s
}

// GetFrontendConfig returns the management configuration for the frontend.
func (s *svc) GetFrontendConfig() FrontendConfig {
	return FrontendConfig{
		ManagementEnabled: s.config.Enabled,
		AuthMethods:       s.config.Auth.Methods(),
	}
}

// RegisterRoutes registers all management-related routes.
func (s *svc) RegisterRoutes(router fiber.Router) {
	// Auth status endpoint (always public).
	router.Get("/auth/session", s.handleAuthSession)

	// GitHub OAuth routes.
	if s.config.Auth.GitHubEnabled() {
		router.Get("/auth/github/login", s.githubHandler.HandleLogin)
		router.Get(
			"/auth/github/callback", s.githubHandler.HandleCallback,
		)
		router.Post("/auth/logout", s.githubHandler.HandleLogout)
	}

	// Admin routes with auth middleware (if any).
	adminGroup := router.Group("/admin")

	if mw := s.buildAuthMiddleware(); mw != nil {
		adminGroup.Use(mw)
	}

	adminGroup.Post("/models/:id/delete-period", s.handlers.DeletePeriod)
	adminGroup.Post("/models/:id/consolidate", s.handlers.Consolidate)
	adminGroup.Put("/models/:id/bounds", s.handlers.UpdateBounds)
	adminGroup.Delete("/models/:id/bounds", s.handlers.DeleteBounds)
	adminGroup.Post("/models/:id/refresh-bounds", s.handlers.TriggerRefreshBounds)
}

// handleAuthSession returns the current authentication status.
func (s *svc) handleAuthSession(c fiber.Ctx) error {
	methods := s.config.Auth.Methods()

	response := fiber.Map{
		"management_enabled": true,
		"auth_methods":       methods,
	}

	// No auth configured — always authenticated.
	if !s.config.Auth.AuthRequired() {
		response["authenticated"] = true

		return c.JSON(response)
	}

	if s.checkPasswordAuth(c) {
		response["authenticated"] = true

		return c.JSON(response)
	}

	if username, ok := s.checkSessionAuth(c); ok {
		response["authenticated"] = true
		response["username"] = username

		return c.JSON(response)
	}

	response["authenticated"] = false

	return c.JSON(response)
}

// checkPasswordAuth validates a Bearer token from the Authorization header.
func (s *svc) checkPasswordAuth(c fiber.Ctx) bool {
	if !s.config.Auth.PasswordEnabled() {
		return false
	}

	auth := c.Get("Authorization")
	if auth == "" {
		return false
	}

	token, found := strings.CutPrefix(auth, "Bearer ")
	if !found {
		return false
	}

	return subtle.ConstantTimeCompare(
		[]byte(token), []byte(s.config.Auth.Password),
	) == 1
}

// checkSessionAuth validates a session cookie and returns the username if
// valid.
func (s *svc) checkSessionAuth(c fiber.Ctx) (string, bool) {
	if !s.config.Auth.GitHubEnabled() || s.sessionStore == nil {
		return "", false
	}

	sessionID := c.Cookies(sessionCookieName)
	if sessionID == "" {
		return "", false
	}

	data, err := s.sessionStore.Get(c.Context(), sessionID)
	if err != nil || data == nil {
		return "", false
	}

	return data.Username, true
}

// buildAuthMiddleware returns the appropriate auth middleware based on config,
// or nil if no auth is required.
func (s *svc) buildAuthMiddleware() fiber.Handler {
	passwordEnabled := s.config.Auth.PasswordEnabled()
	githubEnabled := s.config.Auth.GitHubEnabled()

	switch {
	case passwordEnabled && githubEnabled:
		return CombinedAuthMiddleware(
			s.config.Auth.Password, s.sessionStore,
		)
	case passwordEnabled:
		return PasswordAuthMiddleware(s.config.Auth.Password)
	case githubEnabled:
		return SessionAuthMiddleware(s.sessionStore)
	default:
		return nil
	}
}
