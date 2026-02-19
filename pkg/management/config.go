// Package management provides management API capabilities with optional
// authentication for the CBT engine.
package management

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrGitHubClientIDRequired is returned when GitHub OAuth is configured
	// without a client ID.
	ErrGitHubClientIDRequired = errors.New("github client_id is required")
	// ErrGitHubClientSecretRequired is returned when GitHub OAuth is
	// configured without a client secret.
	ErrGitHubClientSecretRequired = errors.New(
		"github client_secret is required",
	)
	// ErrGitHubCallbackURLRequired is returned when GitHub OAuth is
	// configured without a callback URL.
	ErrGitHubCallbackURLRequired = errors.New(
		"github callback_url is required",
	)
	// ErrGitHubSessionSecretRequired is returned when GitHub OAuth is
	// configured without a session secret.
	ErrGitHubSessionSecretRequired = errors.New(
		"github session_secret is required",
	)
	// ErrGitHubAuthorizationRequired is returned when GitHub OAuth is
	// configured without org or allowed_users.
	ErrGitHubAuthorizationRequired = errors.New(
		"github requires at least one of org or allowed_users",
	)
)

// Config represents the management API configuration.
type Config struct {
	Enabled bool       `yaml:"enabled"`
	Auth    AuthConfig `yaml:"auth"`
}

// AuthConfig holds authentication configuration for the management API.
type AuthConfig struct {
	// Password is a simple bearer token. Empty means disabled.
	Password string `yaml:"password"`
	// GitHub holds GitHub OAuth configuration. Nil means disabled.
	GitHub *GitHubConfig `yaml:"github"`
}

// GitHubConfig holds GitHub OAuth flow configuration.
type GitHubConfig struct {
	ClientID      string        `yaml:"client_id"`
	ClientSecret  string        `yaml:"client_secret"`
	CallbackURL   string        `yaml:"callback_url"`
	Org           string        `yaml:"org"`
	AllowedUsers  []string      `yaml:"allowed_users"`
	SessionTTL    time.Duration `yaml:"session_ttl"`
	SessionSecret string        `yaml:"session_secret"`
}

// PasswordEnabled reports whether password authentication is configured.
func (c *AuthConfig) PasswordEnabled() bool {
	return c.Password != ""
}

// GitHubEnabled reports whether GitHub OAuth authentication is configured.
func (c *AuthConfig) GitHubEnabled() bool {
	return c.GitHub != nil && c.GitHub.ClientID != ""
}

// AuthRequired reports whether any authentication method is configured.
func (c *AuthConfig) AuthRequired() bool {
	return c.PasswordEnabled() || c.GitHubEnabled()
}

// Methods returns the list of enabled authentication method names.
func (c *AuthConfig) Methods() []string {
	methods := make([]string, 0, 2)

	if c.PasswordEnabled() {
		methods = append(methods, "password")
	}

	if c.GitHubEnabled() {
		methods = append(methods, "github")
	}

	return methods
}

// Validate validates the management configuration.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	return c.Auth.Validate()
}

// Validate validates the authentication configuration.
func (c *AuthConfig) Validate() error {
	if c.GitHub != nil {
		if err := c.GitHub.Validate(); err != nil {
			return fmt.Errorf("github auth: %w", err)
		}
	}

	return nil
}

// Validate validates the GitHub OAuth configuration.
func (c *GitHubConfig) Validate() error {
	if c.ClientID == "" {
		return ErrGitHubClientIDRequired
	}

	if c.ClientSecret == "" {
		return ErrGitHubClientSecretRequired
	}

	if c.CallbackURL == "" {
		return ErrGitHubCallbackURLRequired
	}

	if c.SessionSecret == "" {
		return ErrGitHubSessionSecretRequired
	}

	if c.Org == "" && len(c.AllowedUsers) == 0 {
		return ErrGitHubAuthorizationRequired
	}

	// Default session TTL to 24h if not set.
	if c.SessionTTL == 0 {
		c.SessionTTL = 24 * time.Hour
	}

	return nil
}
