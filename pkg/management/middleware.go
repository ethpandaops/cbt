package management

import (
	"crypto/subtle"
	"strings"

	"github.com/gofiber/fiber/v3"
)

// PasswordAuthMiddleware returns middleware that validates a Bearer token
// against the expected password using constant-time comparison.
func PasswordAuthMiddleware(expectedPassword string) fiber.Handler {
	expected := []byte(expectedPassword)

	return func(c fiber.Ctx) error {
		auth := c.Get("Authorization")
		if auth == "" {
			return fiber.NewError(
				fiber.StatusUnauthorized, "missing authorization header",
			)
		}

		token, found := strings.CutPrefix(auth, "Bearer ")
		if !found {
			return fiber.NewError(
				fiber.StatusUnauthorized, "invalid authorization format",
			)
		}

		if subtle.ConstantTimeCompare([]byte(token), expected) != 1 {
			return fiber.NewError(
				fiber.StatusUnauthorized, "invalid credentials",
			)
		}

		return c.Next()
	}
}

// SessionAuthMiddleware returns middleware that validates a session cookie
// against the Redis session store.
func SessionAuthMiddleware(store *SessionStore) fiber.Handler {
	return func(c fiber.Ctx) error {
		sessionID := c.Cookies("cbt_session")
		if sessionID == "" {
			return fiber.NewError(
				fiber.StatusUnauthorized, "missing session cookie",
			)
		}

		data, err := store.Get(c.Context(), sessionID)
		if err != nil {
			return fiber.NewError(
				fiber.StatusInternalServerError, "session lookup failed",
			)
		}

		if data == nil {
			return fiber.NewError(
				fiber.StatusUnauthorized, "invalid or expired session",
			)
		}

		c.Locals("session", data)

		return c.Next()
	}
}

// CombinedAuthMiddleware returns middleware that accepts either Bearer token
// or session cookie authentication. It tries password first, then session.
func CombinedAuthMiddleware(
	expectedPassword string,
	store *SessionStore,
) fiber.Handler {
	passwordExpected := []byte(expectedPassword)

	return func(c fiber.Ctx) error {
		// Try Bearer token first.
		if auth := c.Get("Authorization"); auth != "" {
			if token, found := strings.CutPrefix(auth, "Bearer "); found {
				if subtle.ConstantTimeCompare(
					[]byte(token), passwordExpected,
				) == 1 {
					return c.Next()
				}
			}
		}

		// Try session cookie.
		if sessionID := c.Cookies("cbt_session"); sessionID != "" {
			data, err := store.Get(c.Context(), sessionID)
			if err == nil && data != nil {
				c.Locals("session", data)

				return c.Next()
			}
		}

		return fiber.NewError(
			fiber.StatusUnauthorized, "invalid credentials",
		)
	}
}
