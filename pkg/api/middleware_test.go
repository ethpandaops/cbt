package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupMiddleware(t *testing.T) {
	app := fiber.New()
	setupMiddleware(app)

	app.Get("/ok", func(c fiber.Ctx) error {
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", http.NoBody)
	req.Header.Set("Origin", "http://example.com")

	resp, err := app.Test(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// CORS middleware should echo the wildcard allow-origin.
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestErrorHandler(t *testing.T) {
	tests := []struct {
		name        string
		handlerErr  error
		wantStatus  int
		wantMessage string
	}{
		{
			name:        "fiber_error_uses_its_code_and_message",
			handlerErr:  fiber.NewError(http.StatusNotFound, "missing thing"),
			wantStatus:  http.StatusNotFound,
			wantMessage: "missing thing",
		},
		{
			name:        "generic_error_falls_back_to_500",
			handlerErr:  assert.AnError,
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "Internal Server Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New(fiber.Config{ErrorHandler: errorHandler})
			app.Get("/boom", func(_ fiber.Ctx) error {
				return tt.handlerErr
			})

			req := httptest.NewRequest(http.MethodGet, "/boom", http.NoBody)

			resp, err := app.Test(req)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			var payload struct {
				Error string `json:"error"`
				Code  int    `json:"code"`
			}
			require.NoError(t, json.Unmarshal(body, &payload))

			assert.Equal(t, tt.wantMessage, payload.Error)
			assert.Equal(t, tt.wantStatus, payload.Code)
		})
	}
}
