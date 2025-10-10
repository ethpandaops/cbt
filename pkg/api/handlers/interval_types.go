package handlers

import (
	"github.com/gofiber/fiber/v3"
)

// GetIntervalTypes returns the configured interval type transformations
// GET /api/v1/interval/types
func (s *Server) GetIntervalTypes(c fiber.Ctx) error {
	response := map[string]interface{}{
		"interval_types": s.intervalTypes,
	}

	return c.JSON(response)
}
