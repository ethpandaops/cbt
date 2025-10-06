package handlers

import "github.com/gofiber/fiber/v3"

// ErrModelNotFound is returned when a model is not found
var ErrModelNotFound = fiber.NewError(fiber.StatusNotFound, "model not found")

// ErrInvalidModelID is returned when an invalid model ID format is provided
var ErrInvalidModelID = fiber.NewError(fiber.StatusBadRequest, "invalid model ID format, expected database.table")
