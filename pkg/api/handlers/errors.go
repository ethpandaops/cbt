package handlers

import (
	"errors"

	"github.com/gofiber/fiber/v3"
)

// ErrModelNotFound is returned when a model is not found
var ErrModelNotFound = fiber.NewError(fiber.StatusNotFound, "model not found")

// ErrInvalidModelID is returned when an invalid model ID format is provided
var ErrInvalidModelID = fiber.NewError(fiber.StatusBadRequest, "invalid model ID format, expected database.table")

// Static errors for dependency debug node-type assertions.
var (
	// ErrNodeNotExternal is returned when a node is not an external model.
	ErrNodeNotExternal = errors.New("node is not external model")
	// ErrNodeNotTransformation is returned when a node is not a transformation model.
	ErrNodeNotTransformation = errors.New("node is not transformation model")
)
