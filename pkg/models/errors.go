package models

import "errors"

// Model-specific errors
var (
	ErrModelNotFound      = errors.New("model not found")
	ErrInvalidFrontmatter = errors.New("invalid frontmatter format")
	ErrValidationFailed   = errors.New("validation failed")
)
