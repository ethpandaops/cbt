// Package modelid provides utilities for model identification.
package modelid

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidModelID is returned when a model ID is not in the expected format.
var ErrInvalidModelID = errors.New("invalid model ID format, expected database.table")

// Format creates a standardized model ID from database and table names (format: "database.table").
func Format(database, table string) string {
	return fmt.Sprintf("%s.%s", database, table)
}

// Parse splits a model ID into database and table components.
// Returns an error if the format is invalid.
func Parse(modelID string) (database, table string, err error) {
	parts := strings.Split(modelID, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: %s", ErrInvalidModelID, modelID)
	}

	return parts[0], parts[1], nil
}
