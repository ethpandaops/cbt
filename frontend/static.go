// Package static provides embedded frontend assets
package static

import "embed"

// FS contains the embedded frontend build files
// Using 'all:' prefix to include files starting with '_' or '.'
//
//go:embed all:build/*
var FS embed.FS
