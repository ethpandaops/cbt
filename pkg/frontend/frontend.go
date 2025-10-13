// Package frontend provides a static file server for the frontend assets
package frontend

import (
	"fmt"
	"io/fs"
	"net/http"
	"strings"

	static "github.com/ethpandaops/cbt/frontend"
)

type handler struct {
	fileHandler http.Handler
	filesystem  fs.FS
}

// NewHandler creates a new frontend HTTP handler with SPA fallback support
func NewHandler() (http.Handler, error) {
	frontendFS, err := fs.Sub(static.FS, "build/frontend")
	if err != nil {
		return nil, fmt.Errorf("failed to load frontend filesystem: %w", err)
	}

	h := &handler{
		filesystem:  frontendFS,
		fileHandler: http.FileServer(http.FS(frontendFS)),
	}

	return h, nil
}

// ServeHTTP handles frontend requests with SPA fallback support
func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Check if the file exists
	path := strings.TrimPrefix(req.URL.Path, "/")
	if h.fileExists(path) {
		h.fileHandler.ServeHTTP(w, req)
		return
	}

	// Fall back to index.html for SPA routing
	req.URL.Path = "/"
	h.fileHandler.ServeHTTP(w, req)
}

// fileExists checks if a file exists in the frontend filesystem
func (h *handler) fileExists(path string) bool {
	_, err := fs.Stat(h.filesystem, path)
	return err == nil
}
