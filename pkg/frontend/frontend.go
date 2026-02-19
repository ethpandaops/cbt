// Package frontend provides a static file server for the frontend assets.
package frontend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"time"

	static "github.com/ethpandaops/cbt/frontend"
)

// InjectedConfig holds runtime configuration injected into the frontend HTML.
type InjectedConfig struct {
	ManagementEnabled bool     `json:"managementEnabled,omitempty"`
	AuthMethods       []string `json:"authMethods,omitempty"`
}

type handler struct {
	// cachedIndex holds the index.html content read at startup.
	cachedIndex []byte

	filesystem fs.FS
}

// NewHandler creates a new frontend HTTP handler with SPA fallback support.
// If cfg is non-nil, its fields are injected as window.__CONFIG__ into
// index.html.
func NewHandler(cfg *InjectedConfig) (http.Handler, error) {
	frontendFS, err := fs.Sub(static.FS, "build/frontend")
	if err != nil {
		return nil, fmt.Errorf("failed to load frontend filesystem: %w", err)
	}

	raw, err := fs.ReadFile(frontendFS, "index.html")
	if err != nil {
		return nil, fmt.Errorf("read index.html: %w", err)
	}

	if cfg != nil {
		raw, err = injectConfig(raw, cfg)
		if err != nil {
			return nil, fmt.Errorf("inject config: %w", err)
		}
	}

	h := &handler{
		cachedIndex: raw,
		filesystem:  frontendFS,
	}

	return h, nil
}

// ServeHTTP handles frontend requests with SPA fallback support.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Clean the path; strip leading slash for fs.Open.
	p := path.Clean(r.URL.Path)
	if p == "/" {
		p = "index.html"
	} else {
		// fs.FS paths must not start with /.
		p = p[1:]
	}

	// index.html is always served via the cached copy.
	if p == "index.html" {
		h.serveIndex(w, r)

		return
	}

	f, err := h.filesystem.Open(p)
	if err != nil {
		// File not found — serve index.html for SPA routing.
		h.serveIndex(w, r)

		return
	}

	stat, stErr := f.Stat()
	if stErr != nil || stat.IsDir() {
		_ = f.Close()
		h.serveIndex(w, r)

		return
	}

	serveAndClose(f, stat, w, r)
}

func (h *handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeContent(w, r, "index.html", time.Time{}, bytes.NewReader(h.cachedIndex))
}

// serveAndClose writes the file content to the response and closes the file.
func serveAndClose(f fs.File, stat fs.FileInfo, w http.ResponseWriter, r *http.Request) {
	defer func() { _ = f.Close() }()

	rs, ok := f.(io.ReadSeeker)
	if !ok {
		http.Error(w, "internal error", http.StatusInternalServerError)

		return
	}

	http.ServeContent(w, r, stat.Name(), stat.ModTime(), rs)
}

// injectConfig serializes cfg as JSON and injects a <script> tag setting
// window.__CONFIG__ before the closing </head> tag.
func injectConfig(html []byte, cfg *InjectedConfig) ([]byte, error) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}

	tag := fmt.Sprintf(
		`<script>window.__CONFIG__=%s;</script>`, data,
	)

	return bytes.Replace(html, []byte("</head>"), []byte(tag+"</head>"), 1), nil
}
