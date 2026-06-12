package frontend

import (
	"bytes"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHandler(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *InjectedConfig
		wantInject  bool
		wantContent string
	}{
		{
			name:       "nil_config_no_injection",
			cfg:        nil,
			wantInject: false,
		},
		{
			name:        "with_config_injects_window_config",
			cfg:         &InjectedConfig{ManagementEnabled: true, AuthMethods: []string{"github"}},
			wantInject:  true,
			wantContent: "window.__CONFIG__=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := NewHandler(tt.cfg)
			require.NoError(t, err)
			require.NotNil(t, h)

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			h.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			if tt.wantInject {
				assert.Contains(t, body, tt.wantContent)
			} else {
				assert.NotContains(t, body, "window.__CONFIG__=")
			}
		})
	}
}

func TestServeHTTP_Routing(t *testing.T) {
	h, err := NewHandler(nil)
	require.NoError(t, err)

	tests := []struct {
		name       string
		path       string
		wantStatus int
		// wantIndex indicates the response should be the SPA index.html fallback.
		wantIndex bool
	}{
		{
			name:       "root_serves_index",
			path:       "/",
			wantStatus: http.StatusOK,
			wantIndex:  true,
		},
		{
			name:       "explicit_index_html",
			path:       "/index.html",
			wantStatus: http.StatusOK,
			wantIndex:  true,
		},
		{
			name:       "existing_asset_served_directly",
			path:       "/logo.png",
			wantStatus: http.StatusOK,
			wantIndex:  false,
		},
		{
			name:       "missing_path_falls_back_to_index",
			path:       "/some/spa/route",
			wantStatus: http.StatusOK,
			wantIndex:  true,
		},
		{
			name:       "directory_path_falls_back_to_index",
			path:       "/assets",
			wantStatus: http.StatusOK,
			wantIndex:  true,
		},
	}

	// Capture the canonical index.html bytes for comparison.
	idxRec := httptest.NewRecorder()
	h.ServeHTTP(idxRec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))
	indexBody := idxRec.Body.Bytes()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			h.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			isIndex := bytes.Equal(rec.Body.Bytes(), indexBody)
			assert.Equal(t, tt.wantIndex, isIndex)
		})
	}
}

// notSeekerFile is an fs.File that is deliberately NOT an io.ReadSeeker, used to
// exercise the non-ReadSeeker branch of serveAndClose.
type notSeekerFile struct {
	name   string
	closed bool
}

func (f *notSeekerFile) Stat() (fs.FileInfo, error) { return &notSeekerInfo{name: f.name}, nil }
func (f *notSeekerFile) Read(_ []byte) (int, error) { return 0, io.EOF }
func (f *notSeekerFile) Close() error {
	f.closed = true
	return nil
}

type notSeekerInfo struct {
	name string
}

func (i *notSeekerInfo) Name() string       { return i.name }
func (i *notSeekerInfo) Size() int64        { return 0 }
func (i *notSeekerInfo) Mode() fs.FileMode  { return 0 }
func (i *notSeekerInfo) ModTime() time.Time { return time.Time{} }
func (i *notSeekerInfo) IsDir() bool        { return false }
func (i *notSeekerInfo) Sys() any           { return nil }

func TestServeAndClose_NotReadSeeker(t *testing.T) {
	f := &notSeekerFile{name: "weird"}
	stat, err := f.Stat()
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/weird", http.NoBody)

	serveAndClose(f, stat, rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.True(t, f.closed, "file must be closed")
}

// statErrFile is an fs.File whose Stat returns an error, to exercise the Stat
// error branch of ServeHTTP.
type statErrFile struct{ closed bool }

func (f *statErrFile) Stat() (fs.FileInfo, error) { return nil, fs.ErrInvalid }
func (f *statErrFile) Read(_ []byte) (int, error) { return 0, io.EOF }
func (f *statErrFile) Close() error {
	f.closed = true
	return nil
}

// statErrFS returns a statErrFile for any non-index path.
type statErrFS struct{ f *statErrFile }

func (s statErrFS) Open(_ string) (fs.File, error) { return s.f, nil }

func TestServeHTTP_StatError(t *testing.T) {
	sf := &statErrFile{}
	h := &handler{
		cachedIndex: []byte("INDEX"),
		filesystem:  statErrFS{f: sf},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/anything.js", http.NoBody)
	h.ServeHTTP(rec, req)

	// Falls back to the cached index on stat error.
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "INDEX", rec.Body.String())
	assert.True(t, sf.closed, "file must be closed on stat error")
}

func TestNewHandler_ReadIndexError(t *testing.T) {
	// Swap the embedded FS for one that lacks build/frontend/index.html so the
	// index.html read fails.
	orig := staticFS
	staticFS = fstest.MapFS{
		"build/frontend/other.txt": &fstest.MapFile{Data: []byte("x")},
	}
	t.Cleanup(func() { staticFS = orig })

	h, err := NewHandler(nil)
	require.Error(t, err)
	assert.Nil(t, h)
	assert.Contains(t, err.Error(), "index.html")
}

// subErrFS implements fs.SubFS and returns an error from Sub, to exercise the
// fs.Sub error branch in NewHandler.
type subErrFS struct{}

func (subErrFS) Open(_ string) (fs.File, error) { return nil, fs.ErrNotExist }
func (subErrFS) Sub(_ string) (fs.FS, error)    { return nil, fs.ErrInvalid }

func TestNewHandler_SubError(t *testing.T) {
	orig := staticFS
	staticFS = subErrFS{}
	t.Cleanup(func() { staticFS = orig })

	h, err := NewHandler(nil)
	require.Error(t, err)
	assert.Nil(t, h)
	assert.Contains(t, err.Error(), "frontend filesystem")
}

func TestNewHandler_MarshalError(t *testing.T) {
	orig := marshalConfig
	marshalConfig = func(any) ([]byte, error) { return nil, fs.ErrInvalid }
	t.Cleanup(func() { marshalConfig = orig })

	// cfg must be non-nil so NewHandler calls injectConfig, which calls
	// marshalConfig and fails.
	h, err := NewHandler(&InjectedConfig{ManagementEnabled: true})
	require.Error(t, err)
	assert.Nil(t, h)
	assert.Contains(t, err.Error(), "inject config")
}

func TestInjectConfig(t *testing.T) {
	tests := []struct {
		name     string
		html     []byte
		cfg      *InjectedConfig
		wantHead bool
	}{
		{
			name:     "injects_before_head_close",
			html:     []byte("<html><head><title>x</title></head><body></body></html>"),
			cfg:      &InjectedConfig{ManagementEnabled: true},
			wantHead: true,
		},
		{
			name:     "no_head_tag_leaves_html_unchanged",
			html:     []byte("<html><body></body></html>"),
			cfg:      &InjectedConfig{},
			wantHead: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := injectConfig(tt.html, tt.cfg)
			require.NoError(t, err)

			if tt.wantHead {
				assert.Contains(t, string(out), "window.__CONFIG__=")
				assert.Contains(t, string(out), "</head>")
			} else {
				assert.Equal(t, string(tt.html), string(out))
			}
		})
	}
}
