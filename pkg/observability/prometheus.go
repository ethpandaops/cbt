// Package observability provides observability utilities
package observability

import (
	"errors"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

//nolint:gochecknoglobals // Singleton pattern for metrics server
var (
	metricsServerInstance *http.Server
	once                  sync.Once

	// fatalf reports an unrecoverable metrics-server error. It is a variable so
	// tests can observe the failure path without terminating the process.
	fatalf = defaultFatalf

	// osExit is indirected so the fatal path can be exercised in tests.
	osExit = os.Exit
)

// defaultFatalf logs an unrecoverable metrics-server error and exits.
func defaultFatalf(err error) {
	logrus.WithError(err).Error("Failed to start metrics server")
	osExit(1)
}

// StartMetricsServer starts a Prometheus metrics server if it hasn't been started already.
func StartMetricsServer(addr string) {
	once.Do(func() {
		metricsServerInstance = newMetricsServer(addr)

		go runMetricsServer(metricsServerInstance)
	})
}

// newMetricsServer builds the metrics HTTP server exposing /metrics.
func newMetricsServer(addr string) *http.Server {
	sm := http.NewServeMux()
	sm.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 15 * time.Second,
		Handler:           sm,
	}
}

// runMetricsServer serves the metrics server until it is closed. A clean
// shutdown (http.ErrServerClosed) is ignored; any other error is fatal.
func runMetricsServer(srv *http.Server) {
	logrus.Infof("Starting metrics server on %s", srv.Addr)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatalf(err)
	}
}
