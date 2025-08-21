// Package observability provides observability utilities
package observability

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

//nolint:gochecknoglobals // Singleton pattern for metrics server
var (
	metricsServerInstance *http.Server
	once                  sync.Once
)

// StartMetricsServer starts a Prometheus metrics server if it hasn't been started already.
func StartMetricsServer(addr string) {
	once.Do(func() {
		if metricsServerInstance != nil {
			return
		}

		sm := http.NewServeMux()
		sm.Handle("/metrics", promhttp.Handler())

		metricsServerInstance = &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 15 * time.Second,
			Handler:           sm,
		}

		go func() {
			logrus.Infof("Starting metrics server on %s", addr)

			if err := metricsServerInstance.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logrus.WithError(err).Fatal("Failed to start metrics server")
			}
		}()
	})
}
