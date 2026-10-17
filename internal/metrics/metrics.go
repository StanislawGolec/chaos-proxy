package metrics

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics manages Prometheus metric registration, collection, and HTTP exposition.
type Metrics struct {
	cfg        *config.Config
	server     *http.Server
	registry   *prometheus.Registry
	mu         sync.Mutex
	isReady    bool
	isHealthy  bool
}

// NewMetrics initializes Prometheus collectors and an HTTP server for /metrics and /healthz.
func NewMetrics(cfg *config.Config) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{
		cfg:       cfg,
		registry:  reg,
		isReady:   true,
		isHealthy: true,
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", m.healthHandler)
	mux.HandleFunc("/readyz", m.readyHandler)

	m.server = &http.Server{
		Addr:         cfg.MetricsAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return m
}

func (m *Metrics) healthHandler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isHealthy {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("unhealthy\n"))
}

func (m *Metrics) readyHandler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isReady {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("not ready\n"))
}

// Start begins listening and serving metrics on the configured port.
func (m *Metrics) Start() error {
	return m.server.ListenAndServe()
}

// Shutdown gracefully stops the metrics HTTP server.
func (m *Metrics) Shutdown(ctx context.Context) error {
	return m.server.Shutdown(ctx)
}
