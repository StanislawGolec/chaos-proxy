package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics manages Prometheus metric registration, collection, and HTTP exposition.
type Metrics struct {
	cfg       *config.Config
	server    *http.Server
	registry  *prometheus.Registry
	mu        sync.RWMutex
	isReady   bool
	isHealthy bool

	// SRE Prometheus Collectors
	ActiveTunnels    prometheus.Gauge
	PacketsTotal     *prometheus.CounterVec
	BytesTransferred *prometheus.CounterVec
	InjectedLatency  prometheus.Histogram
	RuleUpdatesTotal prometheus.Counter
}

// NewMetrics initializes Prometheus collectors and an HTTP server for /metrics, /healthz, /readyz, and pprof.
func NewMetrics(cfg *config.Config) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	activeTunnels := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "chaos",
		Name:      "active_tunnels",
		Help:      "Current number of active client-server TCP proxy tunnels.",
	})

	packetsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "chaos",
		Name:      "packets_total",
		Help:      "Total number of network packets processed by the proxy.",
	}, []string{"direction", "action"})

	bytesTransferred := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "chaos",
		Name:      "bytes_transferred_total",
		Help:      "Total number of bytes transferred through the proxy.",
	}, []string{"direction"})

	injectedLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "chaos",
		Name:      "injected_latency_seconds",
		Help:      "Distribution of injected latency delay in seconds.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	})

	ruleUpdatesTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "chaos",
		Name:      "control_plane_rule_updates_total",
		Help:      "Total number of dynamic fault rule updates applied via control plane.",
	})

	reg.MustRegister(activeTunnels, packetsTotal, bytesTransferred, injectedLatency, ruleUpdatesTotal)

	m := &Metrics{
		cfg:              cfg,
		registry:         reg,
		isReady:          false,
		isHealthy:        true,
		ActiveTunnels:    activeTunnels,
		PacketsTotal:     packetsTotal,
		BytesTransferred: bytesTransferred,
		InjectedLatency:  injectedLatency,
		RuleUpdatesTotal: ruleUpdatesTotal,
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", m.healthHandler)
	mux.HandleFunc("/readyz", m.readyHandler)

	// Runtime performance profiling endpoints (net/http/pprof)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	m.server = &http.Server{
		Addr:         cfg.MetricsAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return m
}

func (m *Metrics) healthHandler(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	healthy := m.isHealthy
	m.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if healthy {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "UP",
			"component": "Go-Chaos-Proxy",
		})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "DOWN",
		"component": "Go-Chaos-Proxy",
	})
}

func (m *Metrics) readyHandler(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	ready := m.isReady
	m.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if ready {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "READY",
		})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "NOT_READY",
	})
}

// SetReady sets the readiness status of the proxy.
func (m *Metrics) SetReady(ready bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isReady = ready
}

// SetHealthy sets the health / liveness status of the proxy.
func (m *Metrics) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isHealthy = healthy
}

// IsReady returns the current readiness status.
func (m *Metrics) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isReady
}

// IsHealthy returns the current health status.
func (m *Metrics) IsHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isHealthy
}

// Registry returns the underlying Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// RecordPacket increments the packet counter and adds transferred byte count.
func (m *Metrics) RecordPacket(dir string, action string, bytesCount int) {
	d := strings.ToLower(dir)
	a := strings.ToLower(action)
	m.PacketsTotal.WithLabelValues(d, a).Inc()
	if bytesCount > 0 {
		m.BytesTransferred.WithLabelValues(d).Add(float64(bytesCount))
	}
}

// RecordLatency records an injected delay into the latency histogram.
func (m *Metrics) RecordLatency(d time.Duration) {
	m.InjectedLatency.Observe(d.Seconds())
}

// IncRuleUpdates increments the control plane rule update counter.
func (m *Metrics) IncRuleUpdates() {
	m.RuleUpdatesTotal.Inc()
}

// Start begins listening and serving metrics on the configured address.
func (m *Metrics) Start() error {
	return m.server.ListenAndServe()
}

// Shutdown gracefully stops the metrics HTTP server.
func (m *Metrics) Shutdown(ctx context.Context) error {
	return m.server.Shutdown(ctx)
}
