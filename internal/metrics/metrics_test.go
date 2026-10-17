package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
)

func newTestMetrics() (*Metrics, *config.Config) {
	cfg := &config.Config{
		MetricsAddr: ":0",
	}
	return NewMetrics(cfg), cfg
}

func TestPrometheusMetricsRegistration(t *testing.T) {
	m, _ := newTestMetrics()

	// Record sample traffic
	m.RecordPacket("C2S", "pass", 1024)
	m.RecordPacket("C2S", "delay", 512)
	m.RecordPacket("S2C", "drop", 256)
	m.RecordPacket("S2C", "corrupt", 128)
	m.RecordLatency(150 * time.Millisecond)
	m.IncRuleUpdates()
	m.ActiveTunnels.Set(5)

	// Create test HTTP request to /metrics
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 from /metrics, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read /metrics body: %v", err)
	}
	metricsOutput := string(body)

	// Verify all custom metric names appear in the exposition format
	expectedMetrics := []string{
		"chaos_active_tunnels",
		"chaos_packets_total",
		"chaos_bytes_transferred_total",
		"chaos_injected_latency_seconds",
		"chaos_control_plane_rule_updates_total",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(metricsOutput, metric) {
			t.Errorf("expected metric '%s' in /metrics output", metric)
		}
	}

	// Verify label values
	if !strings.Contains(metricsOutput, `direction="c2s"`) {
		t.Errorf("expected direction=\"c2s\" in metrics output")
	}
	if !strings.Contains(metricsOutput, `action="delay"`) {
		t.Errorf("expected action=\"delay\" in metrics output")
	}
}

func TestHealthzEndpoint(t *testing.T) {
	m, _ := newTestMetrics()

	// Initial state: healthy
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected HTTP 200, got %d", resp.StatusCode)
	}

	var data map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&data)
	if data["status"] != "UP" {
		t.Errorf("expected status UP, got %v", data["status"])
	}

	// Degraded state: unhealthy
	m.SetHealthy(false)
	w2 := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w2, req)

	resp2 := w2.Result()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected HTTP 503 when unhealthy, got %d", resp2.StatusCode)
	}

	var data2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&data2)
	if data2["status"] != "DOWN" {
		t.Errorf("expected status DOWN, got %v", data2["status"])
	}
}

func TestReadyzEndpoint(t *testing.T) {
	m, _ := newTestMetrics()

	// Initial state: not ready
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected HTTP 503 when not ready, got %d", resp.StatusCode)
	}

	// Transition to ready
	m.SetReady(true)
	w2 := httptest.NewRecorder()
	m.server.Handler.ServeHTTP(w2, req)

	resp2 := w2.Result()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected HTTP 200 when ready, got %d", resp2.StatusCode)
	}

	var data map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&data)
	if data["status"] != "READY" {
		t.Errorf("expected status READY, got %v", data["status"])
	}
}

func TestPprofEndpoints(t *testing.T) {
	m, _ := newTestMetrics()

	endpoints := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/goroutine",
		"/debug/pprof/heap",
		"/debug/pprof/threadcreate",
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		w := httptest.NewRecorder()
		m.server.Handler.ServeHTTP(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("endpoint %s returned HTTP %d, expected 200", ep, resp.StatusCode)
		}
	}
}

func TestServerLifecycle(t *testing.T) {
	cfg := &config.Config{
		MetricsAddr: "127.0.0.1:0", // OS-assigned port
	}
	m := NewMetrics(cfg)

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start()
	}()

	// Wait briefly for server to bind
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("failed to shutdown metrics server: %v", err)
	}
}

func TestRegistryAccessor(t *testing.T) {
	m, _ := newTestMetrics()
	reg := m.Registry()
	if reg == nil {
		t.Fatal("expected non-nil registry")
	}

	// Verify custom metric can be gathered
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected non-empty gathered metric families")
	}
}
