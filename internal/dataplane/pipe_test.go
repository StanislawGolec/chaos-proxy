package dataplane

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/StanislawGolec/go-chaos-proxy/internal/config"
	"github.com/StanislawGolec/go-chaos-proxy/internal/engine"
	"github.com/StanislawGolec/go-chaos-proxy/internal/metrics"
)

func TestBufferPool(t *testing.T) {
	poolSize := 4096
	bp := NewBufferPool(poolSize)

	if bp.BufferSize() != poolSize {
		t.Fatalf("expected buffer size %d, got %d", poolSize, bp.BufferSize())
	}

	buf := bp.Get()
	if len(buf) != poolSize {
		t.Fatalf("expected slice length %d, got %d", poolSize, len(buf))
	}
	if cap(buf) < poolSize {
		t.Fatalf("expected slice capacity >= %d, got %d", poolSize, cap(buf))
	}

	copy(buf, []byte("chaos-proxy-test"))
	bp.Put(buf)

	// Retrieve again to ensure reuse without panic
	reused := bp.Get()
	if len(reused) != poolSize {
		t.Fatalf("expected reused slice length %d, got %d", poolSize, len(reused))
	}
}

func TestDataPlaneTransparentProxying(t *testing.T) {
	// 1. Start mock TCP echo upstream server
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	// 2. Initialize DataPlane forwarding to echo upstream
	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0" // Dynamic port
	cfg.TargetAddr = echoListener.Addr().String()
	cfg.ConnectTimeout = 2 * time.Second

	rules := engine.NewRuleContainer()
	dp := NewDataPlane(cfg, rules)

	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}
	defer dp.Close()

	proxyAddr := dp.Addr().String()

	// 3. Connect mock client to proxy
	clientConn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial proxy at %s: %v", proxyAddr, err)
	}
	defer clientConn.Close()

	// Allow pipe setup
	time.Sleep(50 * time.Millisecond)

	if active := dp.ActiveConnections(); active != 1 {
		t.Errorf("expected 1 active connection, got %d", active)
	}

	// 4. Send sample Ultimo protocol command through proxy
	payload := []byte("JOIN Alice room_1\n")
	if _, err := clientConn.Write(payload); err != nil {
		t.Fatalf("failed to write to client connection: %v", err)
	}

	reply := make([]byte, len(payload))
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("failed to read echoed reply through proxy: %v", err)
	}

	if !bytes.Equal(payload, reply) {
		t.Fatalf("payload mismatch: expected %q, got %q", string(payload), string(reply))
	}

	// Verify metrics counters
	if pkts := dp.TotalPackets(); pkts < 2 {
		t.Errorf("expected at least 2 packets processed, got %d", pkts)
	}
	if bytesTotal := dp.TotalBytes(); bytesTotal < uint64(len(payload)*2) {
		t.Errorf("expected at least %d bytes processed, got %d", len(payload)*2, bytesTotal)
	}

	// 5. Close client and verify connection cleanup
	_ = clientConn.Close()
	time.Sleep(100 * time.Millisecond)

	if active := dp.ActiveConnections(); active != 0 {
		t.Errorf("expected 0 active connections after client disconnect, got %d", active)
	}
}

func TestDataPlaneUpstreamUnreachable(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TargetAddr = "127.0.0.1:54321" // Port not in use
	cfg.ConnectTimeout = 100 * time.Millisecond

	rules := engine.NewRuleContainer()
	dp := NewDataPlane(cfg, rules)

	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}
	defer dp.Close()

	clientConn, err := net.Dial("tcp", dp.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer clientConn.Close()

	// Upstream connect will fail, and clientConn should be closed by proxy
	buf := make([]byte, 16)
	_ = clientConn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, rerr := clientConn.Read(buf)
	if n != 0 || (rerr != io.EOF && rerr == nil) {
		t.Fatalf("expected EOF on unreachable upstream, got n=%d, err=%v", n, rerr)
	}
}

func TestDataPlaneWithPrometheusMetrics(t *testing.T) {
	// 1. Echo upstream
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	// 2. DataPlane with Prometheus metrics
	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TargetAddr = echoListener.Addr().String()
	cfg.MetricsAddr = "127.0.0.1:0"

	rules := engine.NewRuleContainer()
	m := metrics.NewMetrics(cfg)
	dp := NewDataPlane(cfg, rules, m)

	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}
	defer dp.Close()

	// 3. Connect client and send payload
	clientConn, err := net.Dial("tcp", dp.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer clientConn.Close()

	payload := []byte("PLAY RED_7\n")
	if _, err := clientConn.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	reply := make([]byte, len(payload))
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("failed to read reply: %v", err)
	}

	// 4. Verify Prometheus metrics updated
	mfs, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("failed to gather prometheus metrics: %v", err)
	}

	foundPackets := false
	foundBytes := false
	for _, mf := range mfs {
		if mf.GetName() == "chaos_packets_total" {
			foundPackets = true
		}
		if mf.GetName() == "chaos_bytes_transferred_total" {
			foundBytes = true
		}
	}

	if !foundPackets {
		t.Errorf("expected chaos_packets_total metric to be gathered")
	}
	if !foundBytes {
		t.Errorf("expected chaos_bytes_transferred_total metric to be gathered")
	}
}

