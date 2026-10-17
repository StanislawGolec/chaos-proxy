package dataplane

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
)

// DataPlane manages the transparent TCP proxy listener, tunnel lifecycle, and active connections.
type DataPlane struct {
	cfg        *config.Config
	rules      *engine.RuleContainer
	pool       *BufferPool
	listener   net.Listener
	activeConn atomic.Int64
	totalPkts  atomic.Uint64
	totalBytes atomic.Uint64
	tunnelSeq  atomic.Uint64
	mu         sync.RWMutex
	tunnels    map[string]*Pipe
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	logger     *slog.Logger
	closed     atomic.Bool
}

// NewDataPlane creates a new DataPlane instance.
func NewDataPlane(cfg *config.Config, rules *engine.RuleContainer) *DataPlane {
	ctx, cancel := context.WithCancel(context.Background())
	return &DataPlane{
		cfg:     cfg,
		rules:   rules,
		pool:    NewBufferPool(cfg.BufferSize),
		tunnels: make(map[string]*Pipe),
		ctx:     ctx,
		cancel:  cancel,
		logger:  slog.Default().With("component", "dataplane"),
	}
}

// Start binds the TCP listener and begins accepting incoming client connections.
func (dp *DataPlane) Start() error {
	l, err := net.Listen("tcp", dp.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind listener on %s: %w", dp.cfg.ListenAddr, err)
	}
	dp.listener = l
	dp.logger.Info("Data plane TCP listener established", "listen_addr", dp.listener.Addr().String(), "target_addr", dp.cfg.TargetAddr)

	dp.wg.Add(1)
	go dp.acceptLoop()

	return nil
}

// acceptLoop runs the listener acceptance loop until cancelled or closed.
func (dp *DataPlane) acceptLoop() {
	defer dp.wg.Done()

	for {
		clientConn, err := dp.listener.Accept()
		if err != nil {
			if dp.closed.Load() {
				return
			}
			dp.logger.Warn("Failed to accept client connection", "error", err)
			select {
			case <-dp.ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		dp.wg.Add(1)
		go dp.handleConnection(clientConn)
	}
}

// handleConnection establishes upstream server connection and launches bidirectional forwarding pipe.
func (dp *DataPlane) handleConnection(clientConn net.Conn) {
	defer dp.wg.Done()

	tunnelID := fmt.Sprintf("tunnel-%d", dp.tunnelSeq.Add(1))
	remoteAddr := clientConn.RemoteAddr().String()

	dp.logger.Debug("New incoming connection accepted", "tunnel_id", tunnelID, "client_addr", remoteAddr)

	// Dial upstream target server
	dialer := net.Dialer{
		Timeout: dp.cfg.ConnectTimeout,
	}
	serverConn, err := dialer.DialContext(dp.ctx, "tcp", dp.cfg.TargetAddr)
	if err != nil {
		dp.logger.Error("Failed to connect to upstream target", "tunnel_id", tunnelID, "target", dp.cfg.TargetAddr, "error", err)
		_ = clientConn.Close()
		return
	}

	pipe := NewPipe(
		tunnelID,
		clientConn,
		serverConn,
		dp.pool,
		dp.rules,
		dp.cfg.Mode,
		&dp.totalPkts,
		&dp.totalBytes,
		dp.logger,
	)

	// Register tunnel
	dp.mu.Lock()
	dp.tunnels[tunnelID] = pipe
	dp.activeConn.Add(1)
	dp.mu.Unlock()

	defer func() {
		dp.mu.Lock()
		delete(dp.tunnels, tunnelID)
		dp.activeConn.Add(-1)
		dp.mu.Unlock()
		pipe.Close()
		dp.logger.Debug("Connection tunnel closed", "tunnel_id", tunnelID)
	}()

	// Start streaming
	if err := pipe.Start(dp.ctx); err != nil && !dp.closed.Load() {
		dp.logger.Debug("Pipe closed with error", "tunnel_id", tunnelID, "error", err)
	}
}

// Close gracefully terminates the listener and all active connection pipes.
func (dp *DataPlane) Close() error {
	if !dp.closed.CompareAndSwap(false, true) {
		return nil
	}

	dp.logger.Info("Shutting down data plane...")
	dp.cancel()

	if dp.listener != nil {
		_ = dp.listener.Close()
	}

	// Close all active tunnels
	dp.mu.Lock()
	for _, pipe := range dp.tunnels {
		pipe.Close()
	}
	dp.mu.Unlock()

	// Wait for goroutines to drain with timeout
	done := make(chan struct{})
	go func() {
		dp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		dp.logger.Info("All active data plane tunnels drained cleanly")
	case <-time.After(3 * time.Second):
		dp.logger.Warn("Timed out waiting for data plane tunnels to drain")
	}

	return nil
}

// Addr returns the listener address or nil if unstarted.
func (dp *DataPlane) Addr() net.Addr {
	if dp.listener != nil {
		return dp.listener.Addr()
	}
	return nil
}

// ActiveConnections returns the number of currently open proxy tunnels.
func (dp *DataPlane) ActiveConnections() int64 {
	return dp.activeConn.Load()
}

// TotalPackets returns the total number of packets forwarded or acted upon.
func (dp *DataPlane) TotalPackets() uint64 {
	return dp.totalPkts.Load()
}

// TotalBytes returns the total number of bytes processed.
func (dp *DataPlane) TotalBytes() uint64 {
	return dp.totalBytes.Load()
}
