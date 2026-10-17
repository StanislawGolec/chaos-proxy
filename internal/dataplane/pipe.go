package dataplane

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
)

// Pipe coordinates bi-directional streaming between a client connection and an upstream server connection.
type Pipe struct {
	id         string
	clientConn net.Conn
	serverConn net.Conn
	pool       *BufferPool
	rules      *engine.RuleContainer
	totalPkts  *atomic.Uint64
	totalBytes *atomic.Uint64
	logger     *slog.Logger
	closed     atomic.Bool
	closeOnce  sync.Once
}

// NewPipe constructs a bi-directional streaming pipe.
func NewPipe(
	id string,
	clientConn net.Conn,
	serverConn net.Conn,
	pool *BufferPool,
	rules *engine.RuleContainer,
	totalPkts *atomic.Uint64,
	totalBytes *atomic.Uint64,
	logger *slog.Logger,
) *Pipe {
	if logger == nil {
		logger = slog.Default()
	}

	return &Pipe{
		id:         id,
		clientConn: clientConn,
		serverConn: serverConn,
		pool:       pool,
		rules:      rules,
		totalPkts:  totalPkts,
		totalBytes: totalBytes,
		logger:     logger.With("pipe_id", id),
	}
}

// Start begins bidirectional forwarding and blocks until both streams terminate or ctx is cancelled.
func (p *Pipe) Start(ctx context.Context) error {
	p.optimizeTCP(p.clientConn)
	p.optimizeTCP(p.serverConn)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// Stream 1: Client -> Server (C2S)
	go func() {
		defer wg.Done()
		p.stream(ctx, p.clientConn, p.serverConn, engine.DirectionC2S)
		cancel() // If client finishes or errors, signal peer stream
	}()

	// Stream 2: Server -> Client (S2C)
	go func() {
		defer wg.Done()
		p.stream(ctx, p.serverConn, p.clientConn, engine.DirectionS2C)
		cancel() // If server finishes or errors, signal peer stream
	}()

	wg.Wait()
	p.Close()
	return nil
}

// stream forwards bytes from src to dst in a zero-allocation loop using pooled buffers.
func (p *Pipe) stream(ctx context.Context, src, dst net.Conn, dir engine.Direction) {
	buf := p.pool.Get()
	defer p.pool.Put(buf)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := src.Read(buf)
		if n > 0 {
			p.totalPkts.Add(1)
			p.totalBytes.Add(uint64(n))

			// Check active fault rules if rules container is provided
			action := p.evaluateFault(dir, buf[:n])
			if action != nil {
				if action.Type == engine.ActionDrop {
					p.logger.Debug("Packet dropped by chaos rule", "direction", dir, "bytes", n)
					continue // Discard packet
				}
				if action.Type == engine.ActionDisconnect {
					p.logger.Debug("Disconnecting pipe by chaos rule", "direction", dir)
					p.Close()
					return
				}
				if action.Type == engine.ActionDelay && action.Delay > 0 {
					select {
					case <-time.After(action.Delay):
					case <-ctx.Done():
						return
					}
				}
				if action.Type == engine.ActionCorrupt && action.Mutation != nil {
					buf = action.Mutation(buf[:n])
				}
			}

			// Forward data to destination socket
			if _, werr := dst.Write(buf[:n]); werr != nil {
				if !p.isClosed() {
					p.logger.Debug("Error writing to destination socket", "direction", dir, "error", werr)
				}
				return
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				p.logger.Debug("Stream reached EOF or socket closed", "direction", dir)
				p.propagateHalfClose(dst)
			} else if !p.isClosed() {
				p.logger.Debug("Stream read error", "direction", dir, "error", err)
			}
			return
		}
	}
}

// evaluateFault checks active rules for a given direction.
func (p *Pipe) evaluateFault(dir engine.Direction, payload []byte) *engine.Action {
	if p.rules == nil {
		return nil
	}
	rules := p.rules.Load()
	if rules == nil {
		return nil
	}

	// Disconnect rule check
	if rules.Disconnect != nil && (rules.Disconnect.Direction == engine.DirectionAll || rules.Disconnect.Direction == dir) {
		return &engine.Action{Type: engine.ActionDisconnect}
	}

	// Drop rule check
	if rules.Drop != nil && (rules.Drop.Direction == engine.DirectionAll || rules.Drop.Direction == dir) {
		if rules.Drop.DropRatio > 0 {
			// Basic probabilistic evaluation (expanded in Stage 4)
			// Will be coordinated with the full statistical engine
		}
	}

	return nil
}

// propagateHalfClose safely signals EOF on the destination TCP connection without tearing down reads.
func (p *Pipe) propagateHalfClose(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
}

// optimizeTCP configures TCP_NODELAY and Keep-Alive parameters for low-latency streaming.
func (p *Pipe) optimizeTCP(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}
}

// Close terminates both client and upstream server connections safely once.
func (p *Pipe) Close() {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if p.clientConn != nil {
			_ = p.clientConn.Close()
		}
		if p.serverConn != nil {
			_ = p.serverConn.Close()
		}
	})
}

func (p *Pipe) isClosed() bool {
	return p.closed.Load()
}
