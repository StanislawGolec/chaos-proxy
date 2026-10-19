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

	"github.com/StanislawGolec/go-chaos-proxy/internal/config"
	"github.com/StanislawGolec/go-chaos-proxy/internal/engine"
	"github.com/StanislawGolec/go-chaos-proxy/internal/metrics"
	"github.com/StanislawGolec/go-chaos-proxy/pkg/protocol"
)

// Pipe coordinates bi-directional streaming between a client connection and an upstream server connection.
type Pipe struct {
	id         string
	clientConn net.Conn
	serverConn net.Conn
	pool       *BufferPool
	rules      *engine.RuleContainer
	engine     *engine.Engine
	mode       config.Mode
	totalPkts  *atomic.Uint64
	totalBytes *atomic.Uint64
	logger     *slog.Logger
	closed     atomic.Bool
	closeOnce  sync.Once
	metrics    *metrics.Metrics
}

// NewPipe constructs a bi-directional streaming pipe.
func NewPipe(
	id string,
	clientConn net.Conn,
	serverConn net.Conn,
	pool *BufferPool,
	rules *engine.RuleContainer,
	eng *engine.Engine,
	mode config.Mode,
	totalPkts *atomic.Uint64,
	totalBytes *atomic.Uint64,
	logger *slog.Logger,
	m ...*metrics.Metrics,
) *Pipe {
	if logger == nil {
		logger = slog.Default()
	}
	if pool == nil {
		pool = NewBufferPool(32 * 1024)
	}

	p := &Pipe{
		id:         id,
		clientConn: clientConn,
		serverConn: serverConn,
		pool:       pool,
		rules:      rules,
		engine:     eng,
		mode:       mode,
		totalPkts:  totalPkts,
		totalBytes: totalBytes,
		logger:     logger.With("pipe_id", id),
	}
	if len(m) > 0 && m[0] != nil {
		p.metrics = m[0]
	}
	return p
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

// GetPtr retrieves a pooled byte slice pointer with zero heap allocations.
func (bp *BufferPool) GetPtr() *[]byte {
	bufPtr := bp.pool.Get().(*[]byte)
	*bufPtr = (*bufPtr)[:bp.size]
	return bufPtr
}

// PutPtr returns a pooled byte slice pointer directly to sync.Pool with zero heap allocations.
func (bp *BufferPool) PutPtr(bufPtr *[]byte) {
	if bufPtr == nil || cap(*bufPtr) < bp.size {
		return
	}
	*bufPtr = (*bufPtr)[:bp.size]
	bp.pool.Put(bufPtr)
}

// stream forwards bytes from src to dst in a zero-allocation loop using pooled buffers.
func (p *Pipe) stream(ctx context.Context, src, dst net.Conn, dir engine.Direction) {
	bufPtr := p.pool.GetPtr()
	defer p.pool.PutPtr(bufPtr)
	buf := *bufPtr

	dirStr := directionLabel(dir)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := src.Read(buf)
		if n > 0 {
			if p.totalPkts != nil {
				p.totalPkts.Add(1)
			}
			if p.totalBytes != nil {
				p.totalBytes.Add(uint64(n))
			}

			payload := buf[:n]

			var inspectRes *protocol.InspectionResult
			if p.mode == config.ModeL7Aware {
				inspectRes = protocol.Inspect(payload)
				p.logger.Debug("L7 packet inspected", "direction", dir, "summary", inspectRes.Summary)
			}

			// Check active fault rules if rules container is provided
			action := p.evaluateFault(dir, payload, inspectRes)
			if action != nil {
				if action.Type == engine.ActionDrop {
					if p.metrics != nil {
						p.metrics.RecordPacket(dirStr, "drop", n)
					}
					p.logger.Debug("Packet dropped by chaos rule", "direction", dir, "bytes", n)
					continue // Discard packet
				}
				if action.Type == engine.ActionDisconnect {
					if p.metrics != nil {
						p.metrics.RecordPacket(dirStr, "disconnect", n)
					}
					p.logger.Debug("Disconnecting pipe by chaos rule", "direction", dir)
					p.closeWithRST()
					return
				}
				if action.Type == engine.ActionDelay && action.Delay > 0 {
					if p.metrics != nil {
						p.metrics.RecordLatency(action.Delay)
						p.metrics.RecordPacket(dirStr, "delay", n)
					}
					select {
					case <-time.After(action.Delay):
					case <-ctx.Done():
						return
					}
				}
				if action.Type == engine.ActionCorrupt && action.Mutation != nil {
					if p.metrics != nil {
						p.metrics.RecordPacket(dirStr, "corrupt", n)
					}
					payload = action.Mutation(payload)
				}
			} else {
				if p.metrics != nil {
					p.metrics.RecordPacket(dirStr, "pass", n)
				}
			}

			// Forward data to destination socket while keeping buf intact for pool reuse
			if _, werr := dst.Write(payload); werr != nil {
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

func directionLabel(dir engine.Direction) string {
	switch dir {
	case engine.DirectionC2S:
		return "c2s"
	case engine.DirectionS2C:
		return "s2c"
	default:
		return "all"
	}
}

// evaluateFault checks active rules for a given direction and optional L7 inspection metadata.
func (p *Pipe) evaluateFault(dir engine.Direction, payload []byte, inspectRes *protocol.InspectionResult) *engine.Action {
	if p.engine == nil {
		return nil
	}
	action := p.engine.Process(dir, payload)
	if action != nil && action.Type != engine.ActionPass {
		return action
	}
	return nil
}

// closeWithRST terminates connections via hardware TCP RST if configured, else clean close.
func (p *Pipe) closeWithRST() {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		var isRST bool
		if p.rules != nil {
			rules := p.rules.Load()
			isRST = rules != nil && rules.Disconnect != nil && rules.Disconnect.TCPReset
		}

		if isRST {
			_ = engine.ApplyTCPReset(p.clientConn)
			_ = engine.ApplyTCPReset(p.serverConn)
		} else {
			if p.clientConn != nil {
				_ = p.clientConn.Close()
			}
			if p.serverConn != nil {
				_ = p.serverConn.Close()
			}
		}
	})
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
