package dataplane

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
)

// BenchmarkBufferPoolGetPut measures single-threaded allocation reuse efficiency.
func BenchmarkBufferPoolGetPut(b *testing.B) {
	pool := NewBufferPool(32 * 1024)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		pool.Put(buf)
	}
}

// BenchmarkBufferPoolParallel measures buffer pool concurrency under parallel goroutines.
func BenchmarkBufferPoolParallel(b *testing.B) {
	pool := NewBufferPool(32 * 1024)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			pool.Put(buf)
		}
	})
}

// BenchmarkFaultEvaluationPassThrough measures the baseline CPU latency of hot-path pass-through.
func BenchmarkFaultEvaluationPassThrough(b *testing.B) {
	container := engine.NewRuleContainer()
	eng := engine.NewEngine(container)
	payload := make([]byte, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		act := eng.Process(engine.DirectionC2S, payload)
		if act.Type != engine.ActionPass {
			b.Fatal("expected pass")
		}
	}
}

// BenchmarkFaultEvaluationDropRule measures CPU cycle overhead of probabilistic drop checks.
func BenchmarkFaultEvaluationDropRule(b *testing.B) {
	container := engine.NewRuleContainer()
	container.Store(&engine.RuleSet{
		Drop: &engine.DropRule{
			DropRatio: 0.0, // 0% drop to avoid branch disruption, benchmarking calculation
			Direction: engine.DirectionAll,
		},
	})
	eng := engine.NewEngine(container)
	payload := make([]byte, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		act := eng.Process(engine.DirectionC2S, payload)
		if act.Type == engine.ActionDrop {
			b.Fatal("unexpected drop")
		}
	}
}

// BenchmarkPipeStreamingThroughput_4KB measures end-to-end socket throughput across Pipe with 4KB payloads.
func BenchmarkPipeStreamingThroughput_4KB(b *testing.B) {
	benchmarkPipeStreaming(b, 4*1024)
}

// BenchmarkPipeStreamingThroughput_32KB measures end-to-end socket throughput across Pipe with 32KB payloads.
func BenchmarkPipeStreamingThroughput_32KB(b *testing.B) {
	benchmarkPipeStreaming(b, 32*1024)
}

func benchmarkPipeStreaming(b *testing.B, chunkSize int) {
	// Set up local loopback TCP listener as upstream target
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to start target listener: %v", err)
	}
	defer targetListener.Close()

	// Upstream target discard sink
	go func() {
		for {
			conn, err := targetListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				sink := make([]byte, 64*1024)
				for {
					_, rerr := c.Read(sink)
					if rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	// Connect proxy <-> upstream server
	serverConn, err := net.Dial("tcp", targetListener.Addr().String())
	if err != nil {
		b.Fatalf("failed to dial target: %v", err)
	}
	defer serverConn.Close()

	// Create in-memory net.Pipe for client simulation
	clientProxySide, clientUserSide := net.Pipe()
	defer clientProxySide.Close()
	defer clientUserSide.Close()

	pool := NewBufferPool(32 * 1024)
	rules := engine.NewRuleContainer()
	eng := engine.NewEngine(rules)
	var pkts atomic.Uint64
	var bytesCount atomic.Uint64
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	pipe := NewPipe("bench-pipe", clientProxySide, serverConn, pool, rules, eng, config.ModeL4Raw, &pkts, &bytesCount, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = pipe.Start(ctx)
	}()

	payload := make([]byte, chunkSize)
	b.SetBytes(int64(chunkSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, werr := clientUserSide.Write(payload); werr != nil {
			b.Fatalf("write failed: %v", werr)
		}
	}

	b.StopTimer()
	cancel()
	pipe.Close()
}
