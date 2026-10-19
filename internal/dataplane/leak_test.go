package dataplane

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/StanislawGolec/go-chaos-proxy/internal/config"
	"github.com/StanislawGolec/go-chaos-proxy/internal/engine"
	"go.uber.org/goleak"
)

func TestGoroutineLeakOnConnectionTeardown(t *testing.T) {
	defer goleak.VerifyNone(t)

	// 1. Upstream echo server
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	var echoWg sync.WaitGroup
	defer func() {
		echoListener.Close()
		echoWg.Wait()
	}()

	echoWg.Add(1)
	go func() {
		defer echoWg.Done()
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			echoWg.Add(1)
			go func(c net.Conn) {
				defer echoWg.Done()
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					n, rerr := c.Read(buf)
					if n > 0 {
						_, _ = c.Write(buf[:n])
					}
					if rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	// 2. DataPlane
	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TargetAddr = echoListener.Addr().String()
	rules := engine.NewRuleContainer()
	dp := NewDataPlane(cfg, rules)

	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}

	proxyAddr := dp.Addr().String()

	// 3. Connect multiple concurrent clients, pump data, and close
	const numClients = 30
	var clientWg sync.WaitGroup
	clientWg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer clientWg.Done()
			conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			msg := []byte("PING\n")
			_, _ = conn.Write(msg)
			resp := make([]byte, len(msg))
			_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, _ = io.ReadFull(conn, resp)
		}(i)
	}

	clientWg.Wait()

	// 4. Close DataPlane and ensure graceful teardown
	if err := dp.Close(); err != nil {
		t.Fatalf("failed to close DataPlane: %v", err)
	}

	// Brief pause to allow OS socket linger and goroutine exit cleanup
	time.Sleep(100 * time.Millisecond)
}

func TestStressConnectionChurnWithFaultInjection(t *testing.T) {
	defer goleak.VerifyNone(t)

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}

	var echoWg sync.WaitGroup
	defer func() {
		echoListener.Close()
		echoWg.Wait()
	}()

	echoWg.Add(1)
	go func() {
		defer echoWg.Done()
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			echoWg.Add(1)
			go func(c net.Conn) {
				defer echoWg.Done()
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					n, rerr := c.Read(buf)
					if n > 0 {
						_, _ = c.Write(buf[:n])
					}
					if rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TargetAddr = echoListener.Addr().String()

	rules := engine.NewRuleContainer()
	// Active fault rules during churn: 10ms jitter delay and 10% drop
	rules.Store(&engine.RuleSet{
		Latency: &engine.LatencyRule{
			BaseLatency: 5 * time.Millisecond,
			Jitter:      2 * time.Millisecond,
			Probability: 0.5,
			Direction:   engine.DirectionAll,
		},
		Drop: &engine.DropRule{
			DropRatio:  0.10,
			BurstCount: 1,
			Direction:  engine.DirectionAll,
		},
	})

	dp := NewDataPlane(cfg, rules)
	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}

	proxyAddr := dp.Addr().String()

	// 50 rapid sequential and concurrent churn cycles
	const churnCycles = 50
	var churnWg sync.WaitGroup
	churnWg.Add(churnCycles)

	for i := 0; i < churnCycles; i++ {
		go func(id int) {
			defer churnWg.Done()
			conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			payload := []byte("PLAY RED_4\n")
			_, _ = conn.Write(payload)

			reply := make([]byte, len(payload))
			_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			_, _ = io.ReadFull(conn, reply)
		}(i)
	}

	churnWg.Wait()

	// Assert active connections cleanup and tear down
	if err := dp.Close(); err != nil {
		t.Fatalf("failed to close DataPlane: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}

func TestLeakOnAbruptTCPRST(t *testing.T) {
	defer goleak.VerifyNone(t)

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	var echoWg sync.WaitGroup
	echoWg.Add(1)
	go func() {
		defer echoWg.Done()
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			echoWg.Add(1)
			go func(c net.Conn) {
				defer echoWg.Done()
				defer c.Close()
				buf := make([]byte, 512)
				_, _ = c.Read(buf)
			}(conn)
		}
	}()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TargetAddr = echoListener.Addr().String()

	rules := engine.NewRuleContainer()
	// Trigger immediate hardware TCP RST disconnect
	rules.Store(&engine.RuleSet{
		Disconnect: &engine.DisconnectRule{
			TCPReset:  true,
			Immediate: true,
			Direction: engine.DirectionAll,
		},
	})

	dp := NewDataPlane(cfg, rules)
	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}

	conn, err := net.DialTimeout("tcp", dp.Addr().String(), 2*time.Second)
	if err == nil {
		_, _ = conn.Write([]byte("HELLO\n"))
		buf := make([]byte, 64)
		_, _ = conn.Read(buf)
		_ = conn.Close()
	}

	_ = dp.Close()
	_ = echoListener.Close()
	echoWg.Wait()

	time.Sleep(100 * time.Millisecond)
}

func TestTargetedVerbDropWithNoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	defer echoListener.Close()

	var echoWg sync.WaitGroup
	defer func() {
		echoListener.Close()
		echoWg.Wait()
	}()

	echoWg.Add(1)
	go func() {
		defer echoWg.Done()
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			echoWg.Add(1)
			go func(c net.Conn) {
				defer echoWg.Done()
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					n, rerr := c.Read(buf)
					if n > 0 {
						_, _ = c.Write(buf[:n])
					}
					if rerr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	cfg := config.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TargetAddr = echoListener.Addr().String()

	rules := engine.NewRuleContainer()
	rules.Store(&engine.RuleSet{
		Drop: &engine.DropRule{
			DropRatio:  1.0,
			BurstCount: 1,
			Direction:  engine.DirectionC2S,
			TargetVerb: "DEFEND",
		},
	})

	dp := NewDataPlane(cfg, rules)
	if err := dp.Start(); err != nil {
		t.Fatalf("failed to start DataPlane: %v", err)
	}

	conn, err := net.DialTimeout("tcp", dp.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}

	// Send DEFEND packet: should be dropped by proxy
	if _, err := conn.Write([]byte("DEFEND RED_2\n")); err != nil {
		t.Fatalf("failed to write DEFEND packet: %v", err)
	}
	time.Sleep(25 * time.Millisecond)

	// Send PLAY packet: should pass through proxy and echo back
	playMsg := []byte("PLAY BLUE_7\n")
	if _, err := conn.Write(playMsg); err != nil {
		t.Fatalf("failed to write PLAY packet: %v", err)
	}

	reply := make([]byte, len(playMsg))
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("failed to read echoed PLAY packet: %v", err)
	}
	if !bytes.Equal(reply, playMsg) {
		t.Fatalf("expected %q, got %q", string(playMsg), string(reply))
	}

	_ = conn.Close()
	if err := dp.Close(); err != nil {
		t.Fatalf("failed to close DataPlane: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}

func init() {
	_ = bytes.NewBuffer(nil)
}
