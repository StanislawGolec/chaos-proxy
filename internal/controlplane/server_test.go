package controlplane

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
	chaosv1 "github.com/StanislawGolec/Go-Chaos-Proxy/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestControlPlaneRPCs(t *testing.T) {
	// 1. Setup in-process gRPC server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind test listener: %v", err)
	}

	cfg := config.DefaultConfig()
	rules := engine.NewRuleContainer()
	srv := NewServer(cfg, rules, nil)
	srv.listener = listener

	go func() {
		_ = srv.grpcSrv.Serve(listener)
	}()
	defer srv.Stop()

	// 2. Connect test client
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial test server: %v", err)
	}
	defer conn.Close()

	client := chaosv1.NewChaosControlClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 3. Test SetLatency
	latResp, err := client.SetLatency(ctx, &chaosv1.LatencyConfig{
		BaseMs:       150,
		JitterMs:     30,
		Distribution: chaosv1.LatencyDistribution_NORMAL,
		Probability:  0.85,
		Direction:    "C2S",
	})
	if err != nil || !latResp.Success {
		t.Fatalf("SetLatency failed: %v, resp=%v", err, latResp)
	}

	activeRules := rules.Load()
	if activeRules.Latency == nil {
		t.Fatal("expected latency rule to be set")
	}
	if activeRules.Latency.BaseLatency != 150*time.Millisecond {
		t.Errorf("expected 150ms base, got %v", activeRules.Latency.BaseLatency)
	}
	if !activeRules.Latency.IsGaussian {
		t.Errorf("expected Gaussian distribution")
	}
	if activeRules.Latency.Direction != engine.DirectionC2S {
		t.Errorf("expected C2S direction, got %v", activeRules.Latency.Direction)
	}

	// 4. Test SetPacketDrop
	dropResp, err := client.SetPacketDrop(ctx, &chaosv1.DropConfig{
		DropRatio:  0.20,
		BurstCount: 3,
		Direction:  "S2C",
	})
	if err != nil || !dropResp.Success {
		t.Fatalf("SetPacketDrop failed: %v, resp=%v", err, dropResp)
	}
	activeRules = rules.Load()
	if activeRules.Drop == nil || activeRules.Drop.DropRatio != 0.20 || activeRules.Drop.BurstCount != 3 {
		t.Errorf("unexpected drop rule state: %+v", activeRules.Drop)
	}

	// 5. Test SetCorruption
	corrResp, err := client.SetCorruption(ctx, &chaosv1.CorruptionConfig{
		BitFlipRate: 0.05,
		Mode:        "L7_SEMANTIC",
		Direction:   "ALL",
	})
	if err != nil || !corrResp.Success {
		t.Fatalf("SetCorruption failed: %v, resp=%v", err, corrResp)
	}
	activeRules = rules.Load()
	if activeRules.Corruption == nil || activeRules.Corruption.Mode != "L7_SEMANTIC" {
		t.Errorf("unexpected corruption rule state: %+v", activeRules.Corruption)
	}

	// 6. Test SetThrottle
	throtResp, err := client.SetThrottle(ctx, &chaosv1.ThrottleConfig{
		BytesPerSecond: 64000,
		Direction:      "ALL",
	})
	if err != nil || !throtResp.Success {
		t.Fatalf("SetThrottle failed: %v, resp=%v", err, throtResp)
	}
	activeRules = rules.Load()
	if activeRules.Throttle == nil || activeRules.Throttle.BytesPerSecond != 64000 {
		t.Errorf("unexpected throttle rule state: %+v", activeRules.Throttle)
	}

	// 7. Test GetStatus
	statusResp, err := client.GetStatus(ctx, &chaosv1.Empty{})
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if statusResp.Latency == nil || statusResp.Latency.BaseMs != 150 {
		t.Errorf("status response missing correct latency config: %+v", statusResp.Latency)
	}
	if statusResp.Drop == nil || statusResp.Drop.DropRatio != 0.20 {
		t.Errorf("status response missing correct drop config: %+v", statusResp.Drop)
	}

	// 8. Test ResetAllFaults
	resetResp, err := client.ResetAllFaults(ctx, &chaosv1.Empty{})
	if err != nil || !resetResp.Success {
		t.Fatalf("ResetAllFaults failed: %v, resp=%v", err, resetResp)
	}
	activeRules = rules.Load()
	if activeRules.Latency != nil || activeRules.Drop != nil || activeRules.Corruption != nil || activeRules.Throttle != nil {
		t.Errorf("expected all rules to be cleared after reset, got %+v", activeRules)
	}
}

func TestBroadcasterFanOut(t *testing.T) {
	b := NewBroadcaster()

	// Sub 1: Filter C2S only
	ch1, cancel1 := b.Subscribe(&chaosv1.StreamFilter{Direction: "C2S"})
	defer cancel1()

	// Sub 2: Filter DROPPED action only
	ch2, cancel2 := b.Subscribe(&chaosv1.StreamFilter{Actions: []string{"DROPPED"}})
	defer cancel2()

	// Sub 3: Unfiltered
	ch3, cancel3 := b.Subscribe(nil)
	defer cancel3()

	if b.ActiveSubscribers() != 3 {
		t.Fatalf("expected 3 active subscribers, got %d", b.ActiveSubscribers())
	}

	// Event A: C2S, PASSED
	evtA := &chaosv1.PacketEvent{
		TimestampNs: time.Now().UnixNano(),
		Direction:   "C2S",
		ActionTaken: "PASSED",
		ByteCount:   100,
	}
	b.Broadcast(evtA)

	// Check Sub 1 (C2S) -> should receive
	select {
	case received := <-ch1:
		if received.Direction != "C2S" {
			t.Errorf("expected C2S event, got %v", received.Direction)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sub 1 timed out waiting for event A")
	}

	// Check Sub 2 (DROPPED) -> must NOT receive
	select {
	case unexpected := <-ch2:
		t.Fatalf("sub 2 should not have received event A: %+v", unexpected)
	default:
	}

	// Check Sub 3 (Unfiltered) -> should receive
	select {
	case received := <-ch3:
		if received.ActionTaken != "PASSED" {
			t.Errorf("expected PASSED event, got %v", received.ActionTaken)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sub 3 timed out waiting for event A")
	}

	// Event B: S2C, DROPPED
	evtB := &chaosv1.PacketEvent{
		TimestampNs: time.Now().UnixNano(),
		Direction:   "S2C",
		ActionTaken: "DROPPED",
		ByteCount:   50,
	}
	b.Broadcast(evtB)

	// Sub 1 (C2S) -> must NOT receive
	select {
	case unexpected := <-ch1:
		t.Fatalf("sub 1 should not have received event B: %+v", unexpected)
	default:
	}

	// Sub 2 (DROPPED) -> should receive
	select {
	case received := <-ch2:
		if received.ActionTaken != "DROPPED" {
			t.Errorf("expected DROPPED event, got %v", received.ActionTaken)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sub 2 timed out waiting for event B")
	}
}
