package engine

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
)

func TestCalculateDelayUniform(t *testing.T) {
	rule := &LatencyRule{
		BaseLatency: 100 * time.Millisecond,
		Jitter:      20 * time.Millisecond,
		Probability: 1.0,
		IsGaussian:  false,
		Direction:   DirectionAll,
	}

	var sum time.Duration
	trials := 1000

	for i := 0; i < trials; i++ {
		d := CalculateDelay(rule)
		if d < 80*time.Millisecond || d > 120*time.Millisecond {
			t.Fatalf("delay %v out of uniform bounds [80ms, 120ms]", d)
		}
		sum += d
	}

	mean := sum / time.Duration(trials)
	// Mean should be close to 100ms
	diff := mean - 100*time.Millisecond
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*time.Millisecond {
		t.Errorf("expected mean ~100ms, got %v (diff: %v)", mean, diff)
	}
}

func TestCalculateDelayGaussian(t *testing.T) {
	rule := &LatencyRule{
		BaseLatency: 200 * time.Millisecond,
		Jitter:      50 * time.Millisecond,
		Probability: 1.0,
		IsGaussian:  true,
		Direction:   DirectionAll,
	}

	var sum time.Duration
	trials := 2000

	for i := 0; i < trials; i++ {
		d := CalculateDelay(rule)
		if d < 0 {
			t.Fatalf("delay cannot be negative, got %v", d)
		}
		sum += d
	}

	mean := sum / time.Duration(trials)
	diff := mean - 200*time.Millisecond
	if diff < 0 {
		diff = -diff
	}
	if diff > 10*time.Millisecond {
		t.Errorf("expected gaussian mean ~200ms, got %v", mean)
	}
}

func TestDropControllerProbabilistic(t *testing.T) {
	dc := NewDropController()
	rule := &DropRule{
		DropRatio:  0.25, // 25% drop rate
		BurstCount: 1,
		Direction:  DirectionAll,
	}

	trials := 4000
	drops := 0

	for i := 0; i < trials; i++ {
		if dc.ShouldDrop(DirectionC2S, rule) {
			drops++
		}
	}

	ratio := float64(drops) / float64(trials)
	// Expected ~0.25 (between 0.22 and 0.28 with 4,000 trials)
	if ratio < 0.21 || ratio > 0.29 {
		t.Errorf("drop ratio out of statistical tolerance: got %.3f, expected ~0.25", ratio)
	}
}

func TestDropControllerBurst(t *testing.T) {
	dc := NewDropController()
	rule := &DropRule{
		DropRatio:  1.0, // Trigger immediately
		BurstCount: 3,   // 3 consecutive drops
		Direction:  DirectionAll,
	}

	// 1st call: triggers burst and drops
	if !dc.ShouldDrop(DirectionC2S, rule) {
		t.Errorf("expected drop 1")
	}

	// Change ratio to 0 to verify burst persistence
	rule.DropRatio = 0.0

	// 2nd call: remaining burst drop 1
	if !dc.ShouldDrop(DirectionC2S, rule) {
		t.Errorf("expected burst drop 2")
	}

	// 3rd call: remaining burst drop 2
	if !dc.ShouldDrop(DirectionC2S, rule) {
		t.Errorf("expected burst drop 3")
	}

	// 4th call: burst exhausted, ratio is 0.0 -> must not drop
	if dc.ShouldDrop(DirectionC2S, rule) {
		t.Errorf("expected no drop after burst exhausted")
	}
}

func TestMutatePayloadL4(t *testing.T) {
	original := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	rule := &CorruptionRule{
		BitFlipRate: 1.0,
		Mode:        "L4_RAW",
		Direction:   DirectionAll,
	}

	mutated := MutatePayload(original, rule)
	if bytes.Equal(original, mutated) {
		t.Errorf("expected mutated payload to differ from original")
	}
	if len(mutated) != len(original) {
		t.Errorf("payload length must be preserved, got %d", len(mutated))
	}
}

func TestMutatePayloadL7(t *testing.T) {
	original := []byte("PLAY RED_7\n")
	rule := &CorruptionRule{
		BitFlipRate: 1.0,
		Mode:        "L7_SEMANTIC",
		Direction:   DirectionAll,
	}

	mutated := MutatePayload(original, rule)
	str := string(mutated)
	if bytes.Equal(original, mutated) {
		t.Errorf("expected L7 mutation to alter payload")
	}
	if !strings.Contains(str, "PURPLE") && !strings.Contains(str, "PLXX") {
		t.Errorf("expected semantic fuzzing replacement (PURPLE or PLXX), got: %s", str)
	}
}

func TestTokenBucketThrottler(t *testing.T) {
	tb := NewTokenBucket(10000) // 10,000 bytes/sec

	// Exhaust initial capacity
	_ = tb.Take(32768)

	// Next request for 10,000 bytes should require roughly 1 second wait
	wait := tb.Take(10000)
	if wait < 800*time.Millisecond || wait > 1200*time.Millisecond {
		t.Errorf("expected throttle wait ~1s, got %v", wait)
	}
}

func TestApplyTCPReset(t *testing.T) {
	// Start TCP listener to test TCPConn SetLinger(0) reset
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	done := make(chan struct{})
	go func() {
		conn, _ := l.Accept()
		if conn != nil {
			_ = conn.Close()
		}
		close(done)
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	if err := ApplyTCPReset(client); err != nil {
		t.Errorf("ApplyTCPReset failed: %v", err)
	}
	<-done
}

func TestEnginePipelineOrchestration(t *testing.T) {
	rules := NewRuleContainer()
	eng := NewEngine(rules)

	payload := []byte("JOIN Alice room_1\n")

	// 1. Transparent Pass
	act1 := eng.Process(DirectionC2S, payload)
	if act1.Type != ActionPass {
		t.Errorf("expected ActionPass, got %v", act1.Type)
	}

	// 2. Drop Rule
	rules.Store(&RuleSet{
		Drop: &DropRule{DropRatio: 1.0, Direction: DirectionC2S},
	})
	act2 := eng.Process(DirectionC2S, payload)
	if act2.Type != ActionDrop {
		t.Errorf("expected ActionDrop for C2S, got %v", act2.Type)
	}
	// Direction filter: S2C should pass
	act2S2C := eng.Process(DirectionS2C, payload)
	if act2S2C.Type != ActionPass {
		t.Errorf("expected ActionPass for S2C with C2S drop rule, got %v", act2S2C.Type)
	}

	// 3. Latency Rule
	rules.Store(&RuleSet{
		Latency: &LatencyRule{BaseLatency: 50 * time.Millisecond, Probability: 1.0, Direction: DirectionAll},
	})
	act3 := eng.Process(DirectionC2S, payload)
	if act3.Type != ActionDelay || act3.Delay < 50*time.Millisecond {
		t.Errorf("expected ActionDelay >= 50ms, got %v (%v)", act3.Type, act3.Delay)
	}

	// 4. Corruption Rule
	rules.Store(&RuleSet{
		Corruption: &CorruptionRule{BitFlipRate: 1.0, Mode: "L4_RAW", Direction: DirectionAll},
	})
	act4 := eng.Process(DirectionC2S, payload)
	if act4.Type != ActionCorrupt || act4.Mutation == nil {
		t.Errorf("expected ActionCorrupt with mutation func, got %v", act4.Type)
	}

	// 5. Disconnect Rule
	rules.Store(&RuleSet{
		Disconnect: &DisconnectRule{TCPReset: true, Immediate: true, Direction: DirectionAll},
	})
	act5 := eng.Process(DirectionC2S, payload)
	if act5.Type != ActionDisconnect {
		t.Errorf("expected ActionDisconnect, got %v", act5.Type)
	}

	// 6. Reset
	eng.Reset()
	act6 := eng.Process(DirectionC2S, payload)
	if act6.Type != ActionPass {
		t.Errorf("expected ActionPass after Reset, got %v", act6.Type)
	}
}
