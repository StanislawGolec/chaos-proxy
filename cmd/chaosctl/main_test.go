package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	chaosv1 "github.com/StanislawGolec/go-chaos-proxy/pkg/proto/v1"
	"google.golang.org/grpc"
)

// mockChaosControlClient implements chaosv1.ChaosControlClient for CLI tests.
type mockChaosControlClient struct {
	latencyConfig     *chaosv1.LatencyConfig
	dropConfig        *chaosv1.DropConfig
	corruptionConfig  *chaosv1.CorruptionConfig
	throttleConfig    *chaosv1.ThrottleConfig
	disconnectCommand *chaosv1.DisconnectCommand
	resetCalled       bool
	statusResponse    *chaosv1.StatusResponse
	streamEvents      []*chaosv1.PacketEvent
}

func (m *mockChaosControlClient) SetLatency(ctx context.Context, in *chaosv1.LatencyConfig, opts ...grpc.CallOption) (*chaosv1.Response, error) {
	m.latencyConfig = in
	return &chaosv1.Response{Success: true, Message: "Latency updated"}, nil
}

func (m *mockChaosControlClient) SetPacketDrop(ctx context.Context, in *chaosv1.DropConfig, opts ...grpc.CallOption) (*chaosv1.Response, error) {
	m.dropConfig = in
	return &chaosv1.Response{Success: true, Message: "Drop updated"}, nil
}

func (m *mockChaosControlClient) SetCorruption(ctx context.Context, in *chaosv1.CorruptionConfig, opts ...grpc.CallOption) (*chaosv1.Response, error) {
	m.corruptionConfig = in
	return &chaosv1.Response{Success: true, Message: "Corruption updated"}, nil
}

func (m *mockChaosControlClient) SetThrottle(ctx context.Context, in *chaosv1.ThrottleConfig, opts ...grpc.CallOption) (*chaosv1.Response, error) {
	m.throttleConfig = in
	return &chaosv1.Response{Success: true, Message: "Throttle updated"}, nil
}

func (m *mockChaosControlClient) TriggerDisconnect(ctx context.Context, in *chaosv1.DisconnectCommand, opts ...grpc.CallOption) (*chaosv1.Response, error) {
	m.disconnectCommand = in
	return &chaosv1.Response{Success: true, Message: "Disconnect triggered"}, nil
}

func (m *mockChaosControlClient) ResetAllFaults(ctx context.Context, in *chaosv1.Empty, opts ...grpc.CallOption) (*chaosv1.Response, error) {
	m.resetCalled = true
	return &chaosv1.Response{Success: true, Message: "All faults reset"}, nil
}

func (m *mockChaosControlClient) GetStatus(ctx context.Context, in *chaosv1.Empty, opts ...grpc.CallOption) (*chaosv1.StatusResponse, error) {
	if m.statusResponse != nil {
		return m.statusResponse, nil
	}
	return &chaosv1.StatusResponse{
		UptimeSeconds:         3600,
		ActiveTunnels:         4,
		TotalPacketsProcessed: 1250,
		TotalBytesTransferred: 524288,
		Latency: &chaosv1.LatencyConfig{
			BaseMs:       100,
			JitterMs:     20,
			Distribution: chaosv1.LatencyDistribution_NORMAL,
			Probability:  0.8,
			Direction:    "C2S",
		},
		Drop: &chaosv1.DropConfig{
			DropRatio:  0.15,
			BurstCount: 2,
			Direction:  "S2C",
		},
	}, nil
}

type mockStreamClient struct {
	grpc.ClientStream
	events []*chaosv1.PacketEvent
	index  int
}

func (s *mockStreamClient) Recv() (*chaosv1.PacketEvent, error) {
	if s.index >= len(s.events) {
		return nil, io.EOF
	}
	ev := s.events[s.index]
	s.index++
	return ev, nil
}

func (m *mockChaosControlClient) StreamEvents(ctx context.Context, in *chaosv1.StreamFilter, opts ...grpc.CallOption) (grpc.ServerStreamingClient[chaosv1.PacketEvent], error) {
	return &mockStreamClient{events: m.streamEvents}, nil
}

// --- CLI Tests ---

func TestCLI_LatencyCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"latency", "--base=150ms", "--jitter=25ms", "--dist=gaussian", "--prob=0.9", "--dir=C2S"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.latencyConfig == nil {
		t.Fatal("expected latency config to be set")
	}
	if mock.latencyConfig.BaseMs != 150 {
		t.Errorf("expected base 150, got %d", mock.latencyConfig.BaseMs)
	}
	if mock.latencyConfig.JitterMs != 25 {
		t.Errorf("expected jitter 25, got %d", mock.latencyConfig.JitterMs)
	}
	if mock.latencyConfig.Distribution != chaosv1.LatencyDistribution_NORMAL {
		t.Errorf("expected normal distribution, got %v", mock.latencyConfig.Distribution)
	}
	if mock.latencyConfig.Probability != 0.9 {
		t.Errorf("expected probability 0.9, got %f", mock.latencyConfig.Probability)
	}
	if mock.latencyConfig.Direction != "C2S" {
		t.Errorf("expected direction C2S, got %s", mock.latencyConfig.Direction)
	}

	if !strings.Contains(stdout.String(), "[OK]") {
		t.Errorf("expected [OK] in output, got: %s", stdout.String())
	}
}

func TestCLI_DropCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"drop", "--rate=0.25", "--burst=3", "--dir=S2C"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.dropConfig == nil {
		t.Fatal("expected drop config to be set")
	}
	if mock.dropConfig.DropRatio != 0.25 {
		t.Errorf("expected drop ratio 0.25, got %f", mock.dropConfig.DropRatio)
	}
	if mock.dropConfig.BurstCount != 3 {
		t.Errorf("expected burst 3, got %d", mock.dropConfig.BurstCount)
	}
	if mock.dropConfig.Direction != "S2C" {
		t.Errorf("expected direction S2C, got %s", mock.dropConfig.Direction)
	}
}

func TestCLI_CorruptCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"corrupt", "--rate=0.10", "--mode=l7", "--dir=ALL"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.corruptionConfig == nil {
		t.Fatal("expected corruption config to be set")
	}
	if mock.corruptionConfig.BitFlipRate != 0.10 {
		t.Errorf("expected rate 0.10, got %f", mock.corruptionConfig.BitFlipRate)
	}
	if mock.corruptionConfig.Mode != "L7_SEMANTIC" {
		t.Errorf("expected mode L7_SEMANTIC, got %s", mock.corruptionConfig.Mode)
	}
}

func TestCLI_ThrottleCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"throttle", "--rate=64kbps", "--dir=ALL"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.throttleConfig == nil {
		t.Fatal("expected throttle config to be set")
	}
	// 64 * 1000 / 8 = 8000
	if mock.throttleConfig.BytesPerSecond != 8000 {
		t.Errorf("expected 8000 B/s, got %d", mock.throttleConfig.BytesPerSecond)
	}
}

func TestCLI_DisconnectCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"disconnect", "--immediate=true", "--dir=C2S"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.disconnectCommand == nil {
		t.Fatal("expected disconnect command to be set")
	}
	if !mock.disconnectCommand.Immediate {
		t.Errorf("expected immediate=true")
	}
	if mock.disconnectCommand.Direction != "C2S" {
		t.Errorf("expected direction C2S, got %s", mock.disconnectCommand.Direction)
	}
}

func TestCLI_ResetCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"reset"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !mock.resetCalled {
		t.Errorf("expected reset to be called")
	}
}

func TestCLI_StatusCommand_Text(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"status", "--no-color"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "Active Tunnels:        4") {
		t.Errorf("missing active tunnels in output: %s", out)
	}
	if !strings.Contains(out, "Total Packets:         1250") {
		t.Errorf("missing total packets in output: %s", out)
	}
	if !strings.Contains(out, "Base=100ms") {
		t.Errorf("missing latency info in output: %s", out)
	}
}

func TestCLI_StatusCommand_JSON(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"status", "--json"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var status chaosv1.StatusResponse
	if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
		t.Fatalf("failed to decode JSON status: %v, raw output: %s", err, stdout.String())
	}

	if status.ActiveTunnels != 4 {
		t.Errorf("expected 4 active tunnels, got %d", status.ActiveTunnels)
	}
}

func TestCLI_TailCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{
		streamEvents: []*chaosv1.PacketEvent{
			{
				TimestampNs: time.Now().UnixNano(),
				TunnelId:    "tunnel-1",
				Direction:   "C2S",
				ByteCount:   64,
				ActionTaken: "PASSED",
				Summary:     "Ultimo: PLAY RED_7",
			},
			{
				TimestampNs:    time.Now().UnixNano(),
				TunnelId:       "tunnel-1",
				Direction:      "S2C",
				ByteCount:      128,
				ActionTaken:    "DELAYED",
				DelayAppliedMs: 150,
				Summary:        "Ultimo: TOP_CARD RED_7",
			},
			{
				TimestampNs: time.Now().UnixNano(),
				TunnelId:    "tunnel-2",
				Direction:   "C2S",
				ByteCount:   32,
				ActionTaken: "DROPPED",
				Summary:     "RFC 6455 frame dropped",
			},
		},
	}
	cli.SetClient(mock)

	args := []string{"tail", "--no-color"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "[PASS]") {
		t.Errorf("expected [PASS] in output: %s", out)
	}
	if !strings.Contains(out, "[DELAY]") {
		t.Errorf("expected [DELAY] in output: %s", out)
	}
	if !strings.Contains(out, "[DROP]") {
		t.Errorf("expected [DROP] in output: %s", out)
	}
	if !strings.Contains(out, "Ultimo: PLAY RED_7") {
		t.Errorf("expected summary details in output: %s", out)
	}
}

func TestParseBandwidthBytes(t *testing.T) {
	cases := []struct {
		input    string
		expected uint64
	}{
		{"64kbps", 8000},
		{"1mbps", 125000},
		{"100kb", 102400},
		{"1mb", 1048576},
		{"64000", 64000},
	}

	for _, c := range cases {
		val, err := parseBandwidthBytes(c.input)
		if err != nil {
			t.Errorf("input %s gave error: %v", c.input, err)
			continue
		}
		if val != c.expected {
			t.Errorf("input %s expected %d, got %d", c.input, c.expected, val)
		}
	}
}

func TestCLI_HelpCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)

	if err := cli.Run(context.Background(), []string{"--help"}); err != nil {
		t.Fatalf("unexpected error on --help: %v", err)
	}
	if !strings.Contains(stderr.String(), "chaosctl - Dynamic L4/L7 Fault Injection Operator CLI") {
		t.Errorf("expected usage output on --help, got: %s", stderr.String())
	}
}

func TestCLI_SubcommandHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)

	if err := cli.Run(context.Background(), []string{"latency", "--help"}); err != nil {
		t.Fatalf("unexpected error on latency --help: %v", err)
	}
	if !strings.Contains(stderr.String(), "Usage of latency:") {
		t.Errorf("expected usage output on latency --help, got: %s", stderr.String())
	}
}

func TestCLI_DropCommand_VerbFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"drop", "--verb=defend", "--rate=1.0", "--burst=1", "--dir=C2S"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.dropConfig == nil {
		t.Fatal("expected drop config to be set")
	}
	if mock.dropConfig.DropRatio != 1.0 {
		t.Errorf("expected drop ratio 1.0, got %f", mock.dropConfig.DropRatio)
	}
	if mock.dropConfig.Direction != "C2S" {
		t.Errorf("expected direction C2S, got %s", mock.dropConfig.Direction)
	}
	if cli.targetDropVerb != "DEFEND" {
		t.Errorf("expected targetDropVerb DEFEND, got %s", cli.targetDropVerb)
	}
	if v := getDropTargetVerb(mock.dropConfig); v != "" && v != "DEFEND" {
		t.Errorf("expected proto target verb DEFEND, got %s", v)
	}
	out := stdout.String()
	if !strings.Contains(out, "[OK]") || !strings.Contains(out, "verb=DEFEND") {
		t.Errorf("expected [OK] and verb=DEFEND in output, got: %s", out)
	}
}

func TestCLI_DropCommand_VerbFlag_JSON(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"drop", "--verb=DEFEND", "--rate=1.0", "--json"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("failed to unmarshal JSON output: %v, raw: %s", err, stdout.String())
	}
	if success, ok := payload["success"].(bool); !ok || !success {
		t.Errorf("expected success=true, got: %v", payload["success"])
	}
	meta, ok := payload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map in JSON output, got: %v", payload["metadata"])
	}
	if meta["verb"] != "DEFEND" {
		t.Errorf("expected metadata.verb=DEFEND, got: %v", meta["verb"])
	}
	if meta["drop_ratio"] != 1.0 {
		t.Errorf("expected metadata.drop_ratio=1.0, got: %v", meta["drop_ratio"])
	}
}

func TestCLI_DisconnectCommand_GraceFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"disconnect", "--grace=120s", "--immediate=true", "--dir=ALL"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.disconnectCommand == nil {
		t.Fatal("expected disconnect command to be set")
	}
	if !mock.disconnectCommand.Immediate {
		t.Errorf("expected immediate=true")
	}
	if cli.disconnectGrace != 120*time.Second {
		t.Errorf("expected disconnectGrace=120s, got %v", cli.disconnectGrace)
	}
	out := stdout.String()
	if !strings.Contains(out, "[OK]") || !strings.Contains(out, "grace=120s") {
		t.Errorf("expected [OK] and grace=120s in output, got: %s", out)
	}

	// Verify invalid grace duration returns an error
	if err := cli.Run(context.Background(), []string{"disconnect", "--grace=invalid"}); err == nil {
		t.Errorf("expected error for invalid --grace flag")
	}
}

func TestCLI_DisconnectCommand_GraceFlag_JSON(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cli := NewCLI(&stdout, &stderr)
	mock := &mockChaosControlClient{}
	cli.SetClient(mock)

	args := []string{"disconnect", "--grace=120s", "--json"}
	if err := cli.Run(context.Background(), args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("failed to unmarshal JSON output: %v, raw: %s", err, stdout.String())
	}
	meta, ok := payload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map in JSON output, got: %v", payload["metadata"])
	}
	if meta["grace_period"] != "120s" {
		t.Errorf("expected metadata.grace_period=120s, got: %v", meta["grace_period"])
	}
	if meta["grace_seconds"] != float64(120) {
		t.Errorf("expected metadata.grace_seconds=120, got: %v", meta["grace_seconds"])
	}
}

func TestCLI_ScenarioCommand(t *testing.T) {
	t.Run("defense-drop", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cli := NewCLI(&stdout, &stderr)
		mock := &mockChaosControlClient{}
		cli.SetClient(mock)

		if err := cli.Run(context.Background(), []string{"scenario", "defense-drop"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.dropConfig == nil || mock.dropConfig.DropRatio != 1.0 {
			t.Fatalf("expected drop ratio 1.0, got %+v", mock.dropConfig)
		}
		if cli.targetDropVerb != "DEFEND" {
			t.Errorf("expected targetDropVerb DEFEND, got %s", cli.targetDropVerb)
		}
	})

	t.Run("reconnect-grace", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cli := NewCLI(&stdout, &stderr)
		mock := &mockChaosControlClient{}
		cli.SetClient(mock)

		if err := cli.Run(context.Background(), []string{"scenario", "reconnect-grace"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.disconnectCommand == nil || !mock.disconnectCommand.Immediate {
			t.Fatalf("expected immediate disconnect command, got %+v", mock.disconnectCommand)
		}
		if cli.disconnectGrace != 120*time.Second {
			t.Errorf("expected disconnectGrace 120s, got %v", cli.disconnectGrace)
		}
	})

	t.Run("unknown-scenario", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cli := NewCLI(&stdout, &stderr)
		mock := &mockChaosControlClient{}
		cli.SetClient(mock)

		if err := cli.Run(context.Background(), []string{"scenario", "nonexistent"}); err == nil {
			t.Fatal("expected error for unknown scenario")
		}
	})
}

func TestCLI_OutputFormatting_TabularANSIAndJSON(t *testing.T) {
	// 1. Tabular ANSI mode (color enabled)
	var ansiOut, ansiErr bytes.Buffer
	cliANSI := NewCLI(&ansiOut, &ansiErr)
	mock := &mockChaosControlClient{}
	cliANSI.SetClient(mock)

	if err := cliANSI.Run(context.Background(), []string{"drop", "--verb=DEFEND", "--rate=1.0"}); err != nil {
		t.Fatalf("unexpected error in drop: %v", err)
	}
	ansiDropOut := ansiOut.String()
	if !strings.Contains(ansiDropOut, colorGreen) || !strings.Contains(ansiDropOut, "[OK]") {
		t.Errorf("expected ANSI green [OK] in output, got: %q", ansiDropOut)
	}

	ansiOut.Reset()
	if err := cliANSI.Run(context.Background(), []string{"status"}); err != nil {
		t.Fatalf("unexpected error in status: %v", err)
	}
	statusANSI := ansiOut.String()
	if !strings.Contains(statusANSI, colorBold) || !strings.Contains(statusANSI, "=== Go-Chaos-Proxy Operational Status ===") {
		t.Errorf("expected bold ANSI header in tabular status output, got: %q", statusANSI)
	}
	if !strings.Contains(statusANSI, colorRed) || !strings.Contains(statusANSI, "Verb=DEFEND") {
		t.Errorf("expected red ANSI packet drop row with Verb=DEFEND in tabular status output, got: %q", statusANSI)
	}

	// 2. JSON mode
	var jsonOut, jsonErr bytes.Buffer
	cliJSON := NewCLI(&jsonOut, &jsonErr)
	cliJSON.SetClient(mock)

	if err := cliJSON.Run(context.Background(), []string{"--json", "status"}); err != nil {
		t.Fatalf("unexpected error in json status: %v", err)
	}
	var statusResp chaosv1.StatusResponse
	if err := json.Unmarshal(jsonOut.Bytes(), &statusResp); err != nil {
		t.Fatalf("expected valid JSON status output, got error: %v, raw: %s", err, jsonOut.String())
	}
	if statusResp.ActiveTunnels != 4 {
		t.Errorf("expected 4 active tunnels in JSON status, got %d", statusResp.ActiveTunnels)
	}
}


