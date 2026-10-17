package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/dataplane"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
	chaosv1 "github.com/StanislawGolec/Go-Chaos-Proxy/pkg/proto/v1"
	"google.golang.org/grpc"
)

// Server implements the ChaosControl gRPC control plane service.
type Server struct {
	chaosv1.UnimplementedChaosControlServer

	cfg         *config.Config
	rules       *engine.RuleContainer
	dp          *dataplane.DataPlane
	broadcaster *Broadcaster
	grpcSrv     *grpc.Server
	listener    net.Listener
	startTime   time.Time
	mu          sync.RWMutex
	logger      *slog.Logger
	closed      bool
}

// NewServer initializes a new Server.
func NewServer(cfg *config.Config, rules *engine.RuleContainer, dp *dataplane.DataPlane) *Server {
	s := &Server{
		cfg:         cfg,
		rules:       rules,
		dp:          dp,
		broadcaster: NewBroadcaster(),
		grpcSrv:     grpc.NewServer(),
		startTime:   time.Now(),
		logger:      slog.Default().With("component", "controlplane"),
	}

	chaosv1.RegisterChaosControlServer(s.grpcSrv, s)
	return s
}

// Start binds the gRPC listener and serves incoming administrative calls.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to bind gRPC listener on %s: %w", s.cfg.GRPCAddr, err)
	}
	s.listener = l
	s.logger.Info("gRPC Control Plane listener active", "addr", s.listener.Addr().String())

	return s.grpcSrv.Serve(s.listener)
}

// Stop gracefully shuts down the gRPC server and cleans up active resources.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	s.logger.Info("Stopping gRPC control plane...")
	s.grpcSrv.GracefulStop()
	if s.listener != nil {
		_ = s.listener.Close()
	}
}

// Addr returns the listening network address or nil.
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Broadcaster returns the event telemetry pub/sub hub.
func (s *Server) Broadcaster() *Broadcaster {
	return s.broadcaster
}

// SetLatency updates the active base delay and jitter configuration.
func (s *Server) SetLatency(ctx context.Context, in *chaosv1.LatencyConfig) (*chaosv1.Response, error) {
	if in == nil {
		return &chaosv1.Response{Success: false, Message: "empty latency configuration"}, nil
	}

	dir := engine.DirectionAll
	if in.Direction != "" {
		dir = engine.Direction(in.Direction)
	}

	rule := &engine.LatencyRule{
		BaseLatency: time.Duration(in.BaseMs) * time.Millisecond,
		Jitter:      time.Duration(in.JitterMs) * time.Millisecond,
		Probability: in.Probability,
		IsGaussian:  in.Distribution == chaosv1.LatencyDistribution_NORMAL,
		Direction:   dir,
	}

	// Atomically clone current rules and update latency
	current := s.rules.Load()
	next := *current
	next.Latency = rule
	s.rules.Store(&next)

	s.logger.Info("Updated latency rule via gRPC",
		"base_ms", in.BaseMs,
		"jitter_ms", in.JitterMs,
		"gaussian", rule.IsGaussian,
		"prob", in.Probability,
		"direction", dir,
	)

	return &chaosv1.Response{
		Success: true,
		Message: fmt.Sprintf("Latency configured: base=%dms, jitter=%dms, prob=%.2f", in.BaseMs, in.JitterMs, in.Probability),
	}, nil
}

// SetPacketDrop updates the probabilistic and burst packet dropping rules.
func (s *Server) SetPacketDrop(ctx context.Context, in *chaosv1.DropConfig) (*chaosv1.Response, error) {
	if in == nil {
		return &chaosv1.Response{Success: false, Message: "empty drop configuration"}, nil
	}

	dir := engine.DirectionAll
	if in.Direction != "" {
		dir = engine.Direction(in.Direction)
	}

	rule := &engine.DropRule{
		DropRatio:  in.DropRatio,
		BurstCount: in.BurstCount,
		Direction:  dir,
	}

	current := s.rules.Load()
	next := *current
	next.Drop = rule
	s.rules.Store(&next)

	s.logger.Info("Updated packet drop rule via gRPC",
		"ratio", in.DropRatio,
		"burst", in.BurstCount,
		"direction", dir,
	)

	return &chaosv1.Response{
		Success: true,
		Message: fmt.Sprintf("Packet drop configured: ratio=%.2f, burst=%d", in.DropRatio, in.BurstCount),
	}, nil
}

// SetCorruption updates the bit flipping and payload mutation rules.
func (s *Server) SetCorruption(ctx context.Context, in *chaosv1.CorruptionConfig) (*chaosv1.Response, error) {
	if in == nil {
		return &chaosv1.Response{Success: false, Message: "empty corruption configuration"}, nil
	}

	dir := engine.DirectionAll
	if in.Direction != "" {
		dir = engine.Direction(in.Direction)
	}

	rule := &engine.CorruptionRule{
		BitFlipRate: in.BitFlipRate,
		Mode:        in.Mode,
		Direction:   dir,
	}

	current := s.rules.Load()
	next := *current
	next.Corruption = rule
	s.rules.Store(&next)

	s.logger.Info("Updated payload corruption rule via gRPC",
		"rate", in.BitFlipRate,
		"mode", in.Mode,
		"direction", dir,
	)

	return &chaosv1.Response{
		Success: true,
		Message: fmt.Sprintf("Corruption configured: rate=%.2f, mode=%s", in.BitFlipRate, in.Mode),
	}, nil
}

// SetThrottle updates bandwidth rate limiting rules.
func (s *Server) SetThrottle(ctx context.Context, in *chaosv1.ThrottleConfig) (*chaosv1.Response, error) {
	if in == nil {
		return &chaosv1.Response{Success: false, Message: "empty throttle configuration"}, nil
	}

	dir := engine.DirectionAll
	if in.Direction != "" {
		dir = engine.Direction(in.Direction)
	}

	rule := &engine.ThrottleRule{
		BytesPerSecond: in.BytesPerSecond,
		Direction:      dir,
	}

	current := s.rules.Load()
	next := *current
	next.Throttle = rule
	s.rules.Store(&next)

	s.logger.Info("Updated bandwidth throttle via gRPC",
		"bytes_per_sec", in.BytesPerSecond,
		"direction", dir,
	)

	return &chaosv1.Response{
		Success: true,
		Message: fmt.Sprintf("Throttle configured: %d bytes/sec", in.BytesPerSecond),
	}, nil
}

// TriggerDisconnect commands immediate or on-packet socket teardown.
func (s *Server) TriggerDisconnect(ctx context.Context, in *chaosv1.DisconnectCommand) (*chaosv1.Response, error) {
	if in == nil {
		in = &chaosv1.DisconnectCommand{TcpReset: true, Immediate: true}
	}

	dir := engine.DirectionAll
	if in.Direction != "" {
		dir = engine.Direction(in.Direction)
	}

	rule := &engine.DisconnectRule{
		TCPReset:  in.TcpReset,
		Immediate: in.Immediate,
		Direction: dir,
	}

	current := s.rules.Load()
	next := *current
	next.Disconnect = rule
	s.rules.Store(&next)

	s.logger.Info("Triggered disconnect command via gRPC",
		"tcp_reset", in.TcpReset,
		"immediate", in.Immediate,
		"direction", dir,
	)

	return &chaosv1.Response{
		Success: true,
		Message: fmt.Sprintf("Disconnect rule activated: tcp_reset=%v, immediate=%v", in.TcpReset, in.Immediate),
	}, nil
}

// ResetAllFaults removes all active chaos rules, restoring transparent proxying.
func (s *Server) ResetAllFaults(ctx context.Context, in *chaosv1.Empty) (*chaosv1.Response, error) {
	s.rules.Reset()
	s.logger.Info("All fault rules reset to transparent pass-through via gRPC")
	return &chaosv1.Response{
		Success: true,
		Message: "All faults cleared, proxy operating in transparent mode",
	}, nil
}

// GetStatus returns the current proxy operational metrics and active fault configuration.
func (s *Server) GetStatus(ctx context.Context, in *chaosv1.Empty) (*chaosv1.StatusResponse, error) {
	rules := s.rules.Load()

	resp := &chaosv1.StatusResponse{
		UptimeSeconds: uint64(time.Since(s.startTime).Seconds()),
	}

	if s.dp != nil {
		resp.ActiveTunnels = uint64(s.dp.ActiveConnections())
		resp.TotalPacketsProcessed = s.dp.TotalPackets()
		resp.TotalBytesTransferred = s.dp.TotalBytes()
	}

	if rules != nil {
		if rules.Latency != nil {
			dist := chaosv1.LatencyDistribution_UNIFORM
			if rules.Latency.IsGaussian {
				dist = chaosv1.LatencyDistribution_NORMAL
			}
			resp.Latency = &chaosv1.LatencyConfig{
				BaseMs:       uint32(rules.Latency.BaseLatency.Milliseconds()),
				JitterMs:     uint32(rules.Latency.Jitter.Milliseconds()),
				Distribution: dist,
				Probability:  rules.Latency.Probability,
				Direction:    string(rules.Latency.Direction),
			}
		}

		if rules.Drop != nil {
			resp.Drop = &chaosv1.DropConfig{
				DropRatio:  rules.Drop.DropRatio,
				BurstCount: rules.Drop.BurstCount,
				Direction:  string(rules.Drop.Direction),
			}
		}

		if rules.Corruption != nil {
			resp.Corruption = &chaosv1.CorruptionConfig{
				BitFlipRate: rules.Corruption.BitFlipRate,
				Mode:        rules.Corruption.Mode,
				Direction:   string(rules.Corruption.Direction),
			}
		}

		if rules.Throttle != nil {
			resp.Throttle = &chaosv1.ThrottleConfig{
				BytesPerSecond: rules.Throttle.BytesPerSecond,
				Direction:      string(rules.Throttle.Direction),
			}
		}
	}

	return resp, nil
}

// StreamEvents establishes a real-time server-streaming channel of packet telemetry.
func (s *Server) StreamEvents(in *chaosv1.StreamFilter, stream grpc.ServerStreamingServer[chaosv1.PacketEvent]) error {
	ch, cancel := s.broadcaster.Subscribe(in)
	defer cancel()

	s.logger.Debug("New telemetry streaming subscriber connected")

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("Telemetry streaming subscriber disconnected")
			return ctx.Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}
