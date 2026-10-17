package controlplane

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/dataplane"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
	"google.golang.org/grpc"
)

// Server implements the Control Plane server, managing gRPC service life cycle and broadcasting events.
type Server struct {
	cfg       *config.Config
	rules     *engine.RuleContainer
	dp        *dataplane.DataPlane
	grpcSrv   *grpc.Server
	listener  net.Listener
	startTime time.Time
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewServer initializes a new control plane server.
func NewServer(cfg *config.Config, rules *engine.RuleContainer, dp *dataplane.DataPlane) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:       cfg,
		rules:     rules,
		dp:        dp,
		startTime: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Uptime returns elapsed time since control plane startup.
func (s *Server) Uptime() time.Duration {
	return time.Since(s.startTime)
}
