package dataplane

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
)

// DataPlane manages the transparent TCP proxy listener, tunnel lifecycle, and active connections.
type DataPlane struct {
	cfg        *config.Config
	rules      *engine.RuleContainer
	listener   net.Listener
	activeConn atomic.Int64
	totalPkts  atomic.Uint64
	totalBytes atomic.Uint64
	mu         sync.RWMutex
	tunnels    map[string]net.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewDataPlane creates an unstarted DataPlane instance.
func NewDataPlane(cfg *config.Config, rules *engine.RuleContainer) *DataPlane {
	ctx, cancel := context.WithCancel(context.Background())
	return &DataPlane{
		cfg:     cfg,
		rules:   rules,
		tunnels: make(map[string]net.Conn),
		ctx:     ctx,
		cancel:  cancel,
	}
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
