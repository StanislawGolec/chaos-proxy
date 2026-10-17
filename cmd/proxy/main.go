package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/StanislawGolec/go-chaos-proxy/internal/config"
	"github.com/StanislawGolec/go-chaos-proxy/internal/controlplane"
	"github.com/StanislawGolec/go-chaos-proxy/internal/dataplane"
	"github.com/StanislawGolec/go-chaos-proxy/internal/engine"
	"github.com/StanislawGolec/go-chaos-proxy/internal/metrics"
)

func main() {
	cfg, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing configuration: %v\n", err)
		os.Exit(1)
	}

	logger := config.InitLogger(cfg.LogLevel, cfg.LogFormat, os.Stdout)
	logger.Info("Starting Go-Chaos-Proxy",
		"listen", cfg.ListenAddr,
		"target", cfg.TargetAddr,
		"grpc", cfg.GRPCAddr,
		"metrics", cfg.MetricsAddr,
		"mode", cfg.Mode,
	)

	// Initialize thread-safe atomic rule container
	rules := engine.NewRuleContainer()

	// Initialize metrics, data plane, and control plane components
	m := metrics.NewMetrics(cfg)
	dp := dataplane.NewDataPlane(cfg, rules, m)
	cp := controlplane.NewServer(cfg, rules, dp, m)

	// Graceful shutdown handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start data plane listener
	if err := dp.Start(); err != nil {
		logger.Error("Failed to start data plane listener", "error", err)
		os.Exit(1)
	}

	// Start control plane gRPC server
	go func() {
		if err := cp.Start(); err != nil {
			logger.Debug("Control plane gRPC server stopped", "error", err)
		}
	}()

	go func() {
		if err := m.Start(); err != nil {
			logger.Debug("Metrics server stopped", "error", err)
		}
	}()

	m.SetReady(true)
	m.SetHealthy(true)
	logger.Info("Proxy daemon running and ready for traffic", "mode", cfg.Mode)

	<-ctx.Done()
	logger.Info("Shutdown signal received, shutting down gracefully...")
	cp.Stop()
	_ = dp.Close()
	_ = m.Shutdown(context.Background())
	logger.Info("Go-Chaos-Proxy terminated cleanly")
}
