package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/config"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/controlplane"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/dataplane"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/engine"
	"github.com/StanislawGolec/Go-Chaos-Proxy/internal/metrics"
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

	// Initialize data plane, control plane, and metrics components
	dp := dataplane.NewDataPlane(cfg, rules)
	cp := controlplane.NewServer(cfg, rules, dp)
	m := metrics.NewMetrics(cfg)

	// Graceful shutdown handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start data plane listener
	if err := dp.Start(); err != nil {
		logger.Error("Failed to start data plane listener", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := m.Start(); err != nil {
			logger.Debug("Metrics server stopped", "error", err)
		}
	}()

	logger.Info("Proxy daemon running and ready for traffic", "mode", cfg.Mode)

	<-ctx.Done()
	logger.Info("Shutdown signal received, shutting down gracefully...")
	_ = cp
	_ = dp.Close()
	_ = m.Shutdown(context.Background())
	logger.Info("Go-Chaos-Proxy terminated cleanly")
}
