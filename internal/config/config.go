package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Mode defines the packet inspection mode.
type Mode string

const (
	ModeL4Raw   Mode = "l4"
	ModeL7Aware Mode = "l7"
)

// Config holds configuration parameters for the Go-Chaos-Proxy server.
type Config struct {
	// ListenAddr is the network address the proxy listens on for client connections (e.g. :8090).
	ListenAddr string
	// TargetAddr is the upstream destination server address (e.g. localhost:8080).
	TargetAddr string
	// GRPCAddr is the control plane gRPC service address (e.g. :9090).
	GRPCAddr string
	// MetricsAddr is the Prometheus metrics and health check HTTP address (e.g. :9091).
	MetricsAddr string
	// Mode defines whether the proxy operates as transparent L4 or L7 message-aware.
	Mode Mode
	// LogLevel is the minimum logging severity: debug, info, warn, error.
	LogLevel string
	// LogFormat specifies log output format: text or json.
	LogFormat string
	// ConnectTimeout is the timeout duration for establishing upstream connection.
	ConnectTimeout time.Duration
	// ReadTimeout is the socket idle read timeout (0 for no timeout).
	ReadTimeout time.Duration
	// BufferSize is the preallocated byte buffer size for streaming (e.g. 32768).
	BufferSize int
}

// DefaultConfig returns a Config initialized with sensible production defaults.
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:     ":8090",
		TargetAddr:     "localhost:8080",
		GRPCAddr:       ":9090",
		MetricsAddr:    ":9091",
		Mode:           ModeL4Raw,
		LogLevel:       "info",
		LogFormat:      "text",
		ConnectTimeout: 5 * time.Second,
		ReadTimeout:    0,
		BufferSize:     32 * 1024, // 32 KB
	}
}

// ParseFlags parses command line arguments and populates Config.
func ParseFlags(args []string) (*Config, error) {
	cfg := DefaultConfig()

	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)

	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "Address for proxy to listen on (e.g. :8090)")
	fs.StringVar(&cfg.TargetAddr, "target", cfg.TargetAddr, "Upstream target server address (e.g. localhost:8080)")
	fs.StringVar(&cfg.GRPCAddr, "grpc", cfg.GRPCAddr, "Control plane gRPC listen address (e.g. :9090)")
	fs.StringVar(&cfg.MetricsAddr, "metrics", cfg.MetricsAddr, "Prometheus metrics HTTP address (e.g. :9091)")
	
	modeStr := string(cfg.Mode)
	fs.StringVar(&modeStr, "mode", modeStr, "Inspection mode: 'l4' (raw transparent TCP) or 'l7' (WebSocket/Ultimo aware)")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: text, json")
	fs.DurationVar(&cfg.ConnectTimeout, "connect-timeout", cfg.ConnectTimeout, "Upstream connection timeout")
	fs.IntVar(&cfg.BufferSize, "buffer-size", cfg.BufferSize, "Buffer size in bytes for data plane pooling")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg.Mode = Mode(strings.ToLower(modeStr))
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks configuration sanity.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.ListenAddr) == "" {
		return errors.New("listen address must not be empty")
	}
	if strings.TrimSpace(c.TargetAddr) == "" {
		return errors.New("target address must not be empty")
	}
	if strings.TrimSpace(c.GRPCAddr) == "" {
		return errors.New("grpc address must not be empty")
	}
	if strings.TrimSpace(c.MetricsAddr) == "" {
		return errors.New("metrics address must not be empty")
	}
	if c.Mode != ModeL4Raw && c.Mode != ModeL7Aware {
		return fmt.Errorf("invalid mode '%s', expected 'l4' or 'l7'", c.Mode)
	}
	if c.BufferSize <= 0 {
		return errors.New("buffer size must be greater than zero")
	}
	return nil
}

// LookupEnvOr returns environment variable value or fallback default.
func LookupEnvOr(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}
