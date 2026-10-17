package config

import (
	"bytes"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ListenAddr != ":8090" {
		t.Errorf("expected :8090, got %s", cfg.ListenAddr)
	}
	if cfg.TargetAddr != "localhost:8080" {
		t.Errorf("expected localhost:8080, got %s", cfg.TargetAddr)
	}
	if cfg.GRPCAddr != ":9090" {
		t.Errorf("expected :9090, got %s", cfg.GRPCAddr)
	}
	if cfg.MetricsAddr != ":9091" {
		t.Errorf("expected :9091, got %s", cfg.MetricsAddr)
	}
	if cfg.Mode != ModeL4Raw {
		t.Errorf("expected ModeL4Raw, got %s", cfg.Mode)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config failed validation: %v", err)
	}
}

func TestParseFlags(t *testing.T) {
	args := []string{
		"-listen", ":8888",
		"-target", "127.0.0.1:9999",
		"-grpc", ":7070",
		"-metrics", ":7071",
		"-mode", "l7",
		"-log-level", "debug",
		"-log-format", "json",
		"-connect-timeout", "10s",
		"-buffer-size", "65536",
	}

	cfg, err := ParseFlags(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ListenAddr != ":8888" {
		t.Errorf("expected :8888, got %s", cfg.ListenAddr)
	}
	if cfg.TargetAddr != "127.0.0.1:9999" {
		t.Errorf("expected 127.0.0.1:9999, got %s", cfg.TargetAddr)
	}
	if cfg.GRPCAddr != ":7070" {
		t.Errorf("expected :7070, got %s", cfg.GRPCAddr)
	}
	if cfg.MetricsAddr != ":7071" {
		t.Errorf("expected :7071, got %s", cfg.MetricsAddr)
	}
	if cfg.Mode != ModeL7Aware {
		t.Errorf("expected ModeL7Aware, got %s", cfg.Mode)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected debug, got %s", cfg.LogLevel)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("expected json, got %s", cfg.LogFormat)
	}
	if cfg.ConnectTimeout != 10*time.Second {
		t.Errorf("expected 10s, got %v", cfg.ConnectTimeout)
	}
	if cfg.BufferSize != 65536 {
		t.Errorf("expected 65536, got %d", cfg.BufferSize)
	}
}

func TestConfigValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{
			name: "empty listen address",
			mutate: func(c *Config) {
				c.ListenAddr = ""
			},
			wantErr: true,
		},
		{
			name: "empty target address",
			mutate: func(c *Config) {
				c.TargetAddr = "   "
			},
			wantErr: true,
		},
		{
			name: "invalid mode",
			mutate: func(c *Config) {
				c.Mode = "invalid_mode"
			},
			wantErr: true,
		},
		{
			name: "invalid buffer size",
			mutate: func(c *Config) {
				c.BufferSize = 0
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestInitLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := InitLogger("debug", "json", buf)
	if logger == nil {
		t.Fatal("logger must not be nil")
	}

	logger.Info("test message", "key", "value")
	output := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("test message")) {
		t.Errorf("expected 'test message' in output, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte("\"key\":\"value\"")) {
		t.Errorf("expected json key-value in output, got: %s", output)
	}
}
