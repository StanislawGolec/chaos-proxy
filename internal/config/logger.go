package config

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// InitLogger initializes and registers a global slog.Logger based on log level and format.
func InitLogger(levelStr, formatStr string, out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}

	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: level == slog.LevelDebug,
	}

	var handler slog.Handler
	if strings.ToLower(formatStr) == "json" {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// ContextKey is a custom type for contextual logging keys.
type ContextKey string

const (
	TunnelIDKey ContextKey = "tunnel_id"
	RemoteAddrKey ContextKey = "remote_addr"
)

// LoggerWithContext returns a logger enriched with contextual attributes from ctx.
func LoggerWithContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}

	var attrs []any
	if tid, ok := ctx.Value(TunnelIDKey).(string); ok && tid != "" {
		attrs = append(attrs, slog.String("tunnel_id", tid))
	}
	if addr, ok := ctx.Value(RemoteAddrKey).(string); ok && addr != "" {
		attrs = append(attrs, slog.String("remote_addr", addr))
	}

	if len(attrs) == 0 {
		return logger
	}
	return logger.With(attrs...)
}
