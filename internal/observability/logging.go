package observability

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const (
	WorkerTypeKey contextKey = "worker_type"
)

// InitLogger initializes the global JSON structured logger.
func InitLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch levelStr {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// WithWorkerType attaches worker_type to context.
func WithWorkerType(ctx context.Context, workerType string) context.Context {
	return context.WithValue(ctx, WorkerTypeKey, workerType)
}
