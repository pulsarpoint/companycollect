package logging

import (
	"io"
	"log/slog"
	"os"
)

func ConfigureDefaultConsoleLogger(logLevel string) {
	slog.SetDefault(NewConsoleLogger(os.Stdout, logLevel))
}

func NewConsoleLogger(output io.Writer, logLevel string) *slog.Logger {
	level := slogLevel(logLevel)
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level:     level,
		AddSource: level == slog.LevelDebug,
	}))
}

func slogLevel(logLevel string) slog.Level {
	switch logLevel {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
