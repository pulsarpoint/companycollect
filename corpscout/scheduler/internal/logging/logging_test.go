package logging

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestNewConsoleLoggerFiltersDebugAtInfoLevel(t *testing.T) {
	var output bytes.Buffer
	logger := NewConsoleLogger(&output, "info")

	logger.DebugContext(context.Background(), "debug message")
	logger.InfoContext(context.Background(), "info message")

	logs := output.String()
	if strings.Contains(logs, "debug message") {
		t.Fatalf("debug log should be filtered at info level: %s", logs)
	}
	if !strings.Contains(logs, "info message") {
		t.Fatalf("info log should be emitted at info level: %s", logs)
	}
}

func TestNewConsoleLoggerEmitsDebugAtDebugLevel(t *testing.T) {
	var output bytes.Buffer
	logger := NewConsoleLogger(&output, "debug")

	logger.DebugContext(context.Background(), "debug message")

	if !strings.Contains(output.String(), "debug message") {
		t.Fatalf("debug log should be emitted at debug level: %s", output.String())
	}
}
