package main

import (
	"bytes"
	"context"
	"testing"
)

func TestServeRefusesInvalidDefinitions(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"serve", "-out", t.TempDir(), "-definitions", t.TempDir()}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}

func TestServeStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var stdout, stderr bytes.Buffer
	go func() {
		done <- run(ctx, []string{"serve", "-out", t.TempDir(), "-definitions", "../../definitions", "-listen", "127.0.0.1:0"}, &stdout, &stderr)
	}()
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}
