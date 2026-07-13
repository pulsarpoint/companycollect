package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRunSupervisorCreatesWorkingDirectoryBeforeCancellation(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "nested", "axfr")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runSupervisor(ctx, []string{"--dir", directory})
	if err != nil && err != context.Canceled {
		t.Fatalf("runSupervisor() error = %v", err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", directory)
	}
}

func TestRunSupervisorRejectsUnknownFlag(t *testing.T) {
	err := runSupervisor(context.Background(), []string{"--axfr-workers", "500"})
	if err == nil {
		t.Fatal("legacy AXFR-prefixed flag unexpectedly accepted")
	}
}
