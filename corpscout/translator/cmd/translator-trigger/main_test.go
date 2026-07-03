package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunPrintsUsageForHelp(t *testing.T) {
	var stdout bytes.Buffer

	err := run(context.Background(), []string{"-h"}, &stdout)
	if err != nil {
		t.Fatalf("run(-h) error = %v, want nil", err)
	}
	if !strings.Contains(stdout.String(), "Usage of translator-trigger") {
		t.Fatalf("run(-h) stdout = %q, want usage text", stdout.String())
	}
}

func TestRunRefusesToStart(t *testing.T) {
	var stdout bytes.Buffer

	err := run(context.Background(), nil, &stdout)
	if err == nil {
		t.Fatal("run() error = nil, want mid-migration refusal error")
	}
	if !strings.Contains(err.Error(), "mid-migration") {
		t.Fatalf("run() error = %v, want mid-migration refusal error", err)
	}
}

func TestRunRejectsUnexpectedPositionalArguments(t *testing.T) {
	var stdout bytes.Buffer

	err := run(context.Background(), []string{"unexpected"}, &stdout)
	if err == nil {
		t.Fatal("run() error = nil, want unexpected positional arguments error")
	}
	if !strings.Contains(err.Error(), "unexpected positional arguments") {
		t.Fatalf("run() error = %v, want unexpected positional arguments error", err)
	}
}
