package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/config"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
)

func TestRunStartsProcessWorkflowWithConfigDefaults(t *testing.T) {
	t.Setenv("TEMPORAL_ADDRESS", "")

	configPath := writeConfig(t, `{
  "temporal": {
    "address": "temporal.test:7233",
    "namespace": "default",
    "batch_size": 25,
    "timeout_seconds": 90
  }
}`)

	factory := &fakeStarterFactory{
		starter: &fakeStarter{
			result: orchestration.WorkflowActionResult{
				WorkflowID: "translator/process",
				RunID:      "run-123",
			},
		},
	}

	var stdout bytes.Buffer
	err := run(
		context.Background(),
		[]string{"-config", configPath},
		&stdout,
		factory.newStarter,
	)
	if err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}

	if !factory.called {
		t.Fatal("run() did not create a workflow starter")
	}
	if factory.cfg.Temporal.Address != "temporal.test:7233" {
		t.Fatalf("run() Temporal address = %q, want %q", factory.cfg.Temporal.Address, "temporal.test:7233")
	}
	if factory.cfg.Temporal.Namespace != "default" {
		t.Fatalf("run() Temporal namespace = %q, want %q", factory.cfg.Temporal.Namespace, "default")
	}
	if factory.cfg.Temporal.BatchSize != 25 {
		t.Fatalf("run() Temporal batch size = %d, want %d", factory.cfg.Temporal.BatchSize, 25)
	}
	if factory.cfg.Temporal.TimeoutSeconds != 90 {
		t.Fatalf("run() Temporal timeout seconds = %d, want %d", factory.cfg.Temporal.TimeoutSeconds, 90)
	}

	if !factory.starter.called {
		t.Fatal("run() did not call StartProcess on the starter")
	}
	if !strings.Contains(stdout.String(), "workflow_id=translator/process") {
		t.Fatalf("run() stdout = %q, want workflow id", stdout.String())
	}
	if !strings.Contains(stdout.String(), "run_id=run-123") {
		t.Fatalf("run() stdout = %q, want run id", stdout.String())
	}
}

func TestRunPrintsUsageForHelp(t *testing.T) {
	var stdout bytes.Buffer

	err := run(context.Background(), []string{"-h"}, &stdout, nil)
	if err != nil {
		t.Fatalf("run(-h) error = %v, want nil", err)
	}
	if !strings.Contains(stdout.String(), "Usage of translator-trigger") {
		t.Fatalf("run(-h) stdout = %q, want usage text", stdout.String())
	}
}

func TestRunRejectsUnexpectedPositionalArguments(t *testing.T) {
	configPath := writeConfig(t, `{}`)
	factory := &fakeStarterFactory{}

	err := run(
		context.Background(),
		[]string{"-config", configPath, "unexpected"},
		&bytes.Buffer{},
		factory.newStarter,
	)
	if err == nil {
		t.Fatal("run() error = nil, want unexpected positional arguments error")
	}
	if !strings.Contains(err.Error(), "unexpected positional arguments") {
		t.Fatalf("run() error = %v, want unexpected positional arguments error", err)
	}
	if factory.called {
		t.Fatal("run() created Temporal starter before validating arguments, want no starter")
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeConfig(%q) error = %v, want nil", path, err)
	}
	return path
}

type fakeStarterFactory struct {
	called  bool
	cfg     config.Config
	starter *fakeStarter
}

func (f *fakeStarterFactory) newStarter(_ context.Context, cfg config.Config) (processStarter, func(), error) {
	f.called = true
	f.cfg = cfg
	if f.starter == nil {
		f.starter = &fakeStarter{}
	}
	return f.starter, func() {}, nil
}

type fakeStarter struct {
	called bool
	result orchestration.WorkflowActionResult
}

func (f *fakeStarter) StartProcess(_ context.Context) (orchestration.WorkflowActionResult, error) {
	f.called = true
	return f.result, nil
}
