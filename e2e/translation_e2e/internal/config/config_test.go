package config

import "testing"

func TestParseUsesDefaultTranslationProviderAndModel(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.Provider != "default" {
		t.Fatalf("provider = %q, want default", cfg.Provider)
	}
	if cfg.Model != "default" {
		t.Fatalf("model = %q, want default", cfg.Model)
	}
	if cfg.MaxInputMessages != 1 {
		t.Fatalf("max input messages = %d, want 1", cfg.MaxInputMessages)
	}
}

func TestParseRejectsMockProvider(t *testing.T) {
	if _, err := Parse([]string{"--provider", "mock"}); err == nil {
		t.Fatalf("expected mock provider to be rejected")
	}
}

func TestParseRejectsSameInputAndOutputQueue(t *testing.T) {
	if _, err := Parse([]string{"--input-queue", "same", "--output-queue", "same"}); err == nil {
		t.Fatalf("expected same input/output queue to be rejected")
	}
}

func TestParseAllowsSharedInputAndOutputStream(t *testing.T) {
	cfg, err := Parse([]string{
		"--input-stream", "SOURCE_TRANSLATION",
		"--output-stream", "SOURCE_TRANSLATION",
	})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.InputStream != cfg.OutputStream {
		t.Fatalf("expected shared stream, got input=%q output=%q", cfg.InputStream, cfg.OutputStream)
	}
}
