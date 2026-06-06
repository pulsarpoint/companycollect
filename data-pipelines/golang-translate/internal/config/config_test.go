package config

import "testing"

func TestParseUsesProductionLikeDefaults(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.BaseURL != defaultBaseURL {
		t.Fatalf("base URL = %q, want %q", cfg.BaseURL, defaultBaseURL)
	}
	if cfg.Model != "qwen3:6b" {
		t.Fatalf("model = %q, want qwen3:6b", cfg.Model)
	}
	if cfg.Strategy != StrategySequential {
		t.Fatalf("strategy = %q, want sequential", cfg.Strategy)
	}
}

func TestParseRejectsUnsupportedStrategy(t *testing.T) {
	if _, err := Parse([]string{"--strategy", "unknown"}); err == nil {
		t.Fatalf("expected unsupported strategy to be rejected")
	}
}

func TestParseRejectsMockModel(t *testing.T) {
	if _, err := Parse([]string{"--model", "mock-model"}); err == nil {
		t.Fatalf("expected mock model to be rejected")
	}
}
