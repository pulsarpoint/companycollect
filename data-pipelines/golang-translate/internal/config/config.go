package config

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

const (
	StrategySingle     = "single"
	StrategySequential = "sequential"
	StrategyParallel   = "parallel"
)

const (
	defaultInputPath      = "input.json"
	defaultBaseURL        = "http://100.77.62.33:8888"
	defaultModel          = "qwen3:6b"
	defaultStrategy       = StrategySequential
	defaultItems          = 32
	defaultBatchSize      = 4
	defaultParallel       = 1
	defaultTimeout        = 5 * time.Minute
	defaultRequestTimeout = 120 * time.Second
	defaultScenario       = "manual"
)

type Config struct {
	InputPath      string
	BaseURL        string
	APIKey         string
	Model          string
	SourceLang     string
	TargetLang     string
	Strategy       string
	Items          int
	BatchSize      int
	Parallel       int
	Timeout        time.Duration
	RequestTimeout time.Duration
	Scenario       string
	Description    string
	ReportJSON     string
	ResponsesJSON  string
}

func Parse(args []string) (Config, error) {
	cfg := Config{APIKey: defaultAPIKey()}
	fs := flag.NewFlagSet("golang-translate", flag.ContinueOnError)
	fs.StringVar(&cfg.InputPath, "input", defaultInputPath, "Translation fixture JSON path")
	fs.StringVar(&cfg.BaseURL, "base-url", defaultBaseURL, "OpenAI-compatible base URL")
	fs.StringVar(&cfg.APIKey, "api-key", cfg.APIKey, "Optional OpenAI-compatible API key")
	fs.StringVar(&cfg.Model, "model", defaultModel, "LLM model name")
	fs.StringVar(&cfg.SourceLang, "source-lang", "", "Source language override; defaults to input.json source_lang")
	fs.StringVar(&cfg.TargetLang, "target-lang", "", "Target language override; defaults to input.json target_lang")
	fs.StringVar(&cfg.Strategy, "strategy", defaultStrategy, "Request strategy: single, sequential, or parallel")
	fs.IntVar(&cfg.Items, "items", defaultItems, "Number of fixture items to translate; 0 means all")
	fs.IntVar(&cfg.BatchSize, "batch-size", defaultBatchSize, "Items per request for sequential/parallel strategies")
	fs.IntVar(&cfg.Parallel, "parallel", defaultParallel, "Maximum concurrent requests for parallel strategy")
	fs.DurationVar(&cfg.Timeout, "timeout", defaultTimeout, "Overall scenario timeout")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", defaultRequestTimeout, "HTTP timeout per LLM request")
	fs.StringVar(&cfg.Scenario, "scenario", defaultScenario, "Scenario name written into reports")
	fs.StringVar(&cfg.Description, "description", "", "Human-readable scenario description")
	fs.StringVar(&cfg.ReportJSON, "report-json", "", "Optional JSON report output path")
	fs.StringVar(&cfg.ResponsesJSON, "responses-json", "", "Optional JSON response detail output path")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.InputPath) == "" {
		return fmt.Errorf("input is required")
	}
	if strings.TrimSpace(c.BaseURL) == "" {
		return fmt.Errorf("base-url is required")
	}
	if strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("model is required")
	}
	if strings.EqualFold(c.Model, "mock") || strings.EqualFold(c.Model, "mock-model") {
		return fmt.Errorf("mock models are not allowed")
	}
	if !slices.Contains([]string{StrategySingle, StrategySequential, StrategyParallel}, c.Strategy) {
		return fmt.Errorf("strategy %q is not supported", c.Strategy)
	}
	if c.Items < 0 {
		return fmt.Errorf("items must be zero or positive")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be positive")
	}
	if c.Parallel <= 0 {
		return fmt.Errorf("parallel must be positive")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request-timeout must be positive")
	}
	if strings.TrimSpace(c.Scenario) == "" {
		return fmt.Errorf("scenario is required")
	}
	return nil
}

func defaultAPIKey() string {
	for _, key := range []string{
		"TRANSLATION_PROVIDER_LOCAL_API_KEY",
		"TRANSLATION_LLM_API_KEY",
		"LLM_API_KEY",
		"OPENAI_API_KEY",
	} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}
