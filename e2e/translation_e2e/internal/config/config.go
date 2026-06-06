package config

import (
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	defaultNATSURL          = "nats://localhost:4222"
	defaultInputQueue       = "e2e.source.translation.jobs"
	defaultOutputQueue      = "e2e.source.translation.results"
	defaultInputStream      = "E2E_SOURCE_TRANSLATION_JOBS"
	defaultOutputStream     = "E2E_SOURCE_TRANSLATION_RESULTS"
	defaultInputConsumer    = "translation-service"
	defaultResultDurable    = "translation-e2e-results"
	defaultBatches          = 50
	defaultTermsPerBatch    = 4
	defaultMaxInputMessages = 1
	defaultTimeout          = 5 * time.Minute
	defaultProvider         = "default"
	defaultModel            = "default"
	defaultPromptVersion    = "v1"
	defaultSource           = "e2e"
	defaultAllowedProviders = "default,local,deepseek,deepseek-v4-flash"
	defaultAllowedModels    = "default,qwen3:6b,deepseek-chat,deepseek-v4-flash"
)

type Config struct {
	NATSURL          string
	InputQueue       string
	OutputQueue      string
	InputStream      string
	OutputStream     string
	InputConsumer    string
	ResultDurable    string
	MaxInputMessages uint64
	Batches          int
	TermsPerBatch    int
	Timeout          time.Duration
	Provider         string
	Model            string
	PromptVersion    string
	Source           string
	AllowedProviders []string
	AllowedModels    []string
	ReportJSONPath   string
	Purge            bool
}

func Parse(args []string) (Config, error) {
	var cfg Config
	var allowedProviders string
	var allowedModels string
	fs := flag.NewFlagSet("translation_e2e", flag.ContinueOnError)
	fs.StringVar(&cfg.NATSURL, "nats-url", defaultNATSURL, "NATS URL used by the translation service worker")
	fs.StringVar(&cfg.InputQueue, "input-queue", defaultInputQueue, "JetStream subject used for translation input jobs")
	fs.StringVar(&cfg.OutputQueue, "output-queue", defaultOutputQueue, "JetStream subject used for translation output results")
	fs.StringVar(&cfg.InputStream, "input-stream", defaultInputStream, "JetStream stream containing the input queue subject")
	fs.StringVar(&cfg.OutputStream, "output-stream", defaultOutputStream, "JetStream stream containing the output queue subject")
	fs.StringVar(&cfg.InputConsumer, "input-consumer", defaultInputConsumer, "Durable consumer name used by the translation service for input jobs")
	fs.StringVar(&cfg.ResultDurable, "result-durable", defaultResultDurable, "Durable consumer name used by this E2E app for output results")
	fs.Uint64Var(&cfg.MaxInputMessages, "max-input-messages", defaultMaxInputMessages, "Maximum live input messages before publishing one more batch")
	fs.IntVar(&cfg.Batches, "batches", defaultBatches, "Number of translation batches to prebuild and send")
	fs.IntVar(&cfg.TermsPerBatch, "terms-per-batch", defaultTermsPerBatch, "Number of translation terms in each batch")
	fs.DurationVar(&cfg.Timeout, "timeout", defaultTimeout, "Maximum end-to-end runtime")
	fs.StringVar(&cfg.Provider, "provider", defaultProvider, "Translation provider sent in each job")
	fs.StringVar(&cfg.Model, "model", defaultModel, "Translation model sent in each job")
	fs.StringVar(&cfg.PromptVersion, "prompt-version", defaultPromptVersion, "Prompt version sent in each job")
	fs.StringVar(&cfg.Source, "source", defaultSource, "Source name sent in each job")
	fs.StringVar(&allowedProviders, "allowed-providers", defaultAllowedProviders, "Comma-separated provider values accepted by this runner")
	fs.StringVar(&allowedModels, "allowed-models", defaultAllowedModels, "Comma-separated model values accepted by this runner")
	fs.StringVar(&cfg.ReportJSONPath, "report-json", "", "Optional path to write a JSON report")
	fs.BoolVar(&cfg.Purge, "purge", true, "Purge input and output streams before running")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	cfg.AllowedProviders = splitCSV(allowedProviders)
	cfg.AllowedModels = splitCSV(allowedModels)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.NATSURL) == "" {
		return fmt.Errorf("nats-url is required")
	}
	if strings.TrimSpace(c.InputQueue) == "" {
		return fmt.Errorf("input-queue is required")
	}
	if strings.TrimSpace(c.OutputQueue) == "" {
		return fmt.Errorf("output-queue is required")
	}
	if c.InputQueue == c.OutputQueue {
		return fmt.Errorf("input-queue and output-queue must be different")
	}
	if strings.TrimSpace(c.InputStream) == "" {
		return fmt.Errorf("input-stream is required")
	}
	if strings.TrimSpace(c.OutputStream) == "" {
		return fmt.Errorf("output-stream is required")
	}
	if strings.TrimSpace(c.InputConsumer) == "" {
		return fmt.Errorf("input-consumer is required")
	}
	if strings.TrimSpace(c.ResultDurable) == "" {
		return fmt.Errorf("result-durable is required")
	}
	if c.Batches <= 0 {
		return fmt.Errorf("batches must be positive")
	}
	if c.TermsPerBatch <= 0 {
		return fmt.Errorf("terms-per-batch must be positive")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if strings.TrimSpace(c.Provider) == "" {
		return fmt.Errorf("provider is required")
	}
	if strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("model is required")
	}
	if strings.EqualFold(c.Provider, "mock") {
		return fmt.Errorf("mock provider is not allowed for this E2E runner")
	}
	if strings.EqualFold(c.Model, "mock-model") {
		return fmt.Errorf("mock-model is not allowed for this E2E runner")
	}
	if !contains(c.AllowedProviders, c.Provider) {
		return fmt.Errorf("provider %q is not allowed; allowed providers: %s", c.Provider, strings.Join(c.AllowedProviders, ","))
	}
	if !contains(c.AllowedModels, c.Model) {
		return fmt.Errorf("model %q is not allowed; allowed models: %s", c.Model, strings.Join(c.AllowedModels, ","))
	}
	if strings.TrimSpace(c.PromptVersion) == "" {
		return fmt.Errorf("prompt-version is required")
	}
	if strings.TrimSpace(c.Source) == "" {
		return fmt.Errorf("source is required")
	}
	return nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func contains(values []string, value string) bool {
	return slices.Contains(values, value)
}
