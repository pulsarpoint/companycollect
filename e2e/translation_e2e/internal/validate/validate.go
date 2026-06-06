package validate

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/contracts"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/fixtures"
)

func Result(cfg config.Config, expected fixtures.ExpectedJob, result contracts.TranslationResult) error {
	if result.JobID != expected.Job.JobID {
		return fmt.Errorf("job_id mismatch: got %q want %q", result.JobID, expected.Job.JobID)
	}
	if result.BatchID != expected.Job.BatchID {
		return fmt.Errorf("batch_id mismatch: got %q want %q", result.BatchID, expected.Job.BatchID)
	}
	if result.Source != expected.Job.Source {
		return fmt.Errorf("source mismatch: got %q want %q", result.Source, expected.Job.Source)
	}
	if !contains(cfg.AllowedProviders, result.Provider) {
		return fmt.Errorf("provider %q is not allowed in result", result.Provider)
	}
	if result.Model != "" && !contains(cfg.AllowedModels, result.Model) {
		return fmt.Errorf("model %q is not allowed in result", result.Model)
	}
	if result.PromptVersion != expected.Job.PromptVersion {
		return fmt.Errorf("prompt_version mismatch: got %q want %q", result.PromptVersion, expected.Job.PromptVersion)
	}
	if !slices.Equal(result.CompanyIDs, expected.Job.CompanyIDs) {
		return fmt.Errorf("company_ids mismatch: got %v want %v", result.CompanyIDs, expected.Job.CompanyIDs)
	}
	if result.DurationMS < 0 {
		return fmt.Errorf("duration_ms must be non-negative")
	}
	if result.Status != "succeeded" {
		return fmt.Errorf("status = %q, want succeeded", result.Status)
	}
	if len(result.Failures) > 0 {
		return fmt.Errorf("expected no failures, got %d", len(result.Failures))
	}
	if len(result.Results) != len(expected.Job.Terms) {
		return fmt.Errorf("result term count = %d, want %d", len(result.Results), len(expected.Job.Terms))
	}
	seen := make(map[string]struct{}, len(result.Results))
	for _, item := range result.Results {
		expectedTerm, ok := expected.Terms[item.TermKey]
		if !ok {
			return fmt.Errorf("unexpected result term_key %q", item.TermKey)
		}
		if _, exists := seen[item.TermKey]; exists {
			return fmt.Errorf("duplicate result term_key %q", item.TermKey)
		}
		seen[item.TermKey] = struct{}{}
		if item.SourceText != expectedTerm.SourceText {
			return fmt.Errorf("source_text mismatch for %s", item.TermKey)
		}
		if item.SourceTextNormalized != expectedTerm.SourceTextNormalized {
			return fmt.Errorf("source_text_normalized mismatch for %s", item.TermKey)
		}
		if item.Status != "succeeded" {
			return fmt.Errorf("term %s status = %q, want succeeded", item.TermKey, item.Status)
		}
		if strings.TrimSpace(item.TranslatedText) == "" {
			return fmt.Errorf("term %s translated_text is required", item.TermKey)
		}
	}
	return nil
}

func contains(values []string, value string) bool {
	return slices.Contains(values, value)
}
