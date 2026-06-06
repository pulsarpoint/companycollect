package validate

import (
	"testing"

	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/contracts"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/fixtures"
)

func TestResultAcceptsSucceededTranslation(t *testing.T) {
	cfg, expected, result := validResultFixture(t)
	if err := Result(cfg, expected, result); err != nil {
		t.Fatalf("validate result: %v", err)
	}
}

func TestResultRejectsUnknownTermKey(t *testing.T) {
	cfg, expected, result := validResultFixture(t)
	result.Results[0].TermKey = fixtures.SHA256Hex("unknown")
	if err := Result(cfg, expected, result); err == nil {
		t.Fatalf("expected unknown term key to be rejected")
	}
}

func TestResultRejectsFailureForNormalProvider(t *testing.T) {
	cfg, expected, result := validResultFixture(t)
	result.Status = "failed"
	result.Results = nil
	result.Failures = []contracts.TranslationFailureResult{{
		TermKey:              expected.Job.Terms[0].TermKey,
		SourceText:           expected.Job.Terms[0].SourceText,
		SourceTextNormalized: expected.Job.Terms[0].SourceTextNormalized,
		Status:               "failed_retryable",
		ErrorCode:            "llm_request_failed",
	}}
	if err := Result(cfg, expected, result); err == nil {
		t.Fatalf("expected failed result to be rejected")
	}
}

func validResultFixture(t *testing.T) (config.Config, fixtures.ExpectedJob, contracts.TranslationResult) {
	t.Helper()
	cfg, err := config.Parse([]string{"--batches", "1", "--terms-per-batch", "1"})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	expected := fixtures.BuildJobs(cfg)[0]
	term := expected.Job.Terms[0]
	result := contracts.TranslationResult{
		JobID:         expected.Job.JobID,
		BatchID:       expected.Job.BatchID,
		Source:        expected.Job.Source,
		Status:        "succeeded",
		Provider:      "local",
		Model:         "qwen3:6b",
		PromptVersion: expected.Job.PromptVersion,
		CompanyIDs:    expected.Job.CompanyIDs,
		DurationMS:    10,
		Results: []contracts.TranslationResultTerm{{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
			TranslatedText:       "translated text",
			Status:               "succeeded",
		}},
	}
	return cfg, expected, result
}
