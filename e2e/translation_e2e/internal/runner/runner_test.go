package runner

import (
	"context"
	"testing"
	"time"

	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/contracts"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/fixtures"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/jetstream"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/report"
)

func TestHandleResultMessageIgnoresUnknownJobID(t *testing.T) {
	cfg, err := config.Parse([]string{"--batches", "1"})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	rep := report.New("run-a", cfg.NATSURL, cfg.InputQueue, cfg.OutputQueue, cfg.InputStream, cfg.OutputStream, 1, time.Now())
	acked := false
	latencies := make([]time.Duration, 0)

	err = handleResultMessage(
		context.Background(),
		cfg,
		jetstream.ResultMessage{
			Result: contracts.TranslationResult{
				JobID:   "foreign-job",
				BatchID: "foreign-batch",
				Source:  "brreg",
			},
			Ack: func(context.Context) error {
				acked = true
				return nil
			},
		},
		map[string]fixtures.ExpectedJob{},
		map[string]time.Time{},
		map[string]time.Time{},
		rep,
		&latencies,
	)

	if err != nil {
		t.Fatalf("handle result: %v", err)
	}
	if !acked {
		t.Fatalf("unknown result was not acked")
	}
	if rep.ResultsIgnored != 1 {
		t.Fatalf("ignored results = %d, want 1", rep.ResultsIgnored)
	}
	if rep.BatchesReceived != 0 {
		t.Fatalf("batches received = %d, want 0", rep.BatchesReceived)
	}
	if rep.FailureReason != "" {
		t.Fatalf("failure reason = %q, want empty", rep.FailureReason)
	}
}

func TestHandleResultMessageAcceptsCurrentRunJob(t *testing.T) {
	cfg, err := config.Parse([]string{"--batches", "1", "--terms-per-batch", "2"})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	expected := fixtures.BuildJobs(cfg, "run-a")[0]
	rep := report.New("run-a", cfg.NATSURL, cfg.InputQueue, cfg.OutputQueue, cfg.InputStream, cfg.OutputStream, 1, time.Now())
	acked := false
	sentAt := map[string]time.Time{expected.Job.JobID: time.Now().Add(-time.Second)}
	receivedAt := map[string]time.Time{}
	latencies := make([]time.Duration, 0)

	err = handleResultMessage(
		context.Background(),
		cfg,
		jetstream.ResultMessage{
			Result: successfulResult(expected),
			Ack: func(context.Context) error {
				acked = true
				return nil
			},
		},
		map[string]fixtures.ExpectedJob{expected.Job.JobID: expected},
		sentAt,
		receivedAt,
		rep,
		&latencies,
	)

	if err != nil {
		t.Fatalf("handle result: %v", err)
	}
	if !acked {
		t.Fatalf("current run result was not acked")
	}
	if rep.BatchesReceived != 1 {
		t.Fatalf("batches received = %d, want 1", rep.BatchesReceived)
	}
	if rep.TermsSucceeded != 2 {
		t.Fatalf("terms succeeded = %d, want 2", rep.TermsSucceeded)
	}
	if _, ok := receivedAt[expected.Job.JobID]; !ok {
		t.Fatalf("current run job was not marked received")
	}
	if len(latencies) != 1 {
		t.Fatalf("latencies len = %d, want 1", len(latencies))
	}
}

func successfulResult(expected fixtures.ExpectedJob) contracts.TranslationResult {
	result := contracts.TranslationResult{
		JobID:         expected.Job.JobID,
		BatchID:       expected.Job.BatchID,
		Source:        expected.Job.Source,
		Status:        "succeeded",
		Provider:      expected.Job.Provider,
		Model:         expected.Job.Model,
		PromptVersion: expected.Job.PromptVersion,
		CompanyIDs:    append([]string(nil), expected.Job.CompanyIDs...),
		Results:       make([]contracts.TranslationResultTerm, 0, len(expected.Job.Terms)),
	}
	for _, term := range expected.Job.Terms {
		result.Results = append(result.Results, contracts.TranslationResultTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
			TranslatedText:       "Translated",
			Status:               "succeeded",
		})
	}
	return result
}
