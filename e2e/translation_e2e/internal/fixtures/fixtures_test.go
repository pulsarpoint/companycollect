package fixtures

import (
	"regexp"
	"testing"

	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
)

func TestBuildJobsCreatesDefaultBatchSet(t *testing.T) {
	cfg, err := config.Parse(nil)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	jobs := BuildJobs(cfg)
	if len(jobs) != 50 {
		t.Fatalf("jobs len = %d, want 50", len(jobs))
	}
	if len(jobs[0].Job.Terms) != 4 {
		t.Fatalf("terms len = %d, want 4", len(jobs[0].Job.Terms))
	}
	if jobs[0].Job.SourceLang != "et" || jobs[0].Job.TargetLang != "en" {
		t.Fatalf("languages = %s/%s, want et/en", jobs[0].Job.SourceLang, jobs[0].Job.TargetLang)
	}
	if jobs[0].Job.Provider != "default" || jobs[0].Job.Model != "default" {
		t.Fatalf("provider/model = %s/%s, want default/default", jobs[0].Job.Provider, jobs[0].Job.Model)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(jobs[0].Job.Terms[0].TermKey) {
		t.Fatalf("term key is not sha256: %s", jobs[0].Job.Terms[0].TermKey)
	}
	if !regexp.MustCompile(`\b\d{9}\b`).MatchString(jobs[0].Job.Terms[0].SourceText) {
		t.Fatalf("source text does not contain 9-digit org number: %s", jobs[0].Job.Terms[0].SourceText)
	}
}
