package fixtures

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/contracts"
)

const (
	sourceLang = "et"
	targetLang = "en"
)

type ExpectedJob struct {
	Job   contracts.TranslationJob
	Terms map[string]contracts.TranslationJobTerm
}

func BuildJobs(cfg config.Config, runID string) []ExpectedJob {
	runPrefix := strings.TrimSpace(runID)
	if runPrefix == "" {
		runPrefix = "translation-e2e"
	}
	jobs := make([]ExpectedJob, 0, cfg.Batches)
	for batchIndex := 0; batchIndex < cfg.Batches; batchIndex++ {
		companyID := fmt.Sprintf("%s-company-%03d", runPrefix, batchIndex+1)
		orgNumber := fmt.Sprintf("81%07d", batchIndex+1)
		job := contracts.TranslationJob{
			JobID:         fmt.Sprintf("%s-job-%03d", runPrefix, batchIndex+1),
			BatchID:       fmt.Sprintf("%s-batch-%03d", runPrefix, batchIndex+1),
			Source:        cfg.Source,
			SourceLang:    sourceLang,
			TargetLang:    targetLang,
			Provider:      cfg.Provider,
			Model:         cfg.Model,
			PromptVersion: cfg.PromptVersion,
			CompanyIDs:    []string{companyID},
			Terms:         make([]contracts.TranslationJobTerm, 0, cfg.TermsPerBatch),
		}
		expectedTerms := make(map[string]contracts.TranslationJobTerm, cfg.TermsPerBatch)
		for termIndex := 0; termIndex < cfg.TermsPerBatch; termIndex++ {
			sourceText := sourceTextFor(batchIndex, termIndex, orgNumber)
			normalized := strings.ToLower(sourceText)
			term := contracts.TranslationJobTerm{
				TermKey:              SHA256Hex(fmt.Sprintf("%s:%s", job.BatchID, normalized)),
				SourceText:           sourceText,
				SourceTextNormalized: normalized,
			}
			job.Terms = append(job.Terms, term)
			expectedTerms[term.TermKey] = term
		}
		jobs = append(jobs, ExpectedJob{Job: job, Terms: expectedTerms})
	}
	return jobs
}

func SHA256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func sourceTextFor(batchIndex int, termIndex int, orgNumber string) string {
	values := []string{
		"Osaühing",
		"Registrisse kantud äriühing",
		"Põhitegevus äriregistri andmetel",
		"Juriidiline aadress ja registrikood",
	}
	base := values[termIndex%len(values)]
	return fmt.Sprintf("%s %03d.%02d registrikood %s", base, batchIndex+1, termIndex+1, orgNumber)
}
