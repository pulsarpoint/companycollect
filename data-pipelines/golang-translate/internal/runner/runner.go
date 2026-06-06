package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/config"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/fixture"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/llm"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/report"
)

type llmClient interface {
	Translate(context.Context, llm.Request) (llm.Result, error)
}

func Run(ctx context.Context, cfg config.Config, input fixture.Input, client llmClient) (report.Report, report.Responses, error) {
	started := time.Now()
	runID := fmt.Sprintf("golang-translate-%s", started.UTC().Format("20060102-150405.000000000"))
	sourceLang, targetLang, err := resolveLanguages(cfg, input)
	if err != nil {
		return report.Report{}, report.Responses{}, err
	}
	items := fixture.SelectItems(input.Items, cfg.Items)
	batches := buildBatches(cfg, items)
	rep := report.Report{
		RunID:           runID,
		Scenario:        cfg.Scenario,
		Description:     cfg.Description,
		Strategy:        cfg.Strategy,
		BaseURL:         cfg.BaseURL,
		Model:           cfg.Model,
		InputPath:       cfg.InputPath,
		SourceLang:      sourceLang,
		TargetLang:      targetLang,
		ItemsPlanned:    len(items),
		BatchSize:       effectiveBatchSize(cfg, len(items)),
		Parallel:        effectiveParallel(cfg),
		RequestsPlanned: len(batches),
		TermsSent:       len(items),
		StartedAt:       started,
	}
	responses := report.Responses{RunID: runID, Scenario: cfg.Scenario}
	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	details, err := runBatches(runCtx, cfg, sourceLang, targetLang, batches, client)
	responses.Responses = details
	latencies := make([]time.Duration, 0, len(details))
	for _, detail := range details {
		if detail.Status == "succeeded" {
			rep.RequestsSucceeded++
		} else {
			rep.RequestsFailed++
		}
		rep.TermsSucceeded += detail.ItemCount - detail.MissingCount
		rep.TermsMissing += detail.MissingCount
		rep.PromptTokens += detail.PromptTokens
		rep.OutputTokens += detail.OutputTokens
		latencies = append(latencies, time.Duration(detail.LatencyMS)*time.Millisecond)
	}
	if err != nil {
		rep.FailureReason = err.Error()
	}
	rep.Finish(latencies)
	if rep.Status != "PASS" && err == nil {
		err = errors.New("translation benchmark did not complete successfully")
	}
	return rep, responses, err
}

func runBatches(
	ctx context.Context,
	cfg config.Config,
	sourceLang string,
	targetLang string,
	batches [][]fixture.Item,
	client llmClient,
) ([]report.ResponseDetail, error) {
	parallel := effectiveParallel(cfg)
	jobs := make(chan batchJob)
	results := make(chan report.ResponseDetail, len(batches))
	var wg sync.WaitGroup
	for worker := 0; worker < parallel; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- translateBatch(ctx, sourceLang, targetLang, job, client)
			}
		}()
	}
sendLoop:
	for index, batch := range batches {
		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- batchJob{ID: fmt.Sprintf("%s-batch-%03d", cfg.Scenario, index+1), Items: batch}:
		}
	}
	close(jobs)
	wg.Wait()
	close(results)

	details := make([]report.ResponseDetail, 0, len(batches))
	for detail := range results {
		details = append(details, detail)
	}
	if err := ctx.Err(); err != nil {
		return details, errors.Wrap(err, "translation benchmark timed out")
	}
	return details, nil
}

type batchJob struct {
	ID    string
	Items []fixture.Item
}

func translateBatch(ctx context.Context, sourceLang string, targetLang string, job batchJob, client llmClient) report.ResponseDetail {
	started := time.Now()
	result, err := client.Translate(ctx, llm.Request{SourceLang: sourceLang, TargetLang: targetLang, Items: job.Items})
	latency := time.Since(started)
	detail := report.ResponseDetail{
		BatchID:   job.ID,
		Status:    "succeeded",
		ItemCount: len(job.Items),
		LatencyMS: latency.Milliseconds(),
	}
	if err != nil {
		detail.Status = "failed"
		detail.MissingCount = len(job.Items)
		detail.Error = err.Error()
		return detail
	}
	detail.RawContent = result.RawContent
	detail.Translations = result.Translations
	detail.PromptTokens = result.PromptTokens
	detail.OutputTokens = result.OutputTokens
	detail.MissingCount = len(job.Items) - len(result.Translations)
	if detail.MissingCount > 0 {
		detail.Status = "missing_terms"
	}
	return detail
}

func buildBatches(cfg config.Config, items []fixture.Item) [][]fixture.Item {
	if cfg.Strategy == config.StrategySingle {
		return [][]fixture.Item{items}
	}
	return fixture.ChunkItems(items, cfg.BatchSize)
}

func effectiveBatchSize(cfg config.Config, itemCount int) int {
	if cfg.Strategy == config.StrategySingle {
		return itemCount
	}
	return cfg.BatchSize
}

func effectiveParallel(cfg config.Config) int {
	if cfg.Strategy == config.StrategyParallel {
		return cfg.Parallel
	}
	return 1
}

func resolveLanguages(cfg config.Config, input fixture.Input) (string, string, error) {
	sourceLang := cfg.SourceLang
	if sourceLang == "" {
		sourceLang = input.SourceLang
	}
	targetLang := cfg.TargetLang
	if targetLang == "" {
		targetLang = input.TargetLang
	}
	if sourceLang == "" {
		return "", "", errors.New("source language is required")
	}
	if targetLang == "" {
		return "", "", errors.New("target language is required")
	}
	return sourceLang, targetLang, nil
}
