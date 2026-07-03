package engine

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

func TestRuntimeLoadsProcessesAndUploadsBRREGQueue(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(10)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	loadResult, err := runtime.LoadNewInput(ctx)
	if err != nil {
		t.Fatalf("load new input: %v", err)
	}
	if loadResult.RowsInserted != 10 {
		t.Fatalf("expected 10 inserted input rows, got %d", loadResult.RowsInserted)
	}

	totalTranslated := 0
	expectedPending := []int{7, 4, 1, 0}
	expectedOutput := []int{3, 6, 9, 10}
	for batch := range expectedPending {
		processResult, err := runtime.ProcessOneBatch(ctx, ProcessInput{
			BatchSize:      3,
			TimeoutSeconds: 30,
		})
		if err != nil {
			t.Fatalf("process one batch: %v", err)
		}
		if processResult.PendingCount != expectedPending[batch] {
			t.Fatalf("batch %d pending count = %d, want %d", batch, processResult.PendingCount, expectedPending[batch])
		}
		if processResult.OutputCount != expectedOutput[batch] {
			t.Fatalf("batch %d output count = %d, want %d", batch, processResult.OutputCount, expectedOutput[batch])
		}
		totalTranslated += processResult.TranslatedCount
	}
	if totalTranslated != 10 {
		t.Fatalf("expected 10 translated rows, got %d", totalTranslated)
	}

	uploadResult, err := runtime.UploadOutput(ctx)
	if err != nil {
		t.Fatalf("upload output: %v", err)
	}
	if uploadResult.RowsSeen != 10 {
		t.Fatalf("expected 10 output rows seen, got %d", uploadResult.RowsSeen)
	}
	if uploadResult.RowsInserted != 10 {
		t.Fatalf("expected 10 output rows inserted, got %d", uploadResult.RowsInserted)
	}
	if len(source.insertedTranslations) != 10 {
		t.Fatalf("expected 10 ClickHouse insert rows, got %d", len(source.insertedTranslations))
	}

	inserted := source.insertedTranslations[0]
	if inserted.Provider != "local" || inserted.Model != "qwen3:6b" {
		t.Fatalf("expected local/qwen3:6b provider metadata, got %s/%s", inserted.Provider, inserted.Model)
	}
	if inserted.TranslatedText == "" {
		t.Fatal("expected translated text")
	}
}

func TestRuntimeWritesOperationalLogs(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	source := newFixtureSource(2)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}
	processResult, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 1, TimeoutSeconds: 30})
	if err != nil {
		t.Fatalf("process one batch: %v", err)
	}
	if processResult.PendingCount != 1 || processResult.OutputCount != 1 {
		t.Fatalf("ProcessOneBatch() result = %#v, want one pending and one output row", processResult)
	}
	if _, err := runtime.UploadOutput(ctx); err != nil {
		t.Fatalf("upload output: %v", err)
	}

	logText := logs.String()
	required := []string{
		`"msg":"runtime initialized"`,
		`"msg":"load input completed"`,
		`"rows_inserted":2`,
		`"msg":"process batch completed"`,
		`"translated_count":1`,
		`"msg":"upload output completed"`,
		`"rows_seen":1`,
	}
	for _, fragment := range required {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("expected runtime logs to contain %s\nlogs:\n%s", fragment, logText)
		}
	}
}

func TestRuntimeLogsQueueCountsWhenBatchIsEmpty(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	source := newFixtureSource(0)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	processResult, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 1, TimeoutSeconds: 30})
	if err != nil {
		t.Fatalf("process one batch: %v", err)
	}
	if processResult.PendingCount != 0 || processResult.OutputCount != 0 {
		t.Fatalf("empty ProcessOneBatch() result = %#v, want zero pending/output counts", processResult)
	}

	logText := logs.String()
	required := []string{
		`"msg":"process batch completed"`,
		`"translated_count":0`,
		`"input_count":0`,
		`"output_count":0`,
		`"pending_count":0`,
	}
	for _, fragment := range required {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("expected empty-batch logs to contain %s\nlogs:\n%s", fragment, logText)
		}
	}
}

func TestRuntimeMarksSingleUnexpectedTranslationResultFailed(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(1)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   unexpectedRuntimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}

	result, err := runtime.ProcessOneBatch(ctx, ProcessInput{
		BatchSize:      10,
		TimeoutSeconds: 30,
	})
	if err != nil {
		t.Fatalf("ProcessOneBatch() error = %v, want nil", err)
	}
	if result.TranslatedCount != 0 || result.PendingCount != 0 || result.OutputCount != 0 {
		t.Fatalf("ProcessOneBatch() result = %#v, want 0 translated, 0 pending, 0 output", result)
	}

	var outputCount int
	if err := runtime.db.QueryRowContext(ctx, "select count(*) from output_items").Scan(&outputCount); err != nil {
		t.Fatalf("count output items: %v", err)
	}
	if outputCount != 0 {
		t.Fatalf("output_items count = %d, want 0", outputCount)
	}
	var failedCount int
	if err := runtime.db.QueryRowContext(ctx, "select count(*) from failed_items").Scan(&failedCount); err != nil {
		t.Fatalf("count failed items: %v", err)
	}
	if failedCount != 1 {
		t.Fatalf("failed_items count = %d, want 1", failedCount)
	}
}

func TestRuntimeRetriesModelOutputFailureWithShuffledItems(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(4)
	translator := &modelOutputThenSuccessTranslator{failuresBeforeSuccess: 1}
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   translator,
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}

	result, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 4, TimeoutSeconds: 30})
	if err != nil {
		t.Fatalf("ProcessOneBatch() error = %v, want nil", err)
	}
	if result.TranslatedCount != 4 || result.PendingCount != 0 || result.OutputCount != 4 {
		t.Fatalf("ProcessOneBatch() result = %#v, want 4 translated, 0 pending, 4 output", result)
	}
	if got := len(translator.calls); got != 2 {
		t.Fatalf("translator calls = %d, want 2", got)
	}
	if slices.Equal(translator.calls[0], translator.calls[1]) {
		t.Fatalf("second translator call order = %v, want shuffled order different from first", translator.calls[1])
	}
}

func TestRuntimeSplitsBatchAfterRepeatedModelOutputFailures(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(4)
	translator := &failLargeBatchTranslator{maxSuccessfulBatchSize: 2}
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   translator,
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}

	result, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 4, TimeoutSeconds: 30})
	if err != nil {
		t.Fatalf("ProcessOneBatch() error = %v, want nil", err)
	}
	if result.TranslatedCount != 4 || result.PendingCount != 0 || result.OutputCount != 4 {
		t.Fatalf("ProcessOneBatch() result = %#v, want 4 translated, 0 pending, 4 output", result)
	}

	wantSizes := []int{4, 4, 2, 2}
	if !slices.Equal(translator.callSizes, wantSizes) {
		t.Fatalf("translator call sizes = %v, want %v", translator.callSizes, wantSizes)
	}
}

func TestRuntimeMarksSingleItemFailedAfterRepeatedModelOutputFailures(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(1)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Definition:   norwayDefinition(),
		Source:       source,
		Translator:   alwaysModelOutputFailureTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}

	result, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 1, TimeoutSeconds: 30})
	if err != nil {
		t.Fatalf("ProcessOneBatch() error = %v, want nil", err)
	}
	if result.TranslatedCount != 0 || result.PendingCount != 0 || result.OutputCount != 0 {
		t.Fatalf("ProcessOneBatch() result = %#v, want 0 translated, 0 pending, 0 output", result)
	}

	var failedCount int
	if err := runtime.db.QueryRowContext(ctx, "select count(*) from failed_items").Scan(&failedCount); err != nil {
		t.Fatalf("count failed items: %v", err)
	}
	if failedCount != 1 {
		t.Fatalf("failed_items count = %d, want 1", failedCount)
	}
}

type runtimeTranslator struct{}

func (runtimeTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	results := make([]translation.TranslationResult, 0, len(items))
	for _, item := range items {
		results = append(results, translation.TranslationResult{
			ItemID:         item.ItemID,
			TranslatedText: fmt.Sprintf("translated %s", item.SourceText),
		})
	}
	return results, nil
}

type unexpectedRuntimeTranslator struct{}

func (unexpectedRuntimeTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	return []translation.TranslationResult{
		{ItemID: items[0].ItemID, TranslatedText: "translated expected"},
		{ItemID: "unknown-item", TranslatedText: "translated unknown"},
	}, nil
}

type modelOutputThenSuccessTranslator struct {
	failuresBeforeSuccess int
	calls                 [][]string
}

func (t *modelOutputThenSuccessTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	t.calls = append(t.calls, itemIDs(items))
	if len(t.calls) <= t.failuresBeforeSuccess {
		return nil, fmt.Errorf("fake model output: %w", translation.ErrModelOutput)
	}
	return successfulRuntimeTranslations(items), nil
}

type failLargeBatchTranslator struct {
	maxSuccessfulBatchSize int
	callSizes              []int
}

func (t *failLargeBatchTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	t.callSizes = append(t.callSizes, len(items))
	if len(items) > t.maxSuccessfulBatchSize {
		return nil, fmt.Errorf("fake model output: %w", translation.ErrModelOutput)
	}
	return successfulRuntimeTranslations(items), nil
}

type alwaysModelOutputFailureTranslator struct{}

func (alwaysModelOutputFailureTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	return nil, fmt.Errorf("fake model output: %w", translation.ErrModelOutput)
}

func successfulRuntimeTranslations(items []translation.TranslationInput) []translation.TranslationResult {
	results := make([]translation.TranslationResult, 0, len(items))
	for _, item := range items {
		results = append(results, translation.TranslationResult{
			ItemID:         item.ItemID,
			TranslatedText: "translated " + item.SourceText,
		})
	}
	return results
}

func itemIDs(items []translation.TranslationInput) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ItemID)
	}
	return ids
}
