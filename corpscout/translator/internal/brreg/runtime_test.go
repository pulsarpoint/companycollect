package brreg

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

func TestRuntimeLoadsProcessesAndUploadsBRREGQueue(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(10)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close(ctx)

	loadResult, err := runtime.LoadNewInput(ctx)
	if err != nil {
		t.Fatalf("load new input: %v", err)
	}
	if loadResult.RowsInserted != 10 {
		t.Fatalf("expected 10 inserted input rows, got %d", loadResult.RowsInserted)
	}

	totalTranslated := 0
	for {
		processResult, err := runtime.ProcessOneBatch(ctx, ProcessInput{
			BatchSize:      3,
			TimeoutSeconds: 30,
		})
		if err != nil {
			t.Fatalf("process one batch: %v", err)
		}
		if processResult.TranslatedCount == 0 {
			break
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

func TestRuntimeSerializesConcurrentBatchProcessing(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(20)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close(ctx)

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}

	var waitGroup sync.WaitGroup
	errs := make(chan error, 20)
	counts := make(chan int, 20)
	for range 20 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			result, err := runtime.ProcessOneBatch(ctx, ProcessInput{
				BatchSize:      1,
				TimeoutSeconds: 30,
			})
			if err != nil {
				errs <- err
				return
			}
			counts <- result.TranslatedCount
		}()
	}
	waitGroup.Wait()
	close(errs)
	close(counts)

	for err := range errs {
		t.Fatalf("concurrent process one batch: %v", err)
	}

	totalTranslated := 0
	for count := range counts {
		totalTranslated += count
	}
	if totalTranslated != 20 {
		t.Fatalf("expected 20 translated rows, got %d", totalTranslated)
	}

	uploadResult, err := runtime.UploadOutput(ctx)
	if err != nil {
		t.Fatalf("upload output: %v", err)
	}
	if uploadResult.RowsInserted != 20 {
		t.Fatalf("expected 20 uploaded rows, got %d", uploadResult.RowsInserted)
	}
}

func TestRuntimeWritesOperationalLogs(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	source := newFixtureSource(2)
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "norway_brreg.duckdb"),
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close(ctx)

	if _, err := runtime.LoadNewInput(ctx); err != nil {
		t.Fatalf("load new input: %v", err)
	}
	if _, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 1, TimeoutSeconds: 30}); err != nil {
		t.Fatalf("process one batch: %v", err)
	}
	if _, err := runtime.UploadOutput(ctx); err != nil {
		t.Fatalf("upload output: %v", err)
	}

	logText := logs.String()
	required := []string{
		`"msg":"brreg runtime initialized"`,
		`"msg":"brreg load input completed"`,
		`"rows_inserted":2`,
		`"msg":"brreg process batch completed"`,
		`"translated_count":1`,
		`"msg":"brreg upload output completed"`,
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
		Source:       source,
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close(ctx)

	if _, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 1, TimeoutSeconds: 30}); err != nil {
		t.Fatalf("process one batch: %v", err)
	}

	logText := logs.String()
	required := []string{
		`"msg":"brreg process batch completed"`,
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
