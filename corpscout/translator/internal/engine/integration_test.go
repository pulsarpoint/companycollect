package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/config"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"
)

// uppercaseEchoTranslator is a fake Translator that returns the source text
// upper-cased, standing in for a real LLM/provider call in the integration
// test below.
type uppercaseEchoTranslator struct{}

func (uppercaseEchoTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
	promptData translation.PromptData,
) ([]translation.TranslationResult, error) {
	results := make([]translation.TranslationResult, 0, len(items))
	for _, item := range items {
		results = append(results, translation.TranslationResult{
			ItemID:         item.ItemID,
			TranslatedText: strings.ToUpper(item.SourceText),
		})
	}
	return results, nil
}

// TestEnqueueProcessFlushWithExistingClickHouse exercises the whole engine
// loop end to end against a real ClickHouse: enqueue two synthetic items,
// process them with a fake translator, flush the output, and confirm the
// rows land in corpscout.text_translations while the SQLite queue empties.
func TestEnqueueProcessFlushWithExistingClickHouse(t *testing.T) {
	if os.Getenv("TRANSLATOR_INTEGRATION_TESTS") != "true" {
		t.Skip("set TRANSLATOR_INTEGRATION_TESTS=true and CLICKHOUSE_* environment variables to run")
	}

	cfg, _, err := config.LoadFromEnvironment()
	if err != nil {
		t.Fatalf("load translator config: %v", err)
	}
	if cfg.ClickHouse.NativeURL == "" {
		t.Fatal("clickhouse native URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("clickhouse", cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	const (
		integrationTable  = "corpscout.integration_test"
		integrationColumn = "enqueue_process_flush_test"
	)

	deleteIntegrationRows := func() {
		_, _ = db.ExecContext(context.Background(), `
			ALTER TABLE corpscout.text_translations
			DELETE WHERE source_table = '`+integrationTable+`'
			  AND source_column = '`+integrationColumn+`'
		`)
	}
	deleteIntegrationRows()
	t.Cleanup(deleteIntegrationRows)

	source, err := OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open clickhouse source: %v", err)
	}
	defer source.Close()

	queuePath := filepath.Join(t.TempDir(), "enqueue_process_flush.sqlite")
	runtime, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    queuePath,
		Source:       source,
		Translator:   uppercaseEchoTranslator{},
		ProviderName: "integration-test",
		Model:        "uppercase-echo",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer runtime.Close()

	enqueueResult, err := runtime.Enqueue(ctx, EnqueueRequest{
		SourceLang:         "no",
		TargetLang:         "en",
		SourceLanguageName: "Norwegian",
		TargetLanguageName: "English",
		Items: []EnqueueItem{
			{
				SourceTable:    integrationTable,
				SourceColumn:   integrationColumn,
				SourceText:     "første tekst",
				SourceTextHash: "1001",
			},
			{
				SourceTable:    integrationTable,
				SourceColumn:   integrationColumn,
				SourceText:     "andre tekst",
				SourceTextHash: "1002",
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if enqueueResult.Received != 2 || enqueueResult.Inserted != 2 {
		t.Fatalf("expected 2/2 enqueued, got %+v", enqueueResult)
	}

	processResult, err := runtime.ProcessOneBatch(ctx, ProcessInput{BatchSize: 10, TimeoutSeconds: 30})
	if err != nil {
		t.Fatalf("process one batch: %v", err)
	}
	if processResult.TranslatedCount != 2 || processResult.PendingCount != 0 || processResult.OutputCount != 2 {
		t.Fatalf("unexpected process result: %+v", processResult)
	}

	flushResult, err := runtime.FlushOutput(ctx)
	if err != nil {
		t.Fatalf("flush output: %v", err)
	}
	if flushResult.RowsSeen != 2 || flushResult.RowsInserted != 2 {
		t.Fatalf("unexpected flush result: %+v", flushResult)
	}

	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count()
		FROM corpscout.text_translations
		WHERE source_table = '`+integrationTable+`'
		  AND source_column = '`+integrationColumn+`'
		  AND translated_text IN ('FØRSTE TEKST', 'ANDRE TEKST')
	`).Scan(&count); err != nil {
		t.Fatalf("count inserted rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 uppercased rows in corpscout.text_translations, got %d", count)
	}

	outputCount, err := countRows(ctx, runtime.db, "output_items")
	if err != nil {
		t.Fatalf("count output_items: %v", err)
	}
	if outputCount != 0 {
		t.Fatalf("output_items count = %d, want 0", outputCount)
	}

	inputCount, err := countRows(ctx, runtime.db, "input_items")
	if err != nil {
		t.Fatalf("count input_items: %v", err)
	}
	if inputCount != 0 {
		t.Fatalf("input_items count = %d, want 0 (matched inputs deleted by flush)", inputCount)
	}
}

// TestInsertTextTranslationsWithExistingClickHouse exercises
// ClickHouse.InsertTextTranslations directly against a real ClickHouse.
func TestInsertTextTranslationsWithExistingClickHouse(t *testing.T) {
	if os.Getenv("TRANSLATOR_INTEGRATION_TESTS") != "true" {
		t.Skip("set TRANSLATOR_INTEGRATION_TESTS=true and CLICKHOUSE_* environment variables to run")
	}

	cfg, _, err := config.LoadFromEnvironment()
	if err != nil {
		t.Fatalf("load translator config: %v", err)
	}
	if cfg.ClickHouse.NativeURL == "" {
		t.Fatal("clickhouse native URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("clickhouse", cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	deleteIntegrationRows := func() {
		_, _ = db.ExecContext(context.Background(), `
			ALTER TABLE corpscout.text_translations
			DELETE WHERE source_table = 'corpscout.integration_test'
			  AND source_column = 'translation_batch_test'
		`)
	}
	deleteIntegrationRows()
	t.Cleanup(deleteIntegrationRows)

	source, err := OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open brreg clickhouse source: %v", err)
	}
	defer source.Close()

	inserted, err := source.InsertTextTranslations(ctx, []TextTranslation{
		{
			SourceTable:    "corpscout.integration_test",
			SourceColumn:   "translation_batch_test",
			SourceTextHash: 18_446_744_073_709_551_615,
			SourceLang:     "no",
			TargetLang:     "en",
			TranslatedText: "integration translation",
			Provider:       "integration-test",
			Model:          "integration-test",
			Version:        time.Now().Unix(),
		},
	})
	if err != nil {
		t.Fatalf("insert text translations: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("expected 1 inserted row, got %d", inserted)
	}

	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count()
		FROM corpscout.text_translations
		WHERE source_table = 'corpscout.integration_test'
		  AND source_column = 'translation_batch_test'
		  AND source_text_hash = 18446744073709551615
		  AND translated_text = 'integration translation'
	`).Scan(&count); err != nil {
		t.Fatalf("count inserted integration row: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted integration row, got %d", count)
	}
}
