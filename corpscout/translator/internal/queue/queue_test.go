package queue_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/queue"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestInitFailsWhenQueueFileIsMissing(t *testing.T) {
	_, err := queue.Init(filepath.Join(t.TempDir(), "missing.duckdb"), &fakeTranslator{})
	if err == nil {
		t.Fatal("expected missing queue file to fail")
	}
	if !strings.Contains(err.Error(), "queue duckdb file does not exist") {
		t.Fatalf("expected missing-file error, got %v", err)
	}
}

func TestInitFailsWhenQueueFileDoesNotHaveQueueTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open fixture duckdb: %v", err)
	}
	if _, err := db.Exec("create table something_else (id integer)"); err != nil {
		t.Fatalf("create invalid fixture table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture duckdb: %v", err)
	}

	_, err = queue.Init(path, &fakeTranslator{})
	if err == nil {
		t.Fatal("expected invalid queue schema to fail")
	}
	if !strings.Contains(err.Error(), "input_items") {
		t.Fatalf("expected input_items schema error, got %v", err)
	}
}

func TestQueueGetsSavesAndProcessesOnlyUntranslatedRows(t *testing.T) {
	ctx := context.Background()
	path := createQueueFixture(t, 100)
	pretranslated := map[uint64]bool{
		1000: true,
		1025: true,
		1099: true,
	}
	insertOutput(t, path, inputFixtureRow(0), "already translated 0", "fixture-provider", "fixture-model")
	insertOutput(t, path, inputFixtureRow(25), "already translated 25", "fixture-provider", "fixture-model")
	insertOutput(t, path, inputFixtureRow(99), "already translated 99", "fixture-provider", "fixture-model")

	translator := &fakeTranslator{}
	q, err := queue.Init(path, translator)
	if err != nil {
		t.Fatalf("init queue: %v", err)
	}
	defer q.Close()

	firstBatch, err := q.GetBatch(ctx, 10)
	if err != nil {
		t.Fatalf("get first batch: %v", err)
	}
	if len(firstBatch) != 10 {
		t.Fatalf("expected first batch of 10 untranslated rows, got %d", len(firstBatch))
	}
	for _, item := range firstBatch {
		if pretranslated[item.SourceTextHash] {
			t.Fatalf("batch returned already translated hash %d", item.SourceTextHash)
		}
	}

	totalProcessed := 0
	for {
		processed, err := q.ProcessBatch(ctx, 17, 45, "local", "qwen3:6b")
		if err != nil {
			t.Fatalf("process batch: %v", err)
		}
		if processed == 0 {
			break
		}
		totalProcessed += processed
	}

	if totalProcessed != 97 {
		t.Fatalf("expected to process 97 untranslated rows, got %d", totalProcessed)
	}
	if translator.timeoutSeconds != 45 {
		t.Fatalf("expected translator timeout 45, got %d", translator.timeoutSeconds)
	}
	if translator.itemsSeen != 97 {
		t.Fatalf("expected translator to receive 97 rows, got %d", translator.itemsSeen)
	}
	for _, item := range translator.items {
		if item.SourceLang != "no" || item.TargetLang != "en" {
			t.Fatalf("expected translator input language no->en, got %s->%s", item.SourceLang, item.TargetLang)
		}
	}

	remaining, err := q.GetBatch(ctx, 10)
	if err != nil {
		t.Fatalf("get remaining batch: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected empty queue after processing, got %d rows", len(remaining))
	}

	if got := outputCount(t, path); got != 100 {
		t.Fatalf("expected 100 output rows after processing, got %d", got)
	}
	assertOutputRow(t, path, inputFixtureRow(42), "translated Norwegian text 042", "local", "qwen3:6b")
}

func TestQueueHandlesUnsignedCityHashValues(t *testing.T) {
	ctx := context.Background()
	path := createEmptyQueueFixture(t)
	row := queue.Item{
		SourceTable:    "corpscout.no_companies",
		SourceColumn:   "activity_text_original",
		SourceText:     "high hash",
		SourceTextHash: math.MaxUint64,
		SourceLang:     "no",
		TargetLang:     "en",
	}
	insertInput(t, path, row)

	q, err := queue.Init(path, &fakeTranslator{})
	if err != nil {
		t.Fatalf("init queue: %v", err)
	}
	defer q.Close()

	batch, err := q.GetBatch(ctx, 1)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected one row, got %d", len(batch))
	}
	if batch[0].SourceTextHash != math.MaxUint64 {
		t.Fatalf("expected max uint64 hash, got %d", batch[0].SourceTextHash)
	}

	if err := q.SaveBatch(ctx, []queue.TranslatedItem{{
		Item:           batch[0],
		TranslatedText: "translated high hash",
		Provider:       "local",
		Model:          "qwen3:6b",
	}}); err != nil {
		t.Fatalf("save batch: %v", err)
	}
	assertOutputRow(t, path, row, "translated high hash", "local", "qwen3:6b")
}

func TestProcessBatchRejectsUnexpectedTranslationResult(t *testing.T) {
	ctx := context.Background()
	path := createQueueFixture(t, 1)

	q, err := queue.Init(path, unexpectedResultTranslator{})
	if err != nil {
		t.Fatalf("init queue: %v", err)
	}

	processed, err := q.ProcessBatch(ctx, 10, 45, "local", "qwen3:6b")
	if err == nil {
		t.Fatal("expected unexpected translation result to fail")
	}
	if processed != 0 {
		t.Fatalf("expected zero processed rows on failure, got %d", processed)
	}
	if !strings.Contains(err.Error(), "unexpected translation result") {
		t.Fatalf("expected unexpected result error, got %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}
	if got := outputCount(t, path); got != 0 {
		t.Fatalf("expected no saved output rows after failure, got %d", got)
	}
}

type fakeTranslator struct {
	timeoutSeconds int
	itemsSeen      int
	items          []translation.TranslationInput
}

func (f *fakeTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	f.timeoutSeconds = timeoutSeconds
	f.itemsSeen += len(items)
	f.items = append(f.items, items...)

	results := make([]translation.TranslationResult, 0, len(items))
	for _, item := range items {
		results = append(results, translation.TranslationResult{
			ItemID:         item.ItemID,
			TranslatedText: "translated " + item.SourceText,
		})
	}
	return results, nil
}

type unexpectedResultTranslator struct{}

func (unexpectedResultTranslator) Translate(
	ctx context.Context,
	items []translation.TranslationInput,
	timeoutSeconds int,
) ([]translation.TranslationResult, error) {
	return []translation.TranslationResult{
		{ItemID: items[0].ItemID, TranslatedText: "translated expected"},
		{ItemID: "unknown-item", TranslatedText: "translated unknown"},
	}, nil
}

func createQueueFixture(t *testing.T, count int) string {
	t.Helper()

	path := createEmptyQueueFixture(t)
	for index := range count {
		insertInput(t, path, inputFixtureRow(index))
	}
	return path
}

func createEmptyQueueFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "queue.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open fixture duckdb: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		create table input_items (
			source_table text not null,
			source_column text not null,
			source_text text not null,
			source_text_hash ubigint not null,
			source_lang text not null,
			target_lang text not null,
			created_at timestamp not null,
			primary key (
				source_table,
				source_column,
				source_text_hash,
				source_lang,
				target_lang
			)
		)
	`); err != nil {
		t.Fatalf("create input_items: %v", err)
	}

	if _, err := db.Exec(`
		create table output_items (
			source_table text not null,
			source_column text not null,
			source_text text not null,
			source_text_hash ubigint not null,
			source_lang text not null,
			target_lang text not null,
			translated_text text not null,
			provider text not null,
			model text not null,
			completed_at timestamp not null,
			primary key (
				source_table,
				source_column,
				source_text_hash,
				source_lang,
				target_lang
			)
		)
	`); err != nil {
		t.Fatalf("create output_items: %v", err)
	}

	return path
}

func inputFixtureRow(index int) queue.Item {
	column := "articles_purpose_original"
	if index >= 50 {
		column = "activity_text_original"
	}
	return queue.Item{
		SourceTable:    "corpscout.no_companies",
		SourceColumn:   column,
		SourceText:     fmt.Sprintf("Norwegian text %03d", index),
		SourceTextHash: uint64(1000 + index),
		SourceLang:     "no",
		TargetLang:     "en",
	}
}

func insertInput(t *testing.T, path string, row queue.Item) {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open fixture duckdb: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		insert into input_items (
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			created_at
		)
		values (?, ?, ?, cast(? as ubigint), ?, ?, current_timestamp)
	`, row.SourceTable, row.SourceColumn, row.SourceText, strconv.FormatUint(row.SourceTextHash, 10), row.SourceLang, row.TargetLang); err != nil {
		t.Fatalf("insert input fixture row: %v", err)
	}
}

func insertOutput(t *testing.T, path string, row queue.Item, translatedText string, provider string, model string) {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open fixture duckdb: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		insert into output_items (
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			translated_text,
			provider,
			model,
			completed_at
		)
		values (?, ?, ?, cast(? as ubigint), ?, ?, ?, ?, ?, current_timestamp)
	`, row.SourceTable, row.SourceColumn, row.SourceText, strconv.FormatUint(row.SourceTextHash, 10), row.SourceLang, row.TargetLang, translatedText, provider, model); err != nil {
		t.Fatalf("insert output fixture row: %v", err)
	}
}

func outputCount(t *testing.T, path string) int {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open fixture duckdb: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("select count(*) from output_items").Scan(&count); err != nil {
		t.Fatalf("count output rows: %v", err)
	}
	return count
}

func assertOutputRow(t *testing.T, path string, row queue.Item, translatedText string, provider string, model string) {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open fixture duckdb: %v", err)
	}
	defer db.Close()

	var gotText string
	var gotProvider string
	var gotModel string
	if err := db.QueryRow(`
		select translated_text, provider, model
		from output_items
		where source_table = ?
			and source_column = ?
			and source_text_hash = cast(? as ubigint)
			and source_lang = ?
			and target_lang = ?
	`, row.SourceTable, row.SourceColumn, strconv.FormatUint(row.SourceTextHash, 10), row.SourceLang, row.TargetLang).Scan(&gotText, &gotProvider, &gotModel); err != nil {
		t.Fatalf("read output row: %v", err)
	}
	if gotText != translatedText || gotProvider != provider || gotModel != model {
		t.Fatalf(
			"expected output (%q, %q, %q), got (%q, %q, %q)",
			translatedText,
			provider,
			model,
			gotText,
			gotProvider,
			gotModel,
		)
	}
}
