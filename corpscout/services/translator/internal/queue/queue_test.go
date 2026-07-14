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
	"github.com/pulsarpoint/corpscout/translator/internal/queuedb"
)

func TestInitFailsWhenQueueFileIsMissing(t *testing.T) {
	_, err := queue.Init(filepath.Join(t.TempDir(), "missing.sqlite"))
	if err == nil {
		t.Fatal("expected missing queue file to fail")
	}
	if !strings.Contains(err.Error(), "queue database file does not exist") {
		t.Fatalf("expected missing-file error, got %v", err)
	}
}

func TestInitFailsWhenQueueFileDoesNotHaveQueueTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.sqlite")
	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
	}
	if _, err := db.Exec("create table something_else (id integer)"); err != nil {
		t.Fatalf("create invalid fixture table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture sqlite: %v", err)
	}

	_, err = queue.Init(path)
	if err == nil {
		t.Fatal("expected invalid queue schema to fail")
	}
	if !strings.Contains(err.Error(), "input_items") {
		t.Fatalf("expected input_items schema error, got %v", err)
	}
}

func TestNewUsesExistingConnectionWithoutClosingIt(t *testing.T) {
	path := createQueueFixture(t, 1)
	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
	}
	defer db.Close()

	q, err := queue.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close queue wrapper: %v", err)
	}

	var count int
	if err := db.QueryRow("select count(*) from input_items").Scan(&count); err != nil {
		t.Fatalf("expected caller-owned db connection to remain open: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 input row, got %d", count)
	}
}

func TestQueueGetsAndSavesOnlyUntranslatedRows(t *testing.T) {
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

	q, err := queue.Init(path)
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

	output := make([]queue.TranslatedItem, 0, len(firstBatch))
	for _, item := range firstBatch {
		output = append(output, queue.TranslatedItem{
			Item:           item,
			TranslatedText: "translated " + item.SourceText,
			Provider:       "local",
			Model:          "qwen3:6b",
		})
	}
	if err := q.SaveBatch(ctx, output); err != nil {
		t.Fatalf("save batch: %v", err)
	}

	remaining, err := q.GetBatch(ctx, 10)
	if err != nil {
		t.Fatalf("get remaining batch: %v", err)
	}
	if len(remaining) != 10 {
		t.Fatalf("expected next batch of 10 untranslated rows, got %d", len(remaining))
	}

	if got := outputCount(t, path); got != 13 {
		t.Fatalf("expected 13 output rows after save, got %d", got)
	}
	assertOutputRow(t, path, firstBatch[0], "translated "+firstBatch[0].SourceText, "local", "qwen3:6b")
}

func TestGetBatchReturnsSingleLanguagePairOldestFirst(t *testing.T) {
	ctx := context.Background()
	db := openTestQueue(t) // existing helper; update its input_items DDL with the two name columns
	q, err := queue.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	// Latvian item inserted FIRST (oldest created_at), then Norwegian items.
	insertTestInput(t, db, "corpscout.lv_companies", "activity_text_original", "Latviešu teksts", 1, "lv", "en", "Latvian", "English")
	insertTestInput(t, db, "corpscout.no_companies", "activity_text_original", "Norsk tekst A", 2, "no", "en", "Norwegian", "English")
	insertTestInput(t, db, "corpscout.no_companies", "activity_text_original", "Norsk tekst B", 3, "no", "en", "Norwegian", "English")

	batch, err := q.GetBatch(ctx, 10)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected only the oldest pair's items (1 Latvian), got %d items", len(batch))
	}
	if batch[0].SourceLang != "lv" || batch[0].SourceLanguageName != "Latvian" || batch[0].TargetLanguageName != "English" {
		t.Fatalf("expected Latvian item with names, got %+v", batch[0])
	}
}

func TestGetBatchReturnsEmptySliceOnEmptyQueue(t *testing.T) {
	db := openTestQueue(t)
	q, err := queue.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	batch, err := q.GetBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("get batch on empty queue: %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected empty batch, got %d items", len(batch))
	}
}

// TestGetBatchQueriesUseIndexes pins the query plans of GetBatch's two hot
// queries: at millions of pending rows a silent regression to a full-table
// scan + sort costs seconds per batch (the whole point of the created_at
// indexes). Plan text is driver/SQLite-version dependent; the assertions
// target the stable parts: the named index appears, and the pair-pick's
// ORDER BY needs no full temp B-tree sort.
func TestGetBatchQueriesUseIndexes(t *testing.T) {
	db := openTestQueue(t)
	insertTestInput(t, db, "corpscout.no_companies", "activity_text_original", "tekst", 1, "no", "en", "Norwegian", "English")

	planFor := func(query string, args ...any) string {
		t.Helper()
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("plan columns: %v", err)
		}

		var plan strings.Builder
		for rows.Next() {
			raw := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range raw {
				ptrs[i] = &raw[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("scan plan row: %v", err)
			}
			// The "detail" column (query plan text) is always the last
			// column regardless of whether the driver returns 3 or 4
			// columns (id, parent, notused, detail).
			detail := fmt.Sprintf("%v", raw[len(raw)-1])
			plan.WriteString(detail)
			plan.WriteString("\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate plan rows: %v", err)
		}
		return plan.String()
	}

	pairPickPlan := planFor(`
		select source_lang, target_lang
		from pending_items
		order by created_at, source_lang, target_lang
		limit 1`)
	t.Logf("pair-pick plan:\n%s", pairPickPlan)
	if !strings.Contains(pairPickPlan, "idx_input_created") {
		t.Fatalf("pair-pick must walk idx_input_created:\n%s", pairPickPlan)
	}
	if strings.Contains(pairPickPlan, "TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("pair-pick must not sort the whole table:\n%s", pairPickPlan)
	}

	batchPlan := planFor(`
		select source_table from pending_items
		where source_lang = ? and target_lang = ?
		order by created_at, source_table, source_column, source_text_hash
		limit ?`, "no", "en", 10)
	t.Logf("batch plan:\n%s", batchPlan)
	if !strings.Contains(batchPlan, "idx_input_pair_created") {
		t.Fatalf("batch query must use idx_input_pair_created:\n%s", batchPlan)
	}
}

func TestQueueHandlesUnsignedCityHashValues(t *testing.T) {
	ctx := context.Background()
	path := createEmptyQueueFixture(t)
	row := queue.Item{
		SourceTable:        "corpscout.no_companies",
		SourceColumn:       "activity_text_original",
		SourceText:         "high hash",
		SourceTextHash:     math.MaxUint64,
		SourceLang:         "no",
		TargetLang:         "en",
		SourceLanguageName: "Norwegian",
		TargetLanguageName: "English",
	}
	insertInput(t, path, row)

	q, err := queue.Init(path)
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

func createQueueFixture(t *testing.T, count int) string {
	t.Helper()

	path := createEmptyQueueFixture(t)
	for index := range count {
		insertInput(t, path, inputFixtureRow(index))
	}
	return path
}

// createEmptyQueueFixture creates a fresh queue database at a temp path via
// queuedb.Open/CreateTables (the schema of record) and returns the path.
func createEmptyQueueFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "queue.sqlite")
	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
	}
	defer db.Close()

	if err := queuedb.CreateTables(context.Background(), db); err != nil {
		t.Fatalf("create fixture tables: %v", err)
	}

	return path
}

// openTestQueue opens a fresh SQLite connection over an empty queue schema
// (via createEmptyQueueFixture) and closes it automatically at test cleanup.
func openTestQueue(t *testing.T) *sql.DB {
	t.Helper()

	path := createEmptyQueueFixture(t)
	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open queue sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var testInputSeq int

func insertTestInput(t *testing.T, db *sql.DB, table, column, text string, hash uint64, srcLang, dstLang, srcName, dstName string) {
	t.Helper()
	testInputSeq++
	if _, err := db.Exec(`
		insert into input_items (
			source_table, source_column, source_text, source_text_hash,
			source_lang, target_lang, source_language_name, target_language_name, created_at
		) values (?, ?, ?, ?, ?, ?, ?, ?, datetime('2026-01-01 00:00:00', '+' || ? || ' seconds'))
	`, table, column, text, strconv.FormatUint(hash, 10), srcLang, dstLang, srcName, dstName, testInputSeq); err != nil {
		t.Fatalf("insert test input: %v", err)
	}
}

func inputFixtureRow(index int) queue.Item {
	column := "articles_purpose_original"
	if index >= 50 {
		column = "activity_text_original"
	}
	return queue.Item{
		SourceTable:        "corpscout.no_companies",
		SourceColumn:       column,
		SourceText:         fmt.Sprintf("Norwegian text %03d", index),
		SourceTextHash:     uint64(1000 + index),
		SourceLang:         "no",
		TargetLang:         "en",
		SourceLanguageName: "Norwegian",
		TargetLanguageName: "English",
	}
}

func insertInput(t *testing.T, path string, row queue.Item) {
	t.Helper()

	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
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
			source_language_name,
			target_language_name,
			created_at
		)
		values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
	`, row.SourceTable, row.SourceColumn, row.SourceText, strconv.FormatUint(row.SourceTextHash, 10), row.SourceLang, row.TargetLang, row.SourceLanguageName, row.TargetLanguageName); err != nil {
		t.Fatalf("insert input fixture row: %v", err)
	}
}

func insertOutput(t *testing.T, path string, row queue.Item, translatedText string, provider string, model string) {
	t.Helper()

	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
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
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
	`, row.SourceTable, row.SourceColumn, row.SourceText, strconv.FormatUint(row.SourceTextHash, 10), row.SourceLang, row.TargetLang, translatedText, provider, model); err != nil {
		t.Fatalf("insert output fixture row: %v", err)
	}
}

func outputCount(t *testing.T, path string) int {
	t.Helper()

	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
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

	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open fixture sqlite: %v", err)
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
			and source_text_hash = ?
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
