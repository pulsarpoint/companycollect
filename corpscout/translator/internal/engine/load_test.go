package engine

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

const (
	testSourceTable     = "corpscout.no_companies"
	testArticlesColumn  = "articles_purpose_original"
	testActivityColumn  = "activity_text_original"
	testLegalFormColumn = "legal_form_description_original"
	testSourceLang      = "no"
	testTargetLang      = "en"
)

func TestLoadInputCreatesQueueDuckDBWithOneHundredRows(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(100)
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	result, err := LoadInput(ctx, source, norwayDefinition(), Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("load input: %v", err)
	}

	if !result.Created {
		t.Fatal("expected new queue file to be created")
	}
	if result.RowsSeen != 100 {
		t.Fatalf("expected 100 source rows, got %d", result.RowsSeen)
	}
	if result.RowsInserted != 100 {
		t.Fatalf("expected 100 inserted rows, got %d", result.RowsInserted)
	}
	if len(source.queries) != 2 {
		t.Fatalf("expected 2 ClickHouse scan queries, got %d", len(source.queries))
	}

	if got := tableCount(t, queuePath, "input_items"); got != 100 {
		t.Fatalf("expected 100 input rows, got %d", got)
	}
	if got := tableCount(t, queuePath, "output_items"); got != 0 {
		t.Fatalf("expected empty output queue, got %d rows", got)
	}

	assertColumnCount(t, queuePath, testArticlesColumn, 50)
	assertColumnCount(t, queuePath, testActivityColumn, 50)
}

func TestLoadInputWithDBUsesCallerOwnedConnection(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(10)
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	db, err := sql.Open("duckdb", queuePath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	result, err := loadInputWithDB(ctx, source, norwayDefinition(), db, queuePath, true)
	if err != nil {
		t.Fatalf("load input with db: %v", err)
	}
	if !result.Created {
		t.Fatal("expected created flag to be preserved")
	}
	if result.RowsInserted != 10 {
		t.Fatalf("expected 10 inserted rows, got %d", result.RowsInserted)
	}

	var count int
	if err := db.QueryRow("select count(*) from input_items").Scan(&count); err != nil {
		t.Fatalf("caller-owned db should remain usable: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10 input rows, got %d", count)
	}
}

func TestLoadInputUpsertsExistingQueueFile(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(100)
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	if _, err := LoadInput(ctx, source, norwayDefinition(), Options{QueuePath: queuePath}); err != nil {
		t.Fatalf("first load input: %v", err)
	}

	source.rowsByColumn[testArticlesColumn] = append(
		source.rowsByColumn[testArticlesColumn],
		fixtureInputItem(testArticlesColumn, 100),
	)

	result, err := LoadInput(ctx, source, norwayDefinition(), Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("second load input: %v", err)
	}
	if result.Created {
		t.Fatal("expected existing queue file to be reused")
	}
	if result.RowsSeen != 101 {
		t.Fatalf("expected 101 source rows on second load, got %d", result.RowsSeen)
	}
	if result.RowsInserted != 1 {
		t.Fatalf("expected 1 new inserted row, got %d", result.RowsInserted)
	}
	if got := tableCount(t, queuePath, "input_items"); got != 101 {
		t.Fatalf("expected 101 input rows after upsert, got %d", got)
	}
}

func TestLoadInputFlushesStaticColumnsDirectlyToClickHouse(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(100)
	source.staticRows = []StaticInput{
		{
			SourceText:     "Aksjeselskap",
			SourceTextHash: 9001,
			Key:            "AS",
		},
		{
			SourceText:     "Ukjent form",
			SourceTextHash: 9002,
			Key:            "UNKNOWN",
		},
	}
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	result, err := LoadInput(ctx, source, norwayDefinition(), Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("load input: %v", err)
	}

	if result.StaticRowsSeen != 2 {
		t.Fatalf("expected 2 static source rows, got %d", result.StaticRowsSeen)
	}
	if result.StaticFlushed != 1 {
		t.Fatalf("expected 1 static row flushed, got %d", result.StaticFlushed)
	}
	if len(source.staticQueries) != 1 {
		t.Fatalf("expected 1 static ClickHouse scan query, got %d", len(source.staticQueries))
	}
	if !strings.Contains(source.staticQueries[0], "c.legal_form_code AS legal_form_code") {
		t.Fatalf("expected static scan to select legal_form_code:\n%s", source.staticQueries[0])
	}
	if got := inputCountByColumn(t, queuePath, testLegalFormColumn); got != 0 {
		t.Fatalf("static legal-form rows must not enter input queue, got %d rows", got)
	}

	if len(source.insertedTranslations) != 1 {
		t.Fatalf("expected 1 inserted static translation, got %d", len(source.insertedTranslations))
	}
	inserted := source.insertedTranslations[0]
	if inserted.SourceTable != testSourceTable {
		t.Fatalf("expected source table %q, got %q", testSourceTable, inserted.SourceTable)
	}
	if inserted.SourceColumn != testLegalFormColumn {
		t.Fatalf("expected source column %q, got %q", testLegalFormColumn, inserted.SourceColumn)
	}
	if inserted.SourceText != "Aksjeselskap" {
		t.Fatalf("expected source text Aksjeselskap, got %q", inserted.SourceText)
	}
	if inserted.SourceTextHash != 9001 {
		t.Fatalf("expected source hash 9001, got %d", inserted.SourceTextHash)
	}
	if inserted.TranslatedText != "Private limited company" {
		t.Fatalf("expected static translation, got %q", inserted.TranslatedText)
	}
	if inserted.Provider != "static" || inserted.Model != "static" {
		t.Fatalf("expected static provider/model, got provider=%q model=%q", inserted.Provider, inserted.Model)
	}
	if inserted.SourceLang != testSourceLang || inserted.TargetLang != testTargetLang {
		t.Fatalf("expected %s->%s, got %s->%s", testSourceLang, testTargetLang, inserted.SourceLang, inserted.TargetLang)
	}
	if inserted.Version == 0 {
		t.Fatal("expected non-zero static translation version")
	}
}

func TestLoadInputStoresUnsignedCityHashValues(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(0)
	source.rowsByColumn[testArticlesColumn] = []InputItem{
		{
			SourceTable:    testSourceTable,
			SourceColumn:   testArticlesColumn,
			SourceText:     "high unsigned hash",
			SourceTextHash: math.MaxUint64,
			SourceLang:     testSourceLang,
			TargetLang:     testTargetLang,
		},
	}
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	result, err := LoadInput(ctx, source, norwayDefinition(), Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("load input: %v", err)
	}

	if result.RowsInserted != 1 {
		t.Fatalf("expected 1 inserted row, got %d", result.RowsInserted)
	}

	db, err := sql.Open("duckdb", queuePath)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var stored string
	if err := db.QueryRow("select source_text_hash::varchar from input_items").Scan(&stored); err != nil {
		t.Fatalf("read source_text_hash: %v", err)
	}
	if stored != "18446744073709551615" {
		t.Fatalf("expected max uint64 hash, got %s", stored)
	}
}

type fixtureSource struct {
	rowsByColumn         map[string][]InputItem
	staticRows           []StaticInput
	queries              []string
	staticQueries        []string
	insertedTranslations []TextTranslation
}

func newFixtureSource(count int) *fixtureSource {
	rowsByColumn := map[string][]InputItem{
		testArticlesColumn: make([]InputItem, 0, count/2),
		testActivityColumn: make([]InputItem, 0, count/2),
	}
	for index := 0; index < count; index++ {
		column := testArticlesColumn
		if index >= count/2 {
			column = testActivityColumn
		}
		rowsByColumn[column] = append(rowsByColumn[column], fixtureInputItem(column, index))
	}
	return &fixtureSource{rowsByColumn: rowsByColumn}
}

func (s *fixtureSource) QueryTranslationInput(ctx context.Context, query string) ([]InputItem, error) {
	s.queries = append(s.queries, query)

	switch {
	case strings.Contains(query, testArticlesColumn):
		return append([]InputItem(nil), s.rowsByColumn[testArticlesColumn]...), nil
	case strings.Contains(query, testActivityColumn):
		return append([]InputItem(nil), s.rowsByColumn[testActivityColumn]...), nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (s *fixtureSource) QueryStaticInput(ctx context.Context, query string) ([]StaticInput, error) {
	s.staticQueries = append(s.staticQueries, query)
	return append([]StaticInput(nil), s.staticRows...), nil
}

func (s *fixtureSource) InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error) {
	s.insertedTranslations = append(s.insertedTranslations, rows...)
	return len(rows), nil
}

func fixtureInputItem(column string, index int) InputItem {
	return InputItem{
		SourceTable:        testSourceTable,
		SourceColumn:       column,
		SourceText:         fmt.Sprintf("Norwegian text %03d", index),
		SourceTextHash:     uint64(10_000 + index),
		SourceLang:         testSourceLang,
		TargetLang:         testTargetLang,
		SourceLanguageName: "Norwegian",
		TargetLanguageName: "English",
	}
}

func tableCount(t *testing.T, path string, table string) int {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("select count(*) from " + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func assertColumnCount(t *testing.T, path string, column string, want int) {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(
		"select count(*) from input_items where source_column = ?",
		column,
	).Scan(&count); err != nil {
		t.Fatalf("count column %s: %v", column, err)
	}
	if count != want {
		t.Fatalf("expected %d rows for %s, got %d", want, column, count)
	}
}

func inputCountByColumn(t *testing.T, path string, column string) int {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(
		"select count(*) from input_items where source_column = ?",
		column,
	).Scan(&count); err != nil {
		t.Fatalf("count column %s: %v", column, err)
	}
	return count
}
