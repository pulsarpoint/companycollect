package brreg

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

func TestInitializeTranslationCreatesQueueDuckDBWithOneHundredRows(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(100)
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	result, err := InitializeTranslation(ctx, source, Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("initialize translation: %v", err)
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

	assertColumnCount(t, queuePath, ArticlesPurposeColumn, 50)
	assertColumnCount(t, queuePath, ActivityTextColumn, 50)
}

func TestInitializeTranslationUpsertsExistingQueueFile(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(100)
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	if _, err := InitializeTranslation(ctx, source, Options{QueuePath: queuePath}); err != nil {
		t.Fatalf("first initialize translation: %v", err)
	}

	source.rowsByColumn[ArticlesPurposeColumn] = append(
		source.rowsByColumn[ArticlesPurposeColumn],
		fixtureInputItem(ArticlesPurposeColumn, 100),
	)

	result, err := InitializeTranslation(ctx, source, Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("second initialize translation: %v", err)
	}
	if result.Created {
		t.Fatal("expected existing queue file to be reused")
	}
	if result.RowsSeen != 101 {
		t.Fatalf("expected 101 source rows on second init, got %d", result.RowsSeen)
	}
	if result.RowsInserted != 1 {
		t.Fatalf("expected 1 new inserted row, got %d", result.RowsInserted)
	}
	if got := tableCount(t, queuePath, "input_items"); got != 101 {
		t.Fatalf("expected 101 input rows after upsert, got %d", got)
	}
}

func TestInitializeTranslationFlushesStaticLegalFormsDirectlyToClickHouse(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(100)
	source.staticLegalFormRows = []StaticLegalFormInput{
		{
			SourceText:     "Aksjeselskap",
			SourceTextHash: 9001,
			LegalFormCode:  "AS",
		},
		{
			SourceText:     "Ukjent form",
			SourceTextHash: 9002,
			LegalFormCode:  "UNKNOWN",
		},
	}
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	result, err := InitializeTranslation(ctx, source, Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("initialize translation: %v", err)
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
	if got := inputCountByColumn(t, queuePath, LegalFormDescriptionColumn); got != 0 {
		t.Fatalf("static legal-form rows must not enter input queue, got %d rows", got)
	}

	if len(source.insertedTranslations) != 1 {
		t.Fatalf("expected 1 inserted static translation, got %d", len(source.insertedTranslations))
	}
	inserted := source.insertedTranslations[0]
	if inserted.SourceTable != SourceTable {
		t.Fatalf("expected source table %q, got %q", SourceTable, inserted.SourceTable)
	}
	if inserted.SourceColumn != LegalFormDescriptionColumn {
		t.Fatalf("expected source column %q, got %q", LegalFormDescriptionColumn, inserted.SourceColumn)
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
	if inserted.SourceLang != SourceLang || inserted.TargetLang != TargetLang {
		t.Fatalf("expected %s->%s, got %s->%s", SourceLang, TargetLang, inserted.SourceLang, inserted.TargetLang)
	}
	if inserted.Version == 0 {
		t.Fatal("expected non-zero static translation version")
	}
}

func TestInitializeTranslationStoresUnsignedCityHashValues(t *testing.T) {
	ctx := context.Background()
	source := newFixtureSource(0)
	source.rowsByColumn[ArticlesPurposeColumn] = []InputItem{
		{
			SourceTable:    SourceTable,
			SourceColumn:   ArticlesPurposeColumn,
			SourceText:     "high unsigned hash",
			SourceTextHash: math.MaxUint64,
			SourceLang:     SourceLang,
			TargetLang:     TargetLang,
		},
	}
	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")

	result, err := InitializeTranslation(ctx, source, Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("initialize translation: %v", err)
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

func TestScanSQLUsesConcreteClickHouseAntiJoinShape(t *testing.T) {
	assertScanSQL(t, articlesPurposeScanSQL, []string{
		"SELECT DISTINCT",
		"'corpscout.no_companies' AS source_table",
		"'articles_purpose_original' AS source_column",
		"c.articles_purpose_original AS source_text",
		"cityHash64(c.articles_purpose_original) AS source_text_hash",
		"FROM corpscout.no_companies AS c",
		"LEFT ANTI JOIN",
		"FROM corpscout.text_translations",
		"WHERE source_table = 'corpscout.no_companies' AND source_column = 'articles_purpose_original'",
		"ON t.source_text_hash = cityHash64(c.articles_purpose_original)",
		"WHERE c.articles_purpose_original <> ''",
	})
	assertScanSQL(t, activityTextScanSQL, []string{
		"SELECT DISTINCT",
		"'corpscout.no_companies' AS source_table",
		"'activity_text_original' AS source_column",
		"c.activity_text_original AS source_text",
		"cityHash64(c.activity_text_original) AS source_text_hash",
		"FROM corpscout.no_companies AS c",
		"LEFT ANTI JOIN",
		"FROM corpscout.text_translations",
		"WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'",
		"ON t.source_text_hash = cityHash64(c.activity_text_original)",
		"WHERE c.activity_text_original <> ''",
	})
}

func assertScanSQL(t *testing.T, sql string, required []string) {
	t.Helper()

	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("expected SQL to contain %q\nSQL:\n%s", fragment, sql)
		}
	}
	if strings.Contains(sql, "{table:String}") || strings.Contains(sql, "{column:String}") {
		t.Fatalf("BRREG scan SQL must not use generic table/column parameters:\n%s", sql)
	}
}

type fixtureSource struct {
	rowsByColumn         map[string][]InputItem
	staticLegalFormRows  []StaticLegalFormInput
	queries              []string
	staticQueries        []string
	insertedTranslations []TextTranslation
}

func newFixtureSource(count int) *fixtureSource {
	rowsByColumn := map[string][]InputItem{
		ArticlesPurposeColumn: make([]InputItem, 0, count/2),
		ActivityTextColumn:    make([]InputItem, 0, count/2),
	}
	for index := 0; index < count; index++ {
		column := ArticlesPurposeColumn
		if index >= count/2 {
			column = ActivityTextColumn
		}
		rowsByColumn[column] = append(rowsByColumn[column], fixtureInputItem(column, index))
	}
	return &fixtureSource{rowsByColumn: rowsByColumn}
}

func (s *fixtureSource) QueryTranslationInput(ctx context.Context, query string) ([]InputItem, error) {
	s.queries = append(s.queries, query)

	switch {
	case strings.Contains(query, ArticlesPurposeColumn):
		return append([]InputItem(nil), s.rowsByColumn[ArticlesPurposeColumn]...), nil
	case strings.Contains(query, ActivityTextColumn):
		return append([]InputItem(nil), s.rowsByColumn[ActivityTextColumn]...), nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (s *fixtureSource) QueryStaticLegalForms(ctx context.Context, query string) ([]StaticLegalFormInput, error) {
	s.staticQueries = append(s.staticQueries, query)
	return append([]StaticLegalFormInput(nil), s.staticLegalFormRows...), nil
}

func (s *fixtureSource) InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error) {
	s.insertedTranslations = append(s.insertedTranslations, rows...)
	return len(rows), nil
}

func fixtureInputItem(column string, index int) InputItem {
	return InputItem{
		SourceTable:    SourceTable,
		SourceColumn:   column,
		SourceText:     fmt.Sprintf("Norwegian text %03d", index),
		SourceTextHash: uint64(10_000 + index),
		SourceLang:     SourceLang,
		TargetLang:     TargetLang,
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
