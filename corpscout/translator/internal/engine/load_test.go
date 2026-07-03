package engine

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/queuedb"
)

const (
	testSourceTable    = "corpscout.no_companies"
	testActivityColumn = "activity_text_original"
	testSourceLang     = "no"
	testTargetLang     = "en"
	testSourceLangName = "Norwegian"
	testTargetLangName = "English"
)

// openTestQueueDB opens a fresh temp SQLite file with the queue tables
// already created, mirroring what NewRuntime does on startup.
func openTestQueueDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "queue.sqlite")
	db, err := queuedb.Open(path)
	if err != nil {
		t.Fatalf("open queue db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := createQueueTables(context.Background(), db); err != nil {
		t.Fatalf("create queue tables: %v", err)
	}
	return db
}

func TestCreateQueueTablesCreatesEmptyInputOutputFailedTables(t *testing.T) {
	ctx := context.Background()
	db := openTestQueueDB(t)

	for _, table := range []string{"input_items", "output_items", "failed_items"} {
		count, err := countRows(ctx, db, table)
		if err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("expected empty %s, got %d rows", table, count)
		}
	}
}

func TestCreateQueueTablesIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := openTestQueueDB(t)

	if err := createQueueTables(ctx, db); err != nil {
		t.Fatalf("create queue tables again: %v", err)
	}
}

func TestUpsertInputItemsInsertsAndDedupsByQueueKey(t *testing.T) {
	ctx := context.Background()
	db := openTestQueueDB(t)

	row := fixtureInputItem(testActivityColumn, 1)
	if err := upsertInputItems(ctx, db, []InputItem{row}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got := tableCount(t, db, "input_items"); got != 1 {
		t.Fatalf("expected 1 row, got %d", got)
	}

	// Re-upsert with the same dedup key (table, column, hash, source_lang,
	// target_lang) but different text: on-conflict-do-nothing must leave the
	// row count unchanged.
	dup := row
	dup.SourceText = "different text, same key"
	if err := upsertInputItems(ctx, db, []InputItem{dup}); err != nil {
		t.Fatalf("upsert duplicate: %v", err)
	}
	if got := tableCount(t, db, "input_items"); got != 1 {
		t.Fatalf("expected dedup to keep 1 row, got %d", got)
	}

	// A distinct hash inserts a new row.
	distinct := fixtureInputItem(testActivityColumn, 2)
	if err := upsertInputItems(ctx, db, []InputItem{distinct}); err != nil {
		t.Fatalf("upsert distinct: %v", err)
	}
	if got := tableCount(t, db, "input_items"); got != 2 {
		t.Fatalf("expected 2 rows after distinct insert, got %d", got)
	}
}

func TestUpsertInputItemsRejectsIncompleteRows(t *testing.T) {
	ctx := context.Background()
	db := openTestQueueDB(t)

	tests := []struct {
		name    string
		mutate  func(*InputItem)
		wantErr string
	}{
		{"missing source_table", func(r *InputItem) { r.SourceTable = "" }, "source_table is required"},
		{"missing source_column", func(r *InputItem) { r.SourceColumn = "" }, "source_column is required"},
		{"missing source_text", func(r *InputItem) { r.SourceText = "" }, "source_text is required"},
		{"missing source_lang", func(r *InputItem) { r.SourceLang = "" }, "source_lang is required"},
		{"missing target_lang", func(r *InputItem) { r.TargetLang = "" }, "target_lang is required"},
		{"missing source_language_name", func(r *InputItem) { r.SourceLanguageName = "" }, "source_language_name is required"},
		{"missing target_language_name", func(r *InputItem) { r.TargetLanguageName = "" }, "target_language_name is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := fixtureInputItem(testActivityColumn, 1)
			tt.mutate(&row)
			err := upsertInputItems(ctx, db, []InputItem{row})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

// TestUpsertInputItemsStoresUnsignedMaxUint64Hash pins that cityHash64
// values above math.MaxInt64 (which don't fit a signed 64-bit SQLite
// integer) round-trip correctly through the source_text_hash text column.
func TestUpsertInputItemsStoresUnsignedMaxUint64Hash(t *testing.T) {
	ctx := context.Background()
	db := openTestQueueDB(t)

	row := InputItem{
		SourceTable:        testSourceTable,
		SourceColumn:       testActivityColumn,
		SourceText:         "high unsigned hash",
		SourceTextHash:     math.MaxUint64,
		SourceLang:         testSourceLang,
		TargetLang:         testTargetLang,
		SourceLanguageName: testSourceLangName,
		TargetLanguageName: testTargetLangName,
	}
	if err := upsertInputItems(ctx, db, []InputItem{row}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	var stored string
	if err := db.QueryRow("select source_text_hash from input_items").Scan(&stored); err != nil {
		t.Fatalf("read source_text_hash: %v", err)
	}
	if stored != "18446744073709551615" {
		t.Fatalf("expected max uint64 hash, got %s", stored)
	}
}

// fixtureSource is an InsertTextTranslations recorder used by flush tests
// (runtime_test.go); it satisfies ClickHouseSource without talking to a real
// ClickHouse.
type fixtureSource struct {
	insertedTranslations []TextTranslation
}

func newFixtureSource() *fixtureSource {
	return &fixtureSource{}
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
		SourceLanguageName: testSourceLangName,
		TargetLanguageName: testTargetLangName,
	}
}

func tableCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	count, err := countRows(context.Background(), db, table)
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
