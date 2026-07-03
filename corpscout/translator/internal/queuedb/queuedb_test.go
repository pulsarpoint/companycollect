package queuedb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func openTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.sqlite")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open queue db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := CreateTables(context.Background(), db); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	return db, path
}

func TestOpenAppliesPragmas(t *testing.T) {
	db, _ := openTestDB(t)

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("expected WAL journal mode, got %q", journalMode)
	}

	var busyTimeout int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("expected busy_timeout 5000, got %d", busyTimeout)
	}
}

func TestCreateTablesIsIdempotentAndCreatesAllObjects(t *testing.T) {
	db, _ := openTestDB(t)
	if err := CreateTables(context.Background(), db); err != nil {
		t.Fatalf("second create tables must be idempotent: %v", err)
	}

	for _, object := range []struct{ kind, name string }{
		{"table", "input_items"},
		{"table", "output_items"},
		{"table", "failed_items"},
		{"view", "pending_items"},
		{"index", "idx_input_created"},
		{"index", "idx_input_pair_created"},
	} {
		var count int
		if err := db.QueryRow(
			"SELECT count(*) FROM sqlite_master WHERE type = ? AND name = ?",
			object.kind, object.name,
		).Scan(&count); err != nil {
			t.Fatalf("check %s %s: %v", object.kind, object.name, err)
		}
		if count != 1 {
			t.Fatalf("expected %s %s to exist", object.kind, object.name)
		}
	}
}

func TestPendingItemsViewSemantics(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()

	insert := func(table, hash string) {
		t.Helper()
		var query string
		switch table {
		case "input_items":
			query = `insert into input_items (source_table, source_column, source_text, source_text_hash,
				source_lang, target_lang, source_language_name, target_language_name)
				values ('corpscout.no_companies', 'activity_text_original', 'tekst', ?, 'no', 'en', 'Norwegian', 'English')`
		case "output_items":
			query = `insert into output_items (source_table, source_column, source_text, source_text_hash,
				source_lang, target_lang, translated_text, provider, model)
				values ('corpscout.no_companies', 'activity_text_original', 'tekst', ?, 'no', 'en', 'text', 'local_llm', 'm')`
		case "failed_items":
			query = `insert into failed_items (source_table, source_column, source_text, source_text_hash,
				source_lang, target_lang, error_message)
				values ('corpscout.no_companies', 'activity_text_original', 'tekst', ?, 'no', 'en', 'boom')`
		}
		if _, err := db.ExecContext(ctx, query, hash); err != nil {
			t.Fatalf("insert into %s: %v", table, err)
		}
	}
	pendingCount := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT count(*) FROM pending_items").Scan(&n); err != nil {
			t.Fatalf("count pending: %v", err)
		}
		return n
	}

	insert("input_items", "1")
	insert("input_items", "2")
	insert("input_items", "3")
	if got := pendingCount(); got != 3 {
		t.Fatalf("expected 3 pending, got %d", got)
	}

	insert("output_items", "1") // translated → leaves the view
	if got := pendingCount(); got != 2 {
		t.Fatalf("expected 2 pending after output, got %d", got)
	}

	insert("failed_items", "2") // failed → leaves the view
	if got := pendingCount(); got != 1 {
		t.Fatalf("expected 1 pending after failure, got %d", got)
	}
}
