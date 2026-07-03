// Package queuedb owns the translator queue's SQLite storage: the embedded
// schema (tables, pending_items view, indexes) and the configured
// connection. The queue is transient working state — flushed rows are
// deleted and pending work is re-derivable from the loaders' anti-join —
// so there is no schema migration machinery, only idempotent creation.
package queuedb

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// Open opens (creating if missing) the queue database at path with WAL
// journaling, a 5s busy timeout, NORMAL synchronous mode, and a single
// pooled connection. One connection serializes all writers (HTTP enqueue
// handler and Temporal activity goroutines share the pool), which
// eliminates SQLITE_BUSY contention outright at queue throughput.
func Open(path string) (*sql.DB, error) {
	if path == "" {
		return nil, fmt.Errorf("queue database path is required")
	}

	// Arbitration (path escaping): tested raw path, url.QueryEscape(path),
	// and url.PathEscape(path) against the pragma/idempotency/view tests
	// plus an ad hoc os.Stat(path) check, all using real t.TempDir()
	// absolute paths. On this machine's modernc.org/sqlite (v1.53.0), the
	// file: URI parser percent-decodes the segment before "?" before
	// opening it, so all three candidates actually open the same correct
	// file — the "PathEscape breaks absolute paths" premise from the task
	// brief did not reproduce here. The raw path is used anyway: it is
	// the simplest of the three (no encode/decode round trip to reason
	// about), it is exactly what the tests exercise, and it avoids a
	// latent bug if a path ever contains a literal "%" (which an
	// escape-then-decode round trip could misinterpret). Only the query
	// string (the _pragma params) needs escaping, and those contain no
	// reserved characters.
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open queue sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping queue sqlite: %w", err)
	}
	return db, nil
}

// CreateTables applies the embedded schema; every statement is
// IF NOT EXISTS, so repeated calls are no-ops.
//
// Arbitration (multi-statement ExecContext): schema.sql is a single string
// containing multiple ";"-terminated statements. Verified empirically
// (TestCreateTablesIsIdempotentAndCreatesAllObjects, which checks all six
// schema objects) that modernc.org/sqlite's driver executes a
// multi-statement string passed to a single ExecContext call sequentially
// without complaint, so no ";\n" splitting loop is needed here.
func CreateTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply queue schema: %w", err)
	}
	return nil
}

// SchemaSQL returns the embedded schema text.
func SchemaSQL() string {
	return schemaSQL
}
