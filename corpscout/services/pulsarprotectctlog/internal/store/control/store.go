// Package control is the SQLite-backed control plane: it persists ingestion
// work units and their resumable cursor so a restart continues where it left
// off. It deliberately avoids Postgres (data plane is ClickHouse-only).
package control

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// Status enumerates the lifecycle of a work unit.
type Status string

// Work-unit lifecycle states, stored in the work_units.status column.
const (
	StatusPending Status = "pending" // inserted, not yet started
	StatusRunning Status = "running" // actively draining
	StatusDone    Status = "done"    // fully drained (frozen ctlog); never re-run
	StatusFailed  Status = "failed"  // aborted on a non-cancellation error; resumable
)

// Store wraps the SQLite control database.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS work_units (
  id           TEXT PRIMARY KEY,
  log_name     TEXT NOT NULL,
  start_index  INTEGER NOT NULL,
  end_index    INTEGER NOT NULL,
  window_from  TIMESTAMP NOT NULL,
  window_to    TIMESTAMP NOT NULL,
  status       TEXT NOT NULL DEFAULT 'pending',
  next_index   INTEGER NOT NULL,
  certs_written INTEGER NOT NULL DEFAULT 0,
  sans_written  INTEGER NOT NULL DEFAULT 0,
  error        TEXT,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

// dsn builds a modernc.org/sqlite DSN with WAL journaling and a 5s busy
// timeout so concurrent writers wait for the lock instead of erroring.
func dsn(path string) string {
	return "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
}

// Open opens (creating the parent directory and schema as needed) the control
// database at path.
func Open(ctx context.Context, path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create control db dir: %w", err)
		}
	}
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("open control db: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize this process's writes; WAL handles cross-process
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init control schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the control database.
func (s *Store) Close() error { return s.db.Close() }

// EnsureWorkUnit inserts the work unit if absent. It returns the resume index
// (next_index) and current status. Re-running an existing unit preserves its
// cursor for idempotent resumption.
func (s *Store) EnsureWorkUnit(ctx context.Context, w model.WorkUnit) (resumeIndex int64, status Status, err error) {
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO work_units (id, log_name, start_index, end_index, window_from, window_to, status, next_index)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
		ON CONFLICT(id) DO NOTHING`,
		w.ID, w.LogName, int64(w.StartIndex), int64(w.EndIndex), w.WindowFrom, w.WindowTo, int64(w.StartIndex))
	if err != nil {
		return 0, "", fmt.Errorf("ensure work unit: %w", err)
	}
	row := s.db.QueryRowContext(ctx, `SELECT next_index, status FROM work_units WHERE id = ?`, w.ID)
	var ni int64
	var st string
	if err := row.Scan(&ni, &st); err != nil {
		return 0, "", fmt.Errorf("read work unit: %w", err)
	}
	return ni, Status(st), nil
}

// WorkUnitStatus is a snapshot of a work unit's processing state.
type WorkUnitStatus struct {
	ID           string
	Status       string
	StartIndex   int64
	EndIndex     int64
	NextIndex    int64
	CertsWritten int64
	SANsWritten  int64
}

// ListWorkUnits returns all tracked work units (processing state per shard).
func (s *Store) ListWorkUnits(ctx context.Context) ([]WorkUnitStatus, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, status, start_index, end_index, next_index, certs_written, sans_written
		FROM work_units ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list work units: %w", err)
	}
	defer rows.Close()
	var out []WorkUnitStatus
	for rows.Next() {
		var w WorkUnitStatus
		if err := rows.Scan(&w.ID, &w.Status, &w.StartIndex, &w.EndIndex, &w.NextIndex, &w.CertsWritten, &w.SANsWritten); err != nil {
			return nil, fmt.Errorf("scan work unit: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// OpenReadOnly opens an existing control database without creating it. It
// returns (nil, nil) if the file does not exist.
func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat control db: %w", err)
	}
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("open control db: %w", err)
	}
	return &Store{db: db}, nil
}

// SetEnd updates a work unit's end_index, used when an active shard grows.
func (s *Store) SetEnd(ctx context.Context, id string, end int64) error {
	return s.exec(ctx, `UPDATE work_units SET end_index=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, end, id)
}

// MarkRunning transitions a work unit to the running state.
func (s *Store) MarkRunning(ctx context.Context, id string) error {
	return s.exec(ctx, `UPDATE work_units SET status='running', error=NULL, updated_at=CURRENT_TIMESTAMP WHERE id=?`, id)
}

// SaveProgress persists the resume cursor and running totals after a batch.
func (s *Store) SaveProgress(ctx context.Context, id string, nextIndex int64, addCerts, addSANs int) error {
	return s.exec(ctx, `
		UPDATE work_units
		SET next_index=?, certs_written=certs_written+?, sans_written=sans_written+?, updated_at=CURRENT_TIMESTAMP
		WHERE id=?`, nextIndex, addCerts, addSANs, id)
}

// MarkDone marks a work unit complete.
func (s *Store) MarkDone(ctx context.Context, id string) error {
	return s.exec(ctx, `UPDATE work_units SET status='done', updated_at=CURRENT_TIMESTAMP WHERE id=?`, id)
}

// MarkFailed records a failure with its error message.
func (s *Store) MarkFailed(ctx context.Context, id, errMsg string) error {
	return s.exec(ctx, `UPDATE work_units SET status='failed', error=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`, errMsg, id)
}

func (s *Store) exec(ctx context.Context, query string, args ...any) error {
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("control exec: %w", err)
	}
	return nil
}

// Now returns the current UTC time; centralised so tests can reason about it.
func Now() time.Time { return time.Now().UTC() }
