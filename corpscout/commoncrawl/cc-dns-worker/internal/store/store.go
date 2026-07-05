// Package store is the durable local stage: an embedded SQLite DB holding the domain work queue
// (via scan_domains.status), the per-domain summary, and the resolved records. It is written by one
// dedicated goroutine (CommitBatch) so SQLite's single-writer lock is never contended, and it makes
// scan resumable — a crash leaves unfinished domains not-'done', which Pending returns.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"cc-dns-worker/internal/model"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite stage.
type Store struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS scan_domains (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'pending',
  etld          TEXT DEFAULT '',
  nameservers   TEXT DEFAULT '[]',
  ns_ips        TEXT DEFAULT '[]',
  dnssec_signed INTEGER DEFAULT 0,
  ds_present    INTEGER DEFAULT 0,
  queries_total INTEGER DEFAULT 0,
  queries_ok    INTEGER DEFAULT 0,
  error         TEXT DEFAULT '',
  resolved_at   TEXT DEFAULT '',
  PRIMARY KEY (scan_id, root_domain)
);
CREATE TABLE IF NOT EXISTS scan_records (
  scan_id      TEXT NOT NULL,
  root_domain  TEXT NOT NULL,
  name         TEXT NOT NULL,
  record_type  TEXT NOT NULL,
  slot         TEXT DEFAULT '',
  value        TEXT NOT NULL,
  ttl          INTEGER DEFAULT 0,
  priority     INTEGER DEFAULT 0,
  rcode        TEXT DEFAULT '',
  resolved_at  TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_records_domain ON scan_records (scan_id, root_domain);
`

// Open opens (creating if needed) the SQLite stage in WAL mode and ensures the schema.
func Open(path string) (*Store, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One writer goroutine calls CommitBatch, but reads may be concurrent; a single open conn keeps
	// writes serialized deterministically.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Seed inserts pending rows for domains, ignoring any already present. Returns rows newly added.
func (s *Store) Seed(ctx context.Context, scanID string, domains []string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO scan_domains (scan_id, root_domain) VALUES (?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	added := 0
	for _, d := range domains {
		r, err := stmt.ExecContext(ctx, scanID, d)
		if err != nil {
			return 0, err
		}
		if n, _ := r.RowsAffected(); n > 0 {
			added++
		}
	}
	return added, tx.Commit()
}

// Pending returns domains for scanID whose status is neither 'done' nor 'error'.
func (s *Store) Pending(ctx context.Context, scanID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT root_domain FROM scan_domains WHERE scan_id = ? AND status NOT IN ('done','error') ORDER BY root_domain`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// CommitBatch writes a batch of results in one transaction, replacing each domain's records so a
// re-commit is idempotent.
func (s *Store) CommitBatch(ctx context.Context, results []model.DomainResult) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	del, err := tx.PrepareContext(ctx, `DELETE FROM scan_records WHERE scan_id = ? AND root_domain = ?`)
	if err != nil {
		return err
	}
	defer del.Close()
	insR, err := tx.PrepareContext(ctx, `INSERT INTO scan_records
		(scan_id, root_domain, name, record_type, slot, value, ttl, priority, rcode, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer insR.Close()
	upD, err := tx.PrepareContext(ctx, `UPDATE scan_domains SET
		status=?, etld=?, nameservers=?, ns_ips=?, dnssec_signed=?, ds_present=?,
		queries_total=?, queries_ok=?, error=?, resolved_at=?
		WHERE scan_id=? AND root_domain=?`)
	if err != nil {
		return err
	}
	defer upD.Close()

	for _, res := range results {
		if _, err := del.ExecContext(ctx, res.ScanID, res.RootDomain); err != nil {
			return err
		}
		ts := res.ResolvedAt.UTC().Format(time.RFC3339Nano)
		for _, rec := range res.Records {
			if _, err := insR.ExecContext(ctx, res.ScanID, res.RootDomain, rec.Name, rec.RecordType,
				rec.Slot, rec.Value, rec.TTL, rec.Priority, rec.Rcode, ts); err != nil {
				return err
			}
		}
		ns, _ := json.Marshal(res.Nameservers)
		nsips, _ := json.Marshal(res.NSIPs)
		if _, err := upD.ExecContext(ctx, res.Status, res.ETLD, string(ns), string(nsips),
			b2i(res.DNSSECSigned), b2i(res.DSPresent), res.QueriesTotal, res.QueriesOK,
			res.Error, ts, res.ScanID, res.RootDomain); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// StagedRecords reads the record stage for a scan into CH RecordRow shape.
func (s *Store) StagedRecords(ctx context.Context, scanID string) ([]model.RecordRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scan_id, root_domain, name, record_type, slot, value,
		ttl, priority, rcode, resolved_at FROM scan_records WHERE scan_id = ?`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RecordRow
	for rows.Next() {
		var r model.RecordRow
		var ts string
		if err := rows.Scan(&r.ScanID, &r.RootDomain, &r.Name, &r.RecordType, &r.Slot, &r.Value,
			&r.TTL, &r.Priority, &r.Rcode, &ts); err != nil {
			return nil, err
		}
		r.ResolvedAt = parseTS(ts)
		r.SourceRunID = scanID
		out = append(out, r)
	}
	return out, rows.Err()
}

// StagedDomains reads finished domain summaries for a scan into CH ScanRow shape.
func (s *Store) StagedDomains(ctx context.Context, scanID string) ([]model.ScanRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scan_id, root_domain, etld, nameservers, ns_ips,
		dnssec_signed, ds_present, status, error, queries_total, queries_ok, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status IN ('done','error')`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ScanRow
	for rows.Next() {
		var r model.ScanRow
		var ns, nsips, ts string
		var dnssec, ds int
		if err := rows.Scan(&r.ScanID, &r.RootDomain, &r.ETLD, &ns, &nsips, &dnssec, &ds,
			&r.Status, &r.Error, &r.QueriesTotal, &r.QueriesOK, &ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(ns), &r.Nameservers)
		_ = json.Unmarshal([]byte(nsips), &r.NSIPs)
		r.DNSSECSigned = uint8(dnssec)
		r.DSPresent = uint8(ds)
		r.ResolvedAt = parseTS(ts)
		r.SourceRunID = scanID
		out = append(out, r)
	}
	return out, rows.Err()
}

// Close closes the DB.
func (s *Store) Close() error { return s.db.Close() }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func parseTS(s string) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC()
	}
	return time.Unix(0, 0).UTC()
}
