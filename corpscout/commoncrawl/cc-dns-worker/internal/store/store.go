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
	"strings"
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
  axfr_open     INTEGER DEFAULT 0,
  axfr_records  INTEGER DEFAULT 0,
  axfr_truncated INTEGER DEFAULT 0,
  axfr_server    TEXT DEFAULT '',
  error         TEXT DEFAULT '',
  source_run_id TEXT DEFAULT '',
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
  source       TEXT DEFAULT 'query',
  discovery    TEXT DEFAULT 'static',
  rcode        TEXT DEFAULT '',
  source_run_id TEXT DEFAULT '',
  resolved_at  TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_records_domain ON scan_records (scan_id, root_domain);
CREATE TABLE IF NOT EXISTS scan_meta (
  scan_id       TEXT PRIMARY KEY,
  seed_complete INTEGER NOT NULL DEFAULT 0,
  seeded_at     TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS load_state (
  scan_id      TEXT PRIMARY KEY,
  loaded_rowid INTEGER NOT NULL DEFAULT 0
);
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
	migrate(db)
	return &Store{db: db}, nil
}

// migrate applies additive column changes to stage DBs created before those columns existed.
// SQLite has no ADD COLUMN IF NOT EXISTS, so a duplicate-column error is expected and ignored.
func migrate(db *sql.DB) {
	for _, stmt := range []string{
		`ALTER TABLE scan_records ADD COLUMN source TEXT DEFAULT 'query'`,
		`ALTER TABLE scan_records ADD COLUMN discovery TEXT DEFAULT 'static'`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_open INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_records INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_truncated INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_server TEXT DEFAULT ''`,
	} {
		_, _ = db.ExecContext(context.Background(), stmt)
	}
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

// SeedComplete reports whether scanID's seed already finished, so a restart can skip re-streaming
// the whole domain list from ClickHouse and resume straight from the SQLite queue. It returns false
// if the seed never ran or was interrupted mid-stream (the marker is only set after a full seed).
func (s *Store) SeedComplete(ctx context.Context, scanID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT seed_complete FROM scan_meta WHERE scan_id = ?`, scanID).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// MarkSeedComplete records that scanID has been fully seeded. Call it only after the seed stream
// finishes without error; subsequent runs then skip the ClickHouse re-stream.
func (s *Store) MarkSeedComplete(ctx context.Context, scanID string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO scan_meta (scan_id, seed_complete, seeded_at) VALUES (?, 1, ?)
		 ON CONFLICT(scan_id) DO UPDATE SET seed_complete = 1, seeded_at = excluded.seeded_at`,
		scanID, time.Now().UTC().Format(time.RFC3339Nano))
	return err
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

// PendingBatch returns up to limit not-yet-terminal domains for scanID whose root_domain is greater
// than afterRootDomain, ordered by root_domain. afterRootDomain="" starts at the beginning.
// Cursor-paginating on the (scan_id, root_domain) primary key keeps streaming dispatch ~O(n) instead
// of re-walking the growing done/error prefix on every call.
func (s *Store) PendingBatch(ctx context.Context, scanID, afterRootDomain string, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT root_domain FROM scan_domains
		 WHERE scan_id = ? AND root_domain > ? AND status NOT IN ('done','error')
		 ORDER BY root_domain LIMIT ?`, scanID, afterRootDomain, limit)
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
		(scan_id, root_domain, name, record_type, slot, value, ttl, priority, rcode, source, discovery, source_run_id, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer insR.Close()
	upD, err := tx.PrepareContext(ctx, `UPDATE scan_domains SET
		status=?, etld=?, nameservers=?, ns_ips=?, dnssec_signed=?, ds_present=?,
		queries_total=?, queries_ok=?, axfr_open=?, axfr_records=?, axfr_truncated=?, axfr_server=?, error=?, source_run_id=?, resolved_at=?
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
				rec.Slot, rec.Value, rec.TTL, rec.Priority, rec.Rcode, rec.Source, rec.Discovery, res.SourceRunID, ts); err != nil {
				return err
			}
		}
		ns, _ := json.Marshal(res.Nameservers)
		nsips, _ := json.Marshal(res.NSIPs)
		res2, err := upD.ExecContext(ctx, res.Status, res.ETLD, string(ns), string(nsips),
			b2i(res.DNSSECSigned), b2i(res.DSPresent), res.QueriesTotal, res.QueriesOK,
			b2i(res.AXFROpen), res.AXFRRecords, b2i(res.AXFRTruncated), res.AXFRServer,
			res.Error, res.SourceRunID, ts, res.ScanID, res.RootDomain)
		if err != nil {
			return err
		}
		if n, err := res2.RowsAffected(); err != nil {
			return err
		} else if n != 1 {
			return fmt.Errorf("commit domain %q: %d rows updated (not seeded?)", res.RootDomain, n)
		}
	}
	return tx.Commit()
}

// StagedRecords reads the record stage for a scan into the distinct-model RecordRow shape. Each row
// carries first_seen = last_seen = resolved_at and scans = 1; ClickHouse's AggregatingMergeTree folds
// them into the record's lifespan on merge.
func (s *Store) StagedRecords(ctx context.Context, scanID string) ([]model.RecordRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, name, record_type, slot, value,
		ttl, priority, rcode, source, discovery, source_run_id, resolved_at FROM scan_records WHERE scan_id = ?`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RecordRow
	for rows.Next() {
		var r model.RecordRow
		var ts string
		if err := rows.Scan(&r.RootDomain, &r.Name, &r.RecordType, &r.Slot, &r.Value,
			&r.TTL, &r.Priority, &r.Rcode, &r.Source, &r.Discovery, &r.LastRunID, &ts); err != nil {
			return nil, err
		}
		t := parseTS(ts)
		r.FirstSeen, r.LastSeen, r.Scans = t, t, 1
		out = append(out, r)
	}
	return out, rows.Err()
}

// StagedDomains reads the per-domain summaries for a scan into ScanRow shape. Only status='done'
// domains are returned: a failed re-scan must not clobber a domain's last-good summary, and a domain
// that never resolves has no DNS state to record.
func (s *Store) StagedDomains(ctx context.Context, scanID string) ([]model.ScanRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, etld, nameservers, ns_ips,
		dnssec_signed, ds_present, status, queries_total, queries_ok, axfr_open, axfr_records, axfr_truncated, axfr_server,
		source_run_id, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status = 'done'`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ScanRow
	for rows.Next() {
		var r model.ScanRow
		var ns, nsips, ts string
		var dnssec, ds, axfrOpen, axfrTrunc int
		var axfrRecs uint32
		if err := rows.Scan(&r.RootDomain, &r.ETLD, &ns, &nsips, &dnssec, &ds,
			&r.Status, &r.QueriesTotal, &r.QueriesOK, &axfrOpen, &axfrRecs, &axfrTrunc, &r.AXFRServer, &r.LastRunID, &ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(ns), &r.Nameservers)
		_ = json.Unmarshal([]byte(nsips), &r.NSIPs)
		r.DNSSECSigned = uint8(dnssec)
		r.DSPresent = uint8(ds)
		r.AXFROpen = uint8(axfrOpen)
		r.AXFRRecords = axfrRecs
		r.AXFRTruncated = uint8(axfrTrunc)
		r.ResolvedAt = parseTS(ts)
		out = append(out, r)
	}
	return out, rows.Err()
}

// PendingCount returns how many domains for scanID are still not terminal (used to detect a finished
// scan: 0 means every domain reached done/error).
func (s *Store) PendingCount(ctx context.Context, scanID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM scan_domains WHERE scan_id = ? AND status NOT IN ('done','error')`, scanID).Scan(&n)
	return n, err
}

// LoadedRowid returns the highest scan_records rowid already loaded to ClickHouse for scanID (0 if
// none). The incremental loader reads records with rowid greater than this.
func (s *Store) LoadedRowid(ctx context.Context, scanID string) (int64, error) {
	var rid int64
	err := s.db.QueryRowContext(ctx, `SELECT loaded_rowid FROM load_state WHERE scan_id = ?`, scanID).Scan(&rid)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return rid, err
}

// SetLoadedRowid advances the loaded-up-to watermark for scanID.
func (s *Store) SetLoadedRowid(ctx context.Context, scanID string, rowid int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO load_state (scan_id, loaded_rowid) VALUES (?, ?)
		 ON CONFLICT(scan_id) DO UPDATE SET loaded_rowid = excluded.loaded_rowid`, scanID, rowid)
	return err
}

// The unary + on scan_id stops the planner from picking idx_records_domain (which would re-sort
// the scan's entire record set per batch); the query must walk the rowid primary key instead.
const recordsAfterQuery = `SELECT rowid, root_domain, name, record_type, slot, value,
	ttl, priority, rcode, source, discovery, source_run_id, resolved_at FROM scan_records
	WHERE +scan_id = ? AND rowid > ? ORDER BY rowid LIMIT ?`

// RecordsAfter returns up to limit records for scanID with rowid > afterRowid, ordered by rowid (i.e.
// commit order, which is monotonic even for late-finishing domains). It also returns the max rowid in
// the batch (the new watermark) and the distinct root_domains touched (so the caller can load their
// summaries). A short batch (< limit) means the loader has caught up.
func (s *Store) RecordsAfter(ctx context.Context, scanID string, afterRowid int64, limit int) ([]model.RecordRow, int64, []string, error) {
	rows, err := s.db.QueryContext(ctx, recordsAfterQuery, scanID, afterRowid, limit)
	if err != nil {
		return nil, afterRowid, nil, err
	}
	defer rows.Close()
	var out []model.RecordRow
	var domains []string
	seen := map[string]bool{}
	maxRowid := afterRowid
	for rows.Next() {
		var r model.RecordRow
		var rid int64
		var ts string
		if err := rows.Scan(&rid, &r.RootDomain, &r.Name, &r.RecordType, &r.Slot, &r.Value,
			&r.TTL, &r.Priority, &r.Rcode, &r.Source, &r.Discovery, &r.LastRunID, &ts); err != nil {
			return nil, afterRowid, nil, err
		}
		t := parseTS(ts)
		r.FirstSeen, r.LastSeen, r.Scans = t, t, 1
		out = append(out, r)
		if rid > maxRowid {
			maxRowid = rid
		}
		if !seen[r.RootDomain] {
			seen[r.RootDomain] = true
			domains = append(domains, r.RootDomain)
		}
	}
	return out, maxRowid, domains, rows.Err()
}

// SummariesFor returns the done-summary rows for the given domains (skips non-'done'). Used by the
// incremental loader to load summaries alongside a batch of records.
func (s *Store) SummariesFor(ctx context.Context, scanID string, domains []string) ([]model.ScanRow, error) {
	if len(domains) == 0 {
		return nil, nil
	}
	ph := strings.Repeat("?,", len(domains))
	ph = ph[:len(ph)-1]
	args := make([]any, 0, len(domains)+1)
	args = append(args, scanID)
	for _, d := range domains {
		args = append(args, d)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, etld, nameservers, ns_ips,
		dnssec_signed, ds_present, status, queries_total, queries_ok, axfr_open, axfr_records, axfr_truncated, axfr_server,
		source_run_id, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status = 'done' AND root_domain IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ScanRow
	for rows.Next() {
		var r model.ScanRow
		var ns, nsips, ts string
		var dnssec, ds, axfrOpen, axfrTrunc int
		var axfrRecs uint32
		if err := rows.Scan(&r.RootDomain, &r.ETLD, &ns, &nsips, &dnssec, &ds,
			&r.Status, &r.QueriesTotal, &r.QueriesOK, &axfrOpen, &axfrRecs, &axfrTrunc, &r.AXFRServer, &r.LastRunID, &ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(ns), &r.Nameservers)
		_ = json.Unmarshal([]byte(nsips), &r.NSIPs)
		r.DNSSECSigned = uint8(dnssec)
		r.DSPresent = uint8(ds)
		r.AXFROpen = uint8(axfrOpen)
		r.AXFRRecords = axfrRecs
		r.AXFRTruncated = uint8(axfrTrunc)
		r.ResolvedAt = parseTS(ts)
		out = append(out, r)
	}
	return out, rows.Err()
}

// DiscoveredHostnames returns the distinct non-static hosts (discovery in ct/axfr) that resolved this
// scan, one per (root_domain, label), for the registry write-back. label = name minus ".<root_domain>";
// apex and non-subdomain names are skipped. Timestamps are left zero for the caller to stamp.
func (s *Store) DiscoveredHostnames(ctx context.Context, scanID string) ([]model.HostnameRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT root_domain, name, discovery
		FROM scan_records
		WHERE scan_id = ? AND discovery IN ('ct','axfr') AND record_type IN ('A','AAAA','CNAME')`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var out []model.HostnameRow
	for rows.Next() {
		var rd, name, disc string
		if err := rows.Scan(&rd, &name, &disc); err != nil {
			return nil, err
		}
		suffix := "." + rd
		if name == rd || !strings.HasSuffix(name, suffix) {
			continue // apex or not a subdomain of rd
		}
		label := strings.ToLower(strings.TrimSuffix(name, suffix))
		if label == "" {
			continue
		}
		key := rd + "\x00" + label
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, model.HostnameRow{RootDomain: rd, Label: label, DiscoverySource: disc})
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
