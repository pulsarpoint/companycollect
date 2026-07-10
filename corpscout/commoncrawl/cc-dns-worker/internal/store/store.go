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
	"sort"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"

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
  ns_endpoints  TEXT DEFAULT '[]',
  dnssec_signed INTEGER DEFAULT 0,
  ds_present    INTEGER DEFAULT 0,
  queries_total INTEGER DEFAULT 0,
  queries_ok    INTEGER DEFAULT 0,
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
CREATE TABLE IF NOT EXISTS scan_hostnames (
  scan_id          TEXT NOT NULL,
  root_domain      TEXT NOT NULL,
  label            TEXT NOT NULL,
  discovery_source TEXT NOT NULL DEFAULT 'ct',
  live_cert        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scan_id, root_domain, label)
);
CREATE TABLE IF NOT EXISTS host_load_state (
  scan_id  TEXT PRIMARY KEY,
  cursor   TEXT NOT NULL DEFAULT '',
  complete INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS host_load_shards (
  scan_id  TEXT NOT NULL,
  shard    INTEGER NOT NULL,
  complete INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scan_id, shard)
);
CREATE TABLE IF NOT EXISTS axfr_domains (
  scan_id     TEXT NOT NULL,
  root_domain TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',
  error       TEXT DEFAULT '',
  observed_at TEXT DEFAULT '',
  PRIMARY KEY (scan_id, root_domain)
);
CREATE INDEX IF NOT EXISTS idx_axfr_domains_pending ON axfr_domains (scan_id, status, root_domain);
CREATE TABLE IF NOT EXISTS axfr_probes (
  scan_id        TEXT NOT NULL,
  root_domain    TEXT NOT NULL,
  name_server    TEXT NOT NULL,
  name_server_ip TEXT NOT NULL,
  verdict        TEXT NOT NULL DEFAULT '',
  reason         TEXT NOT NULL DEFAULT '',
  records        INTEGER DEFAULT 0,
  bytes          INTEGER DEFAULT 0,
  truncated      INTEGER DEFAULT 0,
  observed_at    TEXT DEFAULT '',
  PRIMARY KEY (scan_id, root_domain, name_server, name_server_ip)
);
CREATE TABLE IF NOT EXISTS axfr_changes (
  scan_id        TEXT NOT NULL,
  root_domain    TEXT NOT NULL,
  name_server    TEXT NOT NULL,
  name_server_ip TEXT NOT NULL,
  axfr_open      INTEGER NOT NULL DEFAULT 0,
  changed_at     TEXT NOT NULL,
  PRIMARY KEY (scan_id, root_domain, name_server, name_server_ip, changed_at)
);
-- axfr_load_state will carry the ClickHouse-load watermark/marker for axfr_probes/axfr_changes (Task 5);
-- created now so its ownership lives with the rest of the AXFR schema. Empty/unused by Task 4.
CREATE TABLE IF NOT EXISTS axfr_load_state (
  scan_id  TEXT PRIMARY KEY,
  complete INTEGER NOT NULL DEFAULT 0
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
//
// The four axfr_* ADD COLUMNs are kept — and so still apply to a brand-new DB too, since schema/migrate
// always run back to back in Open — purely to preserve the physical scan_domains.axfr_* columns for
// anything still reading them at the SQLite layer. No Go code in this package reads or writes them
// anymore: they are DEPRECATED in favor of the durable axfr_domains/axfr_probes queue and the
// dns_axfr_latest/dns_axfr_state_changes ClickHouse tables (see store.SeedAXFRDomains and
// internal/load.LoadAXFR).
func migrate(db *sql.DB) {
	for _, stmt := range []string{
		`ALTER TABLE scan_records ADD COLUMN source TEXT DEFAULT 'query'`,
		`ALTER TABLE scan_records ADD COLUMN discovery TEXT DEFAULT 'static'`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_open INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_records INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_truncated INTEGER DEFAULT 0`,
		`ALTER TABLE scan_domains ADD COLUMN axfr_server TEXT DEFAULT ''`,
		`ALTER TABLE scan_domains ADD COLUMN ns_endpoints TEXT DEFAULT '[]'`,
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
		status=?, etld=?, nameservers=?, ns_ips=?, ns_endpoints=?, dnssec_signed=?, ds_present=?,
		queries_total=?, queries_ok=?, error=?, source_run_id=?, resolved_at=?
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
		nsEndpoints, _ := json.Marshal(res.Endpoints)
		res2, err := upD.ExecContext(ctx, res.Status, res.ETLD, string(ns), string(nsips), string(nsEndpoints),
			b2i(res.DNSSECSigned), b2i(res.DSPresent), res.QueriesTotal, res.QueriesOK,
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
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, etld, nameservers, ns_ips, ns_endpoints,
		dnssec_signed, ds_present, status, queries_total, queries_ok,
		source_run_id, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status = 'done'`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ScanRow
	for rows.Next() {
		var r model.ScanRow
		var ns, nsips, nsEndpoints, ts string
		var dnssec, ds int
		if err := rows.Scan(&r.RootDomain, &r.ETLD, &ns, &nsips, &nsEndpoints, &dnssec, &ds,
			&r.Status, &r.QueriesTotal, &r.QueriesOK, &r.LastRunID, &ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(ns), &r.Nameservers)
		_ = json.Unmarshal([]byte(nsips), &r.NSIPs)
		_ = json.Unmarshal([]byte(nsEndpoints), &r.Endpoints)
		r.DNSSECSigned = uint8(dnssec)
		r.DSPresent = uint8(ds)
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
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, etld, nameservers, ns_ips, ns_endpoints,
		dnssec_signed, ds_present, status, queries_total, queries_ok,
		source_run_id, resolved_at
		FROM scan_domains WHERE scan_id = ? AND status = 'done' AND root_domain IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ScanRow
	for rows.Next() {
		var r model.ScanRow
		var ns, nsips, nsEndpoints, ts string
		var dnssec, ds int
		if err := rows.Scan(&r.RootDomain, &r.ETLD, &ns, &nsips, &nsEndpoints, &dnssec, &ds,
			&r.Status, &r.QueriesTotal, &r.QueriesOK, &r.LastRunID, &ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(ns), &r.Nameservers)
		_ = json.Unmarshal([]byte(nsips), &r.NSIPs)
		_ = json.Unmarshal([]byte(nsEndpoints), &r.Endpoints)
		r.DNSSECSigned = uint8(dnssec)
		r.DSPresent = uint8(ds)
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
		if label == "" || strings.Contains(label, "*") {
			continue // empty or wildcard (e.g. "*.example.com") — not a durable host
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

// HostnamesForBatch bulk-loads the labels for a set of domains in one query (the feeder calls it once
// per dispatch batch, not per worker) → map[root_domain][]HostLabel.
func (s *Store) HostnamesForBatch(ctx context.Context, scanID string, domains []string) (map[string][]model.HostLabel, error) {
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
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, label, discovery_source, live_cert
		FROM scan_hostnames WHERE scan_id = ? AND root_domain IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]model.HostLabel{}
	for rows.Next() {
		var rd string
		var h model.HostLabel
		var lc int
		if err := rows.Scan(&rd, &h.Label, &h.DiscoverySource, &lc); err != nil {
			return nil, err
		}
		h.LiveCert = lc != 0
		out[rd] = append(out[rd], h)
	}
	return out, rows.Err()
}

// HostLoadComplete reports whether the host-load phase finished for scanID (resume skips it).
func (s *Store) HostLoadComplete(ctx context.Context, scanID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT complete FROM host_load_state WHERE scan_id = ?`, scanID).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return n == 1, err
}

// MarkHostLoadComplete records that the host-load phase finished for scanID.
func (s *Store) MarkHostLoadComplete(ctx context.Context, scanID string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO host_load_state (scan_id, complete) VALUES (?, 1)
		 ON CONFLICT(scan_id) DO UPDATE SET complete = 1`, scanID)
	return err
}

// HostLoadShardComplete reports whether host-load shard finished for scanID (a restart re-runs only
// the shards not yet marked complete; re-running a shard is idempotent via INSERT OR IGNORE).
func (s *Store) HostLoadShardComplete(ctx context.Context, scanID string, shard int) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT complete FROM host_load_shards WHERE scan_id = ? AND shard = ?`, scanID, shard).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return n == 1, err
}

// MarkHostLoadShardComplete records that host-load shard finished for scanID.
func (s *Store) MarkHostLoadShardComplete(ctx context.Context, scanID string, shard int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO host_load_shards (scan_id, shard, complete) VALUES (?, ?, 1)
		 ON CONFLICT(scan_id, shard) DO UPDATE SET complete = 1`, scanID, shard)
	return err
}

// InsertHostnamesMap bulk-inserts a merged domain→labels map for scanID in one transaction (idempotent
// — INSERT OR IGNORE on the (scan_id, root_domain, label) primary key). Returns rows newly inserted.
// Callers flush per shard, so each shard's ~millions of rows land in a single commit.
func (s *Store) InsertHostnamesMap(ctx context.Context, scanID string, m map[string][]model.HostLabel) (int, error) {
	if len(m) == 0 {
		return 0, nil
	}
	// Insert in (root_domain, label) key order so the scan_hostnames primary-key B-tree grows by
	// mostly-sequential appends instead of thrashing random pages — the dominant cost when a shard
	// commits millions of rows on the pure-Go SQLite driver, and it worsens as the index grows.
	domains := make([]string, 0, len(m))
	for rd := range m {
		domains = append(domains, rd)
	}
	sort.Strings(domains)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO scan_hostnames
		(scan_id, root_domain, label, discovery_source, live_cert) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	added := 0
	for _, rd := range domains {
		hosts := m[rd]
		sort.Slice(hosts, func(i, j int) bool { return hosts[i].Label < hosts[j].Label })
		for _, h := range hosts {
			r, err := stmt.ExecContext(ctx, scanID, rd, h.Label, h.DiscoverySource, b2i(h.LiveCert))
			if err != nil {
				return 0, err
			}
			if n, _ := r.RowsAffected(); n > 0 {
				added++
			}
		}
	}
	return added, tx.Commit()
}

// AXFRTarget is one domain the AXFR pipeline will probe: its root and the NS endpoints resolution
// already discovered and stored (so the pipeline needs no NS re-discovery). Endpoints preserves which
// NS hostname each IP belongs to and each IP's dialability, so the AXFR phase can skip a non-dialable
// address exactly like Tier-2 record queries do (resolve.Resolver.queryAuth's defense-in-depth check).
type AXFRTarget struct {
	RootDomain string
	Endpoints  []model.NameserverEndpoint
}

// SeedAXFRDomains idempotently stages every resolved domain (scan_domains.status='done') with a
// non-empty stored ns_endpoints or ns_ips into the durable axfr_domains work queue, status='pending'.
// INSERT OR IGNORE means an already-present row — pending OR done — is never touched, so re-running the
// seed mid-scan (or after a crash) only ever adds newly-resolved domains and can never reset a finished
// one back to pending. This queue, not a positional cursor, is what makes the AXFR phase resumable: the
// durable position is which rows are 'done', not where a feeder happened to be when the process died.
func (s *Store) SeedAXFRDomains(ctx context.Context, scanID string) (int, error) {
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO axfr_domains (scan_id, root_domain, status)
		SELECT scan_id, root_domain, 'pending' FROM scan_domains
		WHERE scan_id = ? AND status = 'done'
		AND (ns_endpoints NOT IN ('', 'null', '[]') OR ns_ips NOT IN ('', 'null', '[]'))`, scanID)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// PendingAXFRDomains returns up to limit axfr_domains rows for scanID still status='pending', with
// root_domain greater than afterRootDomain, joined to scan_domains for the stored NS endpoints.
// afterRootDomain paginates a SINGLE run's feeder loop forward (mirrors PendingBatch) purely so it
// doesn't re-scan the growing done-prefix on every fetch — it is held in memory only, by the caller, and
// is NEVER persisted. Resume therefore never depends on it: a crash loses only this in-memory position,
// and the next call with afterRootDomain="" reads every row still 'pending', including ones a killed run
// dispatched to a worker but never committed. Rows fall back to ns_ips (reclassified via
// resolve.ClassifyString, so dialability is still correct) when ns_endpoints was never populated.
func (s *Store) PendingAXFRDomains(ctx context.Context, scanID, afterRootDomain string, limit int) ([]AXFRTarget, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT ad.root_domain, sd.ns_endpoints, sd.ns_ips FROM axfr_domains ad
		 JOIN scan_domains sd ON sd.scan_id = ad.scan_id AND sd.root_domain = ad.root_domain
		 WHERE ad.scan_id = ? AND ad.status = 'pending' AND ad.root_domain > ?
		 ORDER BY ad.root_domain LIMIT ?`, scanID, afterRootDomain, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AXFRTarget
	for rows.Next() {
		var rd, epJSON, ipsJSON string
		if err := rows.Scan(&rd, &epJSON, &ipsJSON); err != nil {
			return nil, err
		}
		var eps []model.NameserverEndpoint
		_ = json.Unmarshal([]byte(epJSON), &eps)
		if len(eps) == 0 {
			eps = endpointsFromLegacyIPs(ipsJSON)
		}
		if len(eps) == 0 {
			continue
		}
		out = append(out, AXFRTarget{RootDomain: rd, Endpoints: eps})
	}
	return out, rows.Err()
}

// endpointsFromLegacyIPs synthesizes endpoints from a pre-ns_endpoints ns_ips column: the NS hostname
// each IP belonged to was never recorded (Name is left blank), but Scope/Dialable are reclassified from
// the IP itself so an old row can never look more dialable than it really is.
func endpointsFromLegacyIPs(ipsJSON string) []model.NameserverEndpoint {
	var ips []string
	if err := json.Unmarshal([]byte(ipsJSON), &ips); err != nil || len(ips) == 0 {
		return nil
	}
	out := make([]model.NameserverEndpoint, 0, len(ips))
	for _, ip := range ips {
		ep := model.NameserverEndpoint{IP: ip}
		if scope, ok := resolve.ClassifyString(ip); ok {
			ep.Scope = string(scope)
			ep.Dialable = resolve.Dialable(scope)
		}
		out = append(out, ep)
	}
	return out
}

// CommitAXFRDomain durably records ALL of one domain's probed endpoint outcomes plus its selected open
// zone's records, and marks the domain done — all in ONE transaction. Staged first, checkpointed second:
// a crash any time before this call leaves the domain 'pending' (PendingAXFRDomains returns it again on
// resume, so the next run re-probes it — REPEATS work, never SKIPS it), and a failure partway through
// this call rolls back everything it touched, so a partial probe pass never leaves stray axfr_probes or
// scan_records rows behind for a domain that's still 'pending'. Re-running a domain (resume, or a
// deliberate re-probe) is idempotent: this domain's prior axfr_probes rows and its axfr-sourced
// scan_records rows are replaced wholesale, never unioned with a past attempt — the same replace
// semantics CommitBatch and the removed RecordAXFRZone used. errMsg is a free-form note the caller may
// attach (empty on the normal path); it never gates whether the domain is marked done — axfr_domains has
// no separate error/retry state, only pending|done.
func (s *Store) CommitAXFRDomain(ctx context.Context, scanID, domain string, probes []resolve.AXFROutcome, zone []model.DNSRecord, runID string, observedAt time.Time, errMsg string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM axfr_probes WHERE scan_id = ? AND root_domain = ?`, scanID, domain); err != nil {
		return err
	}
	if len(probes) > 0 {
		insP, err := tx.PrepareContext(ctx, `INSERT INTO axfr_probes
			(scan_id, root_domain, name_server, name_server_ip, verdict, reason, records, bytes, truncated, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		defer insP.Close()
		for _, p := range probes {
			pts := observedAt
			if !p.ObservedAt.IsZero() {
				pts = p.ObservedAt
			}
			if _, err := insP.ExecContext(ctx, scanID, domain, p.NSHost, p.NSIP, string(p.Verdict), string(p.Reason),
				p.Records, p.Bytes, b2i(p.Truncated), pts.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM scan_records WHERE scan_id = ? AND root_domain = ? AND source = 'axfr'`, scanID, domain); err != nil {
		return err
	}
	if len(zone) > 0 {
		insR, err := tx.PrepareContext(ctx, `INSERT INTO scan_records
			(scan_id, root_domain, name, record_type, slot, value, ttl, priority, rcode, source, discovery, source_run_id, resolved_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		defer insR.Close()
		ts := observedAt.UTC().Format(time.RFC3339Nano)
		for _, rec := range zone {
			if _, err := insR.ExecContext(ctx, scanID, domain, rec.Name, rec.RecordType, rec.Slot,
				rec.Value, rec.TTL, rec.Priority, rec.Rcode, rec.Source, rec.Discovery, runID, ts); err != nil {
				return err
			}
		}
	}

	res, err := tx.ExecContext(ctx, `UPDATE axfr_domains SET status = 'done', error = ?, observed_at = ?
		WHERE scan_id = ? AND root_domain = ?`, errMsg, observedAt.UTC().Format(time.RFC3339Nano), scanID, domain)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n != 1 {
		return fmt.Errorf("commit axfr domain %q: %d rows updated (not in axfr_domains queue?)", domain, n)
	}
	return tx.Commit()
}

// AXFRQueueComplete reports whether every axfr_domains row for scanID has reached status='done'. This
// IS the AXFR phase's completion signal — no separate marker exists to fall out of sync with the queue.
// A scan-id that was never seeded (zero rows) also reads as complete, so an --axfr run with no eligible
// domains is a legitimate no-op rather than an error.
func (s *Store) AXFRQueueComplete(ctx context.Context, scanID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM axfr_domains WHERE scan_id = ? AND status = 'pending'`, scanID).Scan(&n)
	return n == 0, err
}

// HasAXFRWork reports whether scanID has ANY axfr_domains rows at all — i.e., whether SeedAXFRDomains
// ever staged the AXFR phase for this scan (--axfr was enabled and at least one resolved domain had a
// non-empty stored NS endpoint/IP set). Callers use this to skip the AXFR ClickHouse load pass
// (load.LoadAXFR) entirely — no ClickHouse round trip at all — when AXFR was disabled or never reached
// this scan-id, rather than relying on LoadAXFR's own internal early-out (which still reads
// dns_axfr_latest from ClickHouse before finding there is nothing staged to load).
func (s *Store) HasAXFRWork(ctx context.Context, scanID string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM axfr_domains WHERE scan_id = ?)`, scanID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// AXFREndpointKey identifies one AXFR-probed endpoint — an authoritative nameserver hostname/IP pair
// under one root domain — the join key shared across axfr_probes, dns_axfr_latest, and
// dns_axfr_state_changes.
type AXFREndpointKey struct {
	RootDomain   string
	NameServer   string
	NameServerIP string
}

// AXFRPriorState is one endpoint's state as last recorded in ClickHouse's dns_axfr_latest (loaded via
// load.LoadAXFRPrior before this scan's changes are staged). The zero value correctly represents an
// endpoint never seen before: HasDefinitive/DelegationActive false, every timestamp zero.
type AXFRPriorState struct {
	HasDefinitive    bool // true once any past probe reached a definitive (open/closed) verdict
	AXFROpen         bool // meaningful only when HasDefinitive is true
	DefinitiveAt     time.Time
	DefinitiveScanID string
	LastProbeVerdict string // most recent probe of ANY kind, including unknown
	LastProbeReason  string
	LastProbedAt     time.Time
	DelegationActive bool // whether the endpoint was active as of the last latest-row write
	DelegationSeenAt time.Time
}

// AXFRProbedEndpoint is one endpoint the AXFR load pass considers for scanID's dns_axfr_latest rows.
// Verdict is non-empty only when a fresh probe landed this scan (Reason/ObservedAt then describe it);
// an empty Verdict means either the endpoint is still delegated but wasn't dialed this scan (e.g. it
// became non-dialable) or it dropped out of the domain's delegation entirely — DelegationActive tells
// these two apart. In both empty-Verdict cases the loader carries the endpoint's AXFRPriorState forward
// untouched except for DelegationActive/DelegationSeenAt.
type AXFRProbedEndpoint struct {
	AXFREndpointKey
	Verdict          string
	Reason           string
	ObservedAt       time.Time
	Definitive       bool
	DelegationActive bool
}

// StageAXFRChanges computes this scan's DEFINITIVE AXFR state changes against prior (the last known
// definitive state per endpoint — see AXFRPriorState, loaded from ClickHouse by load.LoadAXFRPrior) and
// inserts them into the durable axfr_changes stage. An unknown probe never produces a change and is
// never compared against prior. A previously-unseen endpoint (no prior definitive state) writes an
// initial change only when this probe is definitively OPEN — closed is the assumed baseline for an
// endpoint the pipeline has never proven open, so a first-ever closed observation is not a change.
// Every later definitive flip (open->closed or closed->open, including back to a domain's own baseline)
// IS recorded. INSERT OR IGNORE on axfr_changes' full-row primary key (which includes changed_at, the
// probe's own observed_at) makes re-staging the same scan's probes a no-op.
func (s *Store) StageAXFRChanges(ctx context.Context, scanID string, prior map[AXFREndpointKey]AXFRPriorState) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, name_server, name_server_ip, verdict, observed_at
		FROM axfr_probes WHERE scan_id = ?`, scanID)
	if err != nil {
		return 0, err
	}
	type staged struct {
		key       AXFREndpointKey
		axfrOpen  bool
		changedAt string
	}
	var changes []staged
	for rows.Next() {
		var rd, ns, ip, verdict, ts string
		if err := rows.Scan(&rd, &ns, &ip, &verdict, &ts); err != nil {
			rows.Close()
			return 0, err
		}
		if verdict != string(resolve.VerdictOpen) && verdict != string(resolve.VerdictClosed) {
			continue // unknown: never a change, never compared against prior
		}
		open := verdict == string(resolve.VerdictOpen)
		key := AXFREndpointKey{RootDomain: rd, NameServer: ns, NameServerIP: ip}
		if p := prior[key]; p.HasDefinitive {
			if p.AXFROpen == open {
				continue // no flip from the prior definitive state
			}
		} else if !open {
			continue // no prior definitive state: a first-ever closed observation is the baseline
		}
		changes = append(changes, staged{key, open, ts})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(changes) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO axfr_changes
		(scan_id, root_domain, name_server, name_server_ip, axfr_open, changed_at) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	added := 0
	for _, c := range changes {
		r, err := stmt.ExecContext(ctx, scanID, c.key.RootDomain, c.key.NameServer, c.key.NameServerIP, b2i(c.axfrOpen), c.changedAt)
		if err != nil {
			return 0, err
		}
		if n, _ := r.RowsAffected(); n > 0 {
			added++
		}
	}
	return added, tx.Commit()
}

// StagedAXFRChanges reads scanID's staged axfr_changes rows into the ClickHouse row shape, for
// load.WriteAXFRStateChanges to insert into dns_axfr_state_changes.
func (s *Store) StagedAXFRChanges(ctx context.Context, scanID string) ([]model.AXFRStateChangeRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, name_server, name_server_ip, axfr_open, changed_at
		FROM axfr_changes WHERE scan_id = ?`, scanID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.AXFRStateChangeRow
	for rows.Next() {
		var rd, ns, ip, ts string
		var open int
		if err := rows.Scan(&rd, &ns, &ip, &open, &ts); err != nil {
			return nil, err
		}
		out = append(out, model.AXFRStateChangeRow{
			RootDomain: rd, NameServer: ns, NameServerIP: ip,
			AXFROpen: uint8(open), ScanID: scanID, ChangedAt: parseTS(ts),
		})
	}
	return out, rows.Err()
}

// AXFRLoadRows returns, for every domain the AXFR phase finished (axfr_domains.status='done') in
// scanID, one AXFRProbedEndpoint per endpoint worth a dns_axfr_latest update:
//   - every endpoint actually probed this scan (from axfr_probes) — Verdict set, DelegationActive true;
//   - every OTHER endpoint still in the domain's current delegation (scan_domains.ns_endpoints) that has
//     prior state worth carrying forward (e.g. it became non-dialable and so wasn't dialed this scan) —
//     Verdict empty, DelegationActive true;
//   - every endpoint in prior whose root_domain was processed this scan but that no longer appears in
//     the current delegation at all — Verdict empty, DelegationActive false (a removal).
//
// prior (as loaded by load.LoadAXFRPrior) is read here only to know which past endpoints existed; it is
// never mutated. Endpoints with neither a probe this scan nor any prior state are omitted entirely —
// there is nothing yet worth recording for them.
func (s *Store) AXFRLoadRows(ctx context.Context, scanID string, prior map[AXFREndpointKey]AXFRPriorState) ([]AXFRProbedEndpoint, error) {
	domainRows, err := s.db.QueryContext(ctx, `SELECT ad.root_domain, sd.ns_endpoints
		FROM axfr_domains ad JOIN scan_domains sd ON sd.scan_id = ad.scan_id AND sd.root_domain = ad.root_domain
		WHERE ad.scan_id = ? AND ad.status = 'done'`, scanID)
	if err != nil {
		return nil, err
	}
	touched := map[string]bool{}
	delegation := map[string]map[[2]string]bool{} // root_domain -> {(name_server, ip): true}
	for domainRows.Next() {
		var rd, epJSON string
		if err := domainRows.Scan(&rd, &epJSON); err != nil {
			domainRows.Close()
			return nil, err
		}
		touched[rd] = true
		var eps []model.NameserverEndpoint
		_ = json.Unmarshal([]byte(epJSON), &eps)
		set := make(map[[2]string]bool, len(eps))
		for _, ep := range eps {
			set[[2]string{ep.Name, ep.IP}] = true
		}
		delegation[rd] = set
	}
	if err := domainRows.Err(); err != nil {
		domainRows.Close()
		return nil, err
	}
	domainRows.Close()

	probeRows, err := s.db.QueryContext(ctx, `SELECT root_domain, name_server, name_server_ip, verdict, reason, observed_at
		FROM axfr_probes WHERE scan_id = ?`, scanID)
	if err != nil {
		return nil, err
	}
	seen := map[AXFREndpointKey]bool{}
	var out []AXFRProbedEndpoint
	for probeRows.Next() {
		var rd, ns, ip, verdict, reason, ts string
		if err := probeRows.Scan(&rd, &ns, &ip, &verdict, &reason, &ts); err != nil {
			probeRows.Close()
			return nil, err
		}
		key := AXFREndpointKey{RootDomain: rd, NameServer: ns, NameServerIP: ip}
		seen[key] = true
		out = append(out, AXFRProbedEndpoint{
			AXFREndpointKey: key, Verdict: verdict, Reason: reason, ObservedAt: parseTS(ts),
			Definitive:       verdict == string(resolve.VerdictOpen) || verdict == string(resolve.VerdictClosed),
			DelegationActive: true,
		})
	}
	if err := probeRows.Err(); err != nil {
		probeRows.Close()
		return nil, err
	}
	probeRows.Close()

	// Delegated but not probed this scan (e.g. non-dialable/hyperscaler) — only worth a row if there is
	// prior state to carry forward.
	for rd, set := range delegation {
		for nsip := range set {
			key := AXFREndpointKey{RootDomain: rd, NameServer: nsip[0], NameServerIP: nsip[1]}
			if seen[key] {
				continue
			}
			if _, ok := prior[key]; !ok {
				continue
			}
			seen[key] = true
			out = append(out, AXFRProbedEndpoint{AXFREndpointKey: key, DelegationActive: true})
		}
	}

	// Previously active for a domain touched this scan, but no longer in its current delegation at all.
	for key := range prior {
		if seen[key] || !touched[key.RootDomain] {
			continue
		}
		out = append(out, AXFRProbedEndpoint{AXFREndpointKey: key, DelegationActive: false})
	}
	return out, nil
}

// AXFRLoadComplete reports whether the AXFR ClickHouse load pass finished for scanID (a restart skips
// re-running it).
func (s *Store) AXFRLoadComplete(ctx context.Context, scanID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT complete FROM axfr_load_state WHERE scan_id = ?`, scanID).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return n == 1, err
}

// MarkAXFRLoadComplete records that the AXFR ClickHouse load pass finished for scanID.
func (s *Store) MarkAXFRLoadComplete(ctx context.Context, scanID string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO axfr_load_state (scan_id, complete) VALUES (?, 1)
		 ON CONFLICT(scan_id) DO UPDATE SET complete = 1`, scanID)
	return err
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
