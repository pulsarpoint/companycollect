package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
)

const boundedSchema = `
CREATE TABLE IF NOT EXISTS scan_state (
  scan_id          TEXT PRIMARY KEY,
  domain_cursor    TEXT NOT NULL DEFAULT '',
  source_exhausted INTEGER NOT NULL DEFAULT 0,
  domains_fetched  INTEGER NOT NULL DEFAULT 0,
  started_at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dns_work (
  scan_id        TEXT NOT NULL,
  root_domain    TEXT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'pending',
  etld           TEXT NOT NULL DEFAULT '',
  nameservers    TEXT NOT NULL DEFAULT '[]',
  ns_ips         TEXT NOT NULL DEFAULT '[]',
  ns_endpoints   TEXT NOT NULL DEFAULT '[]',
  dnssec_signed  INTEGER NOT NULL DEFAULT 0,
  ds_present     INTEGER NOT NULL DEFAULT 0,
  ds_outcome     TEXT NOT NULL DEFAULT '',
  dnskey_outcome TEXT NOT NULL DEFAULT '',
  result_status  TEXT NOT NULL DEFAULT '',
  queries_total  INTEGER NOT NULL DEFAULT 0,
  queries_ok     INTEGER NOT NULL DEFAULT 0,
  error          TEXT NOT NULL DEFAULT '',
  source_run_id  TEXT NOT NULL DEFAULT '',
  resolved_at    TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (scan_id, root_domain)
);
CREATE INDEX IF NOT EXISTS idx_dns_work_status ON dns_work (scan_id, status, root_domain);
CREATE TABLE IF NOT EXISTS dns_records (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  name          TEXT NOT NULL,
  record_type   TEXT NOT NULL,
  slot          TEXT NOT NULL DEFAULT '',
  value         TEXT NOT NULL,
  ttl           INTEGER NOT NULL DEFAULT 0,
  priority      INTEGER NOT NULL DEFAULT 0,
  source        TEXT NOT NULL DEFAULT 'query',
  discovery     TEXT NOT NULL DEFAULT 'static',
  rcode         TEXT NOT NULL DEFAULT '',
  finding       TEXT NOT NULL DEFAULT '',
  source_run_id TEXT NOT NULL DEFAULT '',
  resolved_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dns_records_work ON dns_records (scan_id, root_domain);
CREATE TABLE IF NOT EXISTS axfr_work (
  scan_id                TEXT NOT NULL,
  root_domain            TEXT NOT NULL,
  ns_endpoints_json      TEXT NOT NULL,
  delegation_observed_at TEXT NOT NULL,
  status                 TEXT NOT NULL DEFAULT 'pending',
  error                  TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (scan_id, root_domain)
);
CREATE INDEX IF NOT EXISTS idx_axfr_work_status ON axfr_work (scan_id, status, root_domain);
CREATE TABLE IF NOT EXISTS axfr_zone_records (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  name          TEXT NOT NULL,
  record_type   TEXT NOT NULL,
  slot          TEXT NOT NULL DEFAULT '',
  value         TEXT NOT NULL,
  ttl           INTEGER NOT NULL DEFAULT 0,
  priority      INTEGER NOT NULL DEFAULT 0,
  rcode         TEXT NOT NULL DEFAULT '',
  discovery     TEXT NOT NULL DEFAULT 'axfr',
  observed_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_axfr_zone_records_work ON axfr_zone_records (scan_id, root_domain);
`

type SourceState struct {
	Cursor          string
	SourceExhausted bool
	DomainsFetched  int
	StartedAt       time.Time
}

type WorkCounts struct {
	Pending int
	Running int
	Ready   int
}

// BeginCycle creates the single durable source cursor for a scan without changing an existing cycle.
func (s *Store) BeginCycle(ctx context.Context, scanID string, startedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO scan_state
		(scan_id, started_at) VALUES (?, ?)`, scanID, startedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) SourceState(ctx context.Context, scanID string) (SourceState, error) {
	var state SourceState
	var exhausted int
	var startedAt string
	err := s.db.QueryRowContext(ctx, `SELECT domain_cursor, source_exhausted, domains_fetched, started_at
		FROM scan_state WHERE scan_id = ?`, scanID).Scan(
		&state.Cursor, &exhausted, &state.DomainsFetched, &startedAt,
	)
	if err != nil {
		return SourceState{}, err
	}
	state.SourceExhausted = exhausted != 0
	state.StartedAt = parseTS(startedAt)
	return state, nil
}

// AddDomainPage inserts one ClickHouse keyset page and advances its cursor in the same transaction.
// If maxDomains clips the page, exhausted is forced true so the cycle cannot fetch past the limit.
func (s *Store) AddDomainPage(ctx context.Context, scanID string, domains []string, exhausted bool, maxDomains int) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var fetched int
	if err := tx.QueryRowContext(ctx, `SELECT domains_fetched FROM scan_state WHERE scan_id = ?`, scanID).Scan(&fetched); err != nil {
		return 0, err
	}
	if maxDomains > 0 && fetched+len(domains) > maxDomains {
		domains = domains[:maxDomains-fetched]
		exhausted = true
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO dns_work (scan_id, root_domain) VALUES (?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	added := 0
	for _, domain := range domains {
		if domain == "" {
			continue
		}
		result, err := stmt.ExecContext(ctx, scanID, domain)
		if err != nil {
			return 0, err
		}
		if rows, _ := result.RowsAffected(); rows == 1 {
			added++
		}
	}
	cursor := ""
	if len(domains) > 0 {
		cursor = domains[len(domains)-1]
	}
	if _, err := tx.ExecContext(ctx, `UPDATE scan_state SET
		domain_cursor = CASE WHEN ? = '' THEN domain_cursor ELSE ? END,
		source_exhausted = ?, domains_fetched = domains_fetched + ? WHERE scan_id = ?`,
		cursor, cursor, b2i(exhausted), added, scanID); err != nil {
		return 0, err
	}
	return added, tx.Commit()
}

func (s *Store) DNSWorkCount(ctx context.Context, scanID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM dns_work WHERE scan_id = ?`, scanID).Scan(&count)
	return count, err
}

func (s *Store) DNSWorkCounts(ctx context.Context, scanID string) (WorkCounts, error) {
	return workCounts(ctx, s.db, "dns_work", scanID)
}

func (s *Store) AXFRWorkCounts(ctx context.Context, scanID string) (WorkCounts, error) {
	return workCounts(ctx, s.db, "axfr_work", scanID)
}

func (s *Store) AXFRWorkCount(ctx context.Context, scanID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_work WHERE scan_id = ?`, scanID).Scan(&count)
	return count, err
}

func (s *Store) CheckpointWAL(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

func workCounts(ctx context.Context, db *sql.DB, table, scanID string) (WorkCounts, error) {
	var counts WorkCounts
	err := db.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE status = 'pending'),
		count(*) FILTER (WHERE status = 'running'),
		count(*) FILTER (WHERE status = 'ready')
		FROM `+table+` WHERE scan_id = ?`, scanID).Scan(&counts.Pending, &counts.Running, &counts.Ready)
	return counts, err
}

func (s *Store) ResetRunning(ctx context.Context, scanID string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE dns_work SET status = 'pending' WHERE scan_id = ? AND status = 'running'`, scanID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE axfr_work SET status = 'pending' WHERE scan_id = ? AND status = 'running'`, scanID)
	return err
}

func (s *Store) ClaimDNS(ctx context.Context, scanID string, limit int) ([]string, error) {
	return claimRoots(ctx, s.db, "dns_work", scanID, limit)
}

func claimRoots(ctx context.Context, db *sql.DB, table, scanID string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT root_domain FROM `+table+`
		WHERE scan_id = ? AND status = 'pending' ORDER BY root_domain LIMIT ?`, scanID, limit)
	if err != nil {
		return nil, err
	}
	var roots []string
	for rows.Next() {
		var root string
		if err := rows.Scan(&root); err != nil {
			rows.Close()
			return nil, err
		}
		roots = append(roots, root)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(roots) == 0 {
		return nil, tx.Commit()
	}
	query, args := rootsQuery(`UPDATE `+table+` SET status = 'running' WHERE scan_id = ? AND status = 'pending' AND root_domain IN (%s)`, scanID, roots)
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if updated, err := result.RowsAffected(); err != nil || int(updated) != len(roots) {
		return nil, fmt.Errorf("claim %s: selected %d roots but updated %d", table, len(roots), updated)
	}
	return roots, tx.Commit()
}

// CommitDNS atomically stores the DNS outbox, marks work ready, and creates independent AXFR work.
func (s *Store) CommitDNS(ctx context.Context, result model.DomainResult, enqueueAXFR bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM dns_records WHERE scan_id = ? AND root_domain = ?`, result.ScanID, result.RootDomain); err != nil {
		return err
	}
	observedAt := result.ResolvedAt.UTC().Format(time.RFC3339Nano)
	for _, record := range result.Records {
		if _, err := tx.ExecContext(ctx, `INSERT INTO dns_records
			(scan_id, root_domain, name, record_type, slot, value, ttl, priority, source, discovery,
			rcode, finding, source_run_id, resolved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			result.ScanID, result.RootDomain, record.Name, record.RecordType, record.Slot, record.Value,
			record.TTL, record.Priority, record.Source, record.Discovery, record.Rcode, record.Finding,
			result.SourceRunID, observedAt); err != nil {
			return err
		}
	}
	nameservers, _ := json.Marshal(result.Nameservers)
	nsIPs, _ := json.Marshal(result.NSIPs)
	endpoints, _ := json.Marshal(result.Endpoints)
	updated, err := tx.ExecContext(ctx, `UPDATE dns_work SET status = 'ready', etld = ?, nameservers = ?,
		ns_ips = ?, ns_endpoints = ?, dnssec_signed = ?, ds_present = ?, ds_outcome = ?,
		dnskey_outcome = ?, result_status = ?, queries_total = ?, queries_ok = ?, error = ?,
		source_run_id = ?, resolved_at = ? WHERE scan_id = ? AND root_domain = ? AND status = 'running'`,
		result.ETLD, string(nameservers), string(nsIPs), string(endpoints), b2i(result.DNSSECSigned),
		b2i(result.DSPresent), result.DSOutcome, result.DNSKEYOutcome, result.Status,
		result.QueriesTotal, result.QueriesOK, result.Error, result.SourceRunID, observedAt,
		result.ScanID, result.RootDomain)
	if err != nil {
		return err
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("commit DNS %q: work is not running", result.RootDomain)
	}
	delegationTrustworthy := result.Status != model.DomainStatusError && len(result.Endpoints) > 0
	if enqueueAXFR && delegationTrustworthy {
		if _, err := tx.ExecContext(ctx, `INSERT INTO axfr_work
			(scan_id, root_domain, ns_endpoints_json, delegation_observed_at, status)
			VALUES (?, ?, ?, ?, 'pending') ON CONFLICT(scan_id, root_domain) DO UPDATE SET
			ns_endpoints_json = excluded.ns_endpoints_json,
			delegation_observed_at = excluded.delegation_observed_at,
			status = CASE WHEN axfr_work.status = 'ready' THEN axfr_work.status ELSE 'pending' END`,
			result.ScanID, result.RootDomain, string(endpoints), observedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ReleaseDNS(ctx context.Context, scanID string, roots []string) error {
	return releaseRoots(ctx, s.db, "dns_work", scanID, roots)
}

func releaseRoots(ctx context.Context, db *sql.DB, table, scanID string, roots []string) error {
	if len(roots) == 0 {
		return nil
	}
	query, args := rootsQuery(`UPDATE `+table+` SET status = 'pending' WHERE scan_id = ? AND status = 'running' AND root_domain IN (%s)`, scanID, roots)
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

type ReadyDNSBatch struct {
	Roots     []string
	Results   []model.DomainResult
	Records   []model.RecordRow
	Summaries []model.ScanRow
	Hostnames []model.HostnameRow
}

func (s *Store) ReadyDNS(ctx context.Context, scanID string, limit int) (ReadyDNSBatch, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, etld, nameservers, ns_ips, ns_endpoints,
		dnssec_signed, ds_present, ds_outcome, dnskey_outcome, result_status, queries_total, queries_ok,
		error, source_run_id, resolved_at FROM dns_work WHERE scan_id = ? AND status = 'ready'
		ORDER BY root_domain LIMIT ?`, scanID, limit)
	if err != nil {
		return ReadyDNSBatch{}, err
	}
	var batch ReadyDNSBatch
	for rows.Next() {
		var result model.DomainResult
		var nameservers, nsIPs, endpoints, observedAt string
		var dnssec, ds int
		result.ScanID = scanID
		if err := rows.Scan(&result.RootDomain, &result.ETLD, &nameservers, &nsIPs, &endpoints,
			&dnssec, &ds, &result.DSOutcome, &result.DNSKEYOutcome, &result.Status,
			&result.QueriesTotal, &result.QueriesOK, &result.Error, &result.SourceRunID, &observedAt); err != nil {
			rows.Close()
			return ReadyDNSBatch{}, err
		}
		_ = json.Unmarshal([]byte(nameservers), &result.Nameservers)
		_ = json.Unmarshal([]byte(nsIPs), &result.NSIPs)
		_ = json.Unmarshal([]byte(endpoints), &result.Endpoints)
		result.DNSSECSigned = dnssec != 0
		result.DSPresent = ds != 0
		result.ResolvedAt = parseTS(observedAt)
		batch.Roots = append(batch.Roots, result.RootDomain)
		batch.Results = append(batch.Results, result)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ReadyDNSBatch{}, err
	}
	rows.Close()
	if len(batch.Roots) == 0 {
		return batch, nil
	}
	return s.populateReadyDNS(ctx, scanID, batch)
}

func (s *Store) populateReadyDNS(ctx context.Context, scanID string, batch ReadyDNSBatch) (ReadyDNSBatch, error) {
	query, args := rootsQuery(`SELECT root_domain, name, record_type, slot, value, ttl, priority,
		rcode, source, discovery, finding, source_run_id, resolved_at FROM dns_records
		WHERE scan_id = ? AND root_domain IN (%s)`, scanID, batch.Roots)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ReadyDNSBatch{}, err
	}
	for rows.Next() {
		var record model.RecordRow
		var observedAt string
		if err := rows.Scan(&record.RootDomain, &record.Name, &record.RecordType, &record.Slot,
			&record.Value, &record.TTL, &record.Priority, &record.Rcode, &record.Source,
			&record.Discovery, &record.Finding, &record.LastRunID, &observedAt); err != nil {
			rows.Close()
			return ReadyDNSBatch{}, err
		}
		record.FirstSeen = parseTS(observedAt)
		record.LastSeen = record.FirstSeen
		record.Scans = 1
		batch.Records = append(batch.Records, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ReadyDNSBatch{}, err
	}
	rows.Close()
	batch.Summaries = scanRows(batch.Results)
	batch.Hostnames = hostnameRows(batch.Records)
	return batch, nil
}

func scanRows(results []model.DomainResult) []model.ScanRow {
	var rows []model.ScanRow
	for _, result := range results {
		if result.Status != model.DomainStatusDone && result.Status != model.DomainStatusNoPublicNSEndpoints {
			continue
		}
		row := model.ScanRow{
			RootDomain: result.RootDomain, ETLD: result.ETLD, Nameservers: result.Nameservers,
			NSIPs: result.NSIPs, Endpoints: result.Endpoints, DNSSECSigned: uint8(b2i(result.DNSSECSigned)),
			DSPresent: uint8(b2i(result.DSPresent)), DSOutcome: result.DSOutcome,
			DNSKEYOutcome: result.DNSKEYOutcome, Status: result.Status,
			QueriesTotal: uint16(result.QueriesTotal), QueriesOK: uint16(result.QueriesOK),
			LastRunID: result.SourceRunID, ResolvedAt: result.ResolvedAt,
		}
		setScanRowEndpoints(&row)
		rows = append(rows, row)
	}
	return rows
}

func hostnameRows(records []model.RecordRow) []model.HostnameRow {
	type key struct{ root, label string }
	byKey := map[key]model.HostnameRow{}
	for _, record := range records {
		if record.Discovery != "ct" && record.Discovery != "axfr" {
			continue
		}
		if record.RecordType != "A" && record.RecordType != "AAAA" && record.RecordType != "CNAME" {
			continue
		}
		name := strings.ToLower(record.Name)
		suffix := "." + strings.ToLower(record.RootDomain)
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		label := strings.TrimSuffix(name, suffix)
		if label == "" || strings.Contains(label, "*") {
			continue
		}
		identity := key{record.RootDomain, label}
		row, exists := byKey[identity]
		if !exists || record.LastSeen.After(row.LastSeen) {
			byKey[identity] = model.HostnameRow{
				RootDomain: record.RootDomain, Label: label, DiscoverySource: record.Discovery,
				FirstSeen: record.FirstSeen, LastSeen: record.LastSeen, LastResolved: record.LastSeen,
			}
		}
	}
	rows := make([]model.HostnameRow, 0, len(byKey))
	for _, row := range byKey {
		rows = append(rows, row)
	}
	return rows
}

func (s *Store) AcknowledgeDNS(ctx context.Context, scanID string, roots []string) error {
	return acknowledgeRoots(ctx, s.db, "dns_work", "dns_records", scanID, roots)
}

func acknowledgeRoots(ctx context.Context, db *sql.DB, workTable, childTable, scanID string, roots []string) error {
	if len(roots) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query, args := rootsQuery(`DELETE FROM `+childTable+` WHERE scan_id = ? AND root_domain IN (%s)`, scanID, roots)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	query, args = rootsQuery(`DELETE FROM `+workTable+` WHERE scan_id = ? AND status = 'ready' AND root_domain IN (%s)`, scanID, roots)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return tx.Commit()
}

func rootsQuery(format, scanID string, roots []string) (string, []any) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(roots)), ",")
	args := make([]any, 0, len(roots)+1)
	args = append(args, scanID)
	for _, root := range roots {
		args = append(args, root)
	}
	return fmt.Sprintf(format, placeholders), args
}

type BoundedAXFRTarget struct {
	RootDomain           string
	Endpoints            []model.NameserverEndpoint
	DelegationObservedAt time.Time
}

func (s *Store) ClaimAXFR(ctx context.Context, scanID string, limit int) ([]BoundedAXFRTarget, error) {
	roots, err := claimRoots(ctx, s.db, "axfr_work", scanID, limit)
	if err != nil || len(roots) == 0 {
		return nil, err
	}
	query, args := rootsQuery(`SELECT root_domain, ns_endpoints_json, delegation_observed_at
		FROM axfr_work WHERE scan_id = ? AND root_domain IN (%s) ORDER BY root_domain`, scanID, roots)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []BoundedAXFRTarget
	for rows.Next() {
		var target BoundedAXFRTarget
		var endpoints, observedAt string
		if err := rows.Scan(&target.RootDomain, &endpoints, &observedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(endpoints), &target.Endpoints); err != nil {
			return nil, err
		}
		target.DelegationObservedAt = parseTS(observedAt)
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (s *Store) CommitAXFR(ctx context.Context, scanID, rootDomain string, probes []resolve.AXFROutcome, zone []model.DNSRecord, errMessage string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM axfr_probes WHERE scan_id = ? AND root_domain = ?`, scanID, rootDomain); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM axfr_zone_records WHERE scan_id = ? AND root_domain = ?`, scanID, rootDomain); err != nil {
		return err
	}
	for _, probe := range probes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO axfr_probes
			(scan_id, root_domain, name_server, name_server_ip, verdict, reason, records, bytes, truncated, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, scanID, rootDomain, probe.NSHost, probe.NSIP,
			string(probe.Verdict), string(probe.Reason), probe.Records, probe.Bytes, b2i(probe.Truncated),
			probe.ObservedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	observedAt := ""
	for _, record := range zone {
		for _, probe := range probes {
			if probe.IsOpen() {
				observedAt = probe.ObservedAt.UTC().Format(time.RFC3339Nano)
				break
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO axfr_zone_records
			(scan_id, root_domain, name, record_type, slot, value, ttl, priority, rcode, discovery, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, scanID, rootDomain, record.Name,
			record.RecordType, record.Slot, record.Value, record.TTL, record.Priority, record.Rcode,
			record.Discovery, observedAt); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE axfr_work SET status = 'ready', error = ?
		WHERE scan_id = ? AND root_domain = ? AND status = 'running'`, errMessage, scanID, rootDomain)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("commit AXFR %q: work is not running", rootDomain)
	}
	return tx.Commit()
}

type ReadyAXFRJob struct {
	BoundedAXFRTarget
	Probes []resolve.AXFROutcome
	Zone   []model.DNSRecord
}

func (s *Store) ReadyAXFR(ctx context.Context, scanID string, limit int) ([]ReadyAXFRJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT root_domain, ns_endpoints_json, delegation_observed_at
		FROM axfr_work WHERE scan_id = ? AND status = 'ready' ORDER BY root_domain LIMIT ?`, scanID, limit)
	if err != nil {
		return nil, err
	}
	var jobs []ReadyAXFRJob
	for rows.Next() {
		var job ReadyAXFRJob
		var endpoints, observedAt string
		if err := rows.Scan(&job.RootDomain, &endpoints, &observedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal([]byte(endpoints), &job.Endpoints); err != nil {
			rows.Close()
			return nil, err
		}
		job.DelegationObservedAt = parseTS(observedAt)
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(jobs) == 0 {
		return nil, nil
	}
	byRoot := make(map[string]*ReadyAXFRJob, len(jobs))
	roots := make([]string, len(jobs))
	for index := range jobs {
		byRoot[jobs[index].RootDomain] = &jobs[index]
		roots[index] = jobs[index].RootDomain
	}
	query, args := rootsQuery(`SELECT root_domain, name_server, name_server_ip, verdict, reason,
		records, bytes, truncated, observed_at FROM axfr_probes
		WHERE scan_id = ? AND root_domain IN (%s)`, scanID, roots)
	probeRows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	for probeRows.Next() {
		var rootDomain, verdict, reason, observedAt string
		var probe resolve.AXFROutcome
		var truncated int
		if err := probeRows.Scan(&rootDomain, &probe.NSHost, &probe.NSIP, &verdict, &reason,
			&probe.Records, &probe.Bytes, &truncated, &observedAt); err != nil {
			probeRows.Close()
			return nil, err
		}
		probe.Verdict = resolve.AXFRVerdict(verdict)
		probe.Reason = resolve.AXFRReason(reason)
		probe.Truncated = truncated != 0
		probe.ObservedAt = parseTS(observedAt)
		byRoot[rootDomain].Probes = append(byRoot[rootDomain].Probes, probe)
	}
	if err := probeRows.Err(); err != nil {
		probeRows.Close()
		return nil, err
	}
	probeRows.Close()
	query, args = rootsQuery(`SELECT root_domain, name, record_type, slot, value, ttl, priority,
		rcode, discovery FROM axfr_zone_records WHERE scan_id = ? AND root_domain IN (%s)`, scanID, roots)
	zoneRows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	for zoneRows.Next() {
		var rootDomain string
		var record model.DNSRecord
		if err := zoneRows.Scan(&rootDomain, &record.Name, &record.RecordType, &record.Slot,
			&record.Value, &record.TTL, &record.Priority, &record.Rcode, &record.Discovery); err != nil {
			zoneRows.Close()
			return nil, err
		}
		record.Source = "axfr"
		byRoot[rootDomain].Zone = append(byRoot[rootDomain].Zone, record)
	}
	if err := zoneRows.Err(); err != nil {
		zoneRows.Close()
		return nil, err
	}
	zoneRows.Close()
	return jobs, nil
}

func (s *Store) AcknowledgeAXFR(ctx context.Context, scanID string, roots []string) error {
	if len(roots) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"axfr_probes", "axfr_zone_records"} {
		query, args := rootsQuery(`DELETE FROM `+table+` WHERE scan_id = ? AND root_domain IN (%s)`, scanID, roots)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	query, args := rootsQuery(`DELETE FROM axfr_work WHERE scan_id = ? AND status = 'ready' AND root_domain IN (%s)`, scanID, roots)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return tx.Commit()
}
