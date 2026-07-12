package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
)

const boundedSchema = `
CREATE TABLE IF NOT EXISTS scan_state (
  scan_id          TEXT PRIMARY KEY,
  domain_cursor    TEXT NOT NULL DEFAULT '',
  source_exhausted INTEGER NOT NULL DEFAULT 0,
  domains_fetched  INTEGER NOT NULL DEFAULT 0,
  domains_processed INTEGER NOT NULL DEFAULT 0,
  records_observed INTEGER NOT NULL DEFAULT 0,
  dns_checks       INTEGER NOT NULL DEFAULT 0,
  dns_checks_ok    INTEGER NOT NULL DEFAULT 0,
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
  record_type_code INTEGER NOT NULL DEFAULT 0,
  record_class_code INTEGER NOT NULL DEFAULT 0,
  slot          TEXT NOT NULL DEFAULT '',
  value         TEXT NOT NULL,
  rdata_wire    BLOB NOT NULL DEFAULT X'',
  ttl           INTEGER NOT NULL DEFAULT 0,
  priority      INTEGER NOT NULL DEFAULT 0,
  source        TEXT NOT NULL DEFAULT 'query',
  discovery     TEXT NOT NULL DEFAULT 'static',
  name_server   TEXT NOT NULL DEFAULT '',
  name_server_ip TEXT NOT NULL DEFAULT '',
  rcode         TEXT NOT NULL DEFAULT '',
  finding       TEXT NOT NULL DEFAULT '',
  source_run_id TEXT NOT NULL DEFAULT '',
  resolved_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dns_records_work ON dns_records (scan_id, root_domain);
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
	formatted := startedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO scan_state (scan_id, started_at) VALUES (?, ?)`, scanID, formatted)
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
	_, err := s.db.ExecContext(ctx, `UPDATE dns_work SET status = 'pending' WHERE scan_id = ? AND status = 'running'`, scanID)
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

// CommitDNS atomically stores the DNS outbox and marks the domain ready for ClickHouse.
func (s *Store) CommitDNS(ctx context.Context, result model.DomainResult) error {
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
			(scan_id, root_domain, name, record_type, record_type_code, record_class_code, slot, value,
			rdata_wire, ttl, priority, source, discovery, name_server, name_server_ip, rcode, finding,
			source_run_id, resolved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			result.ScanID, result.RootDomain, record.Name, record.RecordType, record.TypeCode,
			record.ClassCode, record.Slot, record.Value, []byte(record.RDataWire), record.TTL,
			record.Priority, record.Source, record.Discovery, record.NameServer, record.NameServerIP,
			record.Rcode, record.Finding,
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
	if _, err := tx.ExecContext(ctx, `UPDATE scan_state SET
		domains_processed = domains_processed + 1,
		records_observed = records_observed + ?,
		dns_checks = dns_checks + ?, dns_checks_ok = dns_checks_ok + ? WHERE scan_id = ?`,
		len(result.Records), result.QueriesTotal, result.QueriesOK,
		result.ScanID); err != nil {
		return err
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
	Records   []model.StagedDNSRecord
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
	query, args := rootsQuery(`SELECT root_domain, name, record_type, record_type_code,
		record_class_code, slot, value, rdata_wire, ttl, priority, rcode, source, discovery,
		name_server, name_server_ip, finding, resolved_at FROM dns_records
		WHERE scan_id = ? AND root_domain IN (%s)`, scanID, batch.Roots)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ReadyDNSBatch{}, err
	}
	for rows.Next() {
		var record model.StagedDNSRecord
		var rdataWire []byte
		var observedAt string
		if err := rows.Scan(&record.RootDomain, &record.Name, &record.RecordType, &record.TypeCode,
			&record.ClassCode, &record.Slot, &record.Value, &rdataWire, &record.TTL,
			&record.Priority, &record.Rcode, &record.Source, &record.Discovery, &record.NameServer,
			&record.NameServerIP, &record.Finding, &observedAt); err != nil {
			rows.Close()
			return ReadyDNSBatch{}, err
		}
		record.RDataWire = string(rdataWire)
		record.ObservedAt = parseTS(observedAt)
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

func hostnameRows(records []model.StagedDNSRecord) []model.HostnameRow {
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
		if !exists || record.ObservedAt.After(row.LastSeen) {
			byKey[identity] = model.HostnameRow{
				RootDomain: record.RootDomain, Label: label, DiscoverySource: record.Discovery,
				FirstSeen: record.ObservedAt, LastSeen: record.ObservedAt, LastResolved: record.ObservedAt,
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
