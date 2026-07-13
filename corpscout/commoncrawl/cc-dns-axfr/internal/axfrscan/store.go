package axfrscan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"cc-dns-axfr/internal/axfrprobe"
	"cc-dns-axfr/internal/model"
	"cc-dns-axfr/internal/scheduler"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS axfr_state (
  scan_id          TEXT PRIMARY KEY,
  domain_cursor    TEXT NOT NULL DEFAULT '',
  source_exhausted INTEGER NOT NULL DEFAULT 0,
  domains_fetched  INTEGER NOT NULL DEFAULT 0,
  probes_tried     INTEGER NOT NULL DEFAULT 0,
  probes_successful INTEGER NOT NULL DEFAULT 0,
  probes_open      INTEGER NOT NULL DEFAULT 0,
  probes_closed    INTEGER NOT NULL DEFAULT 0,
  probes_unknown   INTEGER NOT NULL DEFAULT 0,
  started_at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS axfr_delegations (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  endpoints     TEXT NOT NULL,
  observed_at   TEXT NOT NULL,
  PRIMARY KEY (scan_id, root_domain)
);
CREATE TABLE IF NOT EXISTS axfr_work (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  name_server   TEXT NOT NULL,
  name_server_ip TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'pending',
  verdict       TEXT NOT NULL DEFAULT '',
  reason        TEXT NOT NULL DEFAULT '',
  records       INTEGER NOT NULL DEFAULT 0,
  bytes         INTEGER NOT NULL DEFAULT 0,
  truncated     INTEGER NOT NULL DEFAULT 0,
  observed_at   TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (scan_id, root_domain, name_server_ip)
);
CREATE INDEX IF NOT EXISTS idx_axfr_work_status
  ON axfr_work (scan_id, status, root_domain, name_server_ip);
CREATE TABLE IF NOT EXISTS axfr_zone_records (
  scan_id       TEXT NOT NULL,
  root_domain   TEXT NOT NULL,
  name_server_ip TEXT NOT NULL,
  name          TEXT NOT NULL,
  record_type   TEXT NOT NULL,
  record_type_code INTEGER NOT NULL DEFAULT 0,
  record_class_code INTEGER NOT NULL DEFAULT 0,
  slot          TEXT NOT NULL DEFAULT '',
  value         TEXT NOT NULL,
  rdata_wire    BLOB NOT NULL DEFAULT X'',
  ttl           INTEGER NOT NULL DEFAULT 0,
  priority      INTEGER NOT NULL DEFAULT 0,
  rcode         TEXT NOT NULL DEFAULT '',
  name_server   TEXT NOT NULL DEFAULT '',
  observed_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_axfr_zone_records_work
  ON axfr_zone_records (scan_id, root_domain, name_server_ip);
`

type axfrStore struct {
	database *sql.DB
}

type sourceState struct {
	Cursor          string
	SourceExhausted bool
	DomainsFetched  int
	StartedAt       time.Time
}

type probeJob struct {
	RootDomain   string
	NameServer   string
	NameServerIP string
}

type readyDomain struct {
	RootDomain           string
	Endpoints            []model.NameserverEndpoint
	DelegationObservedAt time.Time
	Probes               []axfrprobe.AXFROutcome
	Zone                 []model.DNSRecord
}

type axfrStats struct {
	Tried      int64
	Successful int64
	Open       int64
	Closed     int64
	Unknown    int64
}

func openStore(path string) (*axfrStore, error) {
	database, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open AXFR SQLite: %w", err)
	}
	database.SetMaxOpenConns(1)
	if _, err := database.Exec(schema); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("create AXFR schema: %w", err)
	}
	return &axfrStore{database: database}, nil
}

func (store *axfrStore) close() error { return store.database.Close() }

func (store *axfrStore) begin(ctx context.Context, scanID string, startedAt time.Time) error {
	_, err := store.database.ExecContext(ctx, `INSERT OR IGNORE INTO axfr_state (scan_id, started_at) VALUES (?, ?)`,
		scanID, formatTime(startedAt))
	return err
}

func (store *axfrStore) state(ctx context.Context, scanID string) (sourceState, error) {
	var state sourceState
	var exhausted int
	var startedAt string
	err := store.database.QueryRowContext(ctx, `SELECT domain_cursor, source_exhausted, domains_fetched, started_at
		FROM axfr_state WHERE scan_id = ?`, scanID).Scan(
		&state.Cursor, &exhausted, &state.DomainsFetched, &startedAt,
	)
	state.SourceExhausted = exhausted != 0
	state.StartedAt = parseTime(startedAt)
	return state, err
}

func (store *axfrStore) addPage(ctx context.Context, scanID string, domains []sourceDomain, exhausted bool, maxDomains int) (int, error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer transaction.Rollback()
	var fetched int
	if err := transaction.QueryRowContext(ctx, `SELECT domains_fetched FROM axfr_state WHERE scan_id = ?`, scanID).Scan(&fetched); err != nil {
		return 0, err
	}
	if maxDomains > 0 && fetched+len(domains) > maxDomains {
		domains = domains[:maxDomains-fetched]
		exhausted = true
	}
	for _, domain := range domains {
		encodedEndpoints, err := json.Marshal(domain.Endpoints)
		if err != nil {
			return 0, fmt.Errorf("encode AXFR endpoints for %s: %w", domain.RootDomain, err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO axfr_delegations
			(scan_id, root_domain, endpoints, observed_at) VALUES (?, ?, ?, ?)`,
			scanID, domain.RootDomain, string(encodedEndpoints), formatTime(domain.ObservedAt)); err != nil {
			return 0, err
		}
		seenIPs := map[string]bool{}
		for _, endpoint := range domain.Endpoints {
			if !endpoint.Dialable || scheduler.IsHyperscaler(endpoint.IP) || seenIPs[endpoint.IP] {
				continue
			}
			seenIPs[endpoint.IP] = true
			if _, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO axfr_work
				(scan_id, root_domain, name_server, name_server_ip) VALUES (?, ?, ?, ?)`,
				scanID, domain.RootDomain, endpoint.Name, endpoint.IP); err != nil {
				return 0, err
			}
		}
	}
	cursor := ""
	if len(domains) > 0 {
		cursor = domains[len(domains)-1].RootDomain
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE axfr_state SET
		domain_cursor = CASE WHEN ? = '' THEN domain_cursor ELSE ? END,
		source_exhausted = ?, domains_fetched = domains_fetched + ? WHERE scan_id = ?`,
		cursor, cursor, boolInt(exhausted), len(domains), scanID); err != nil {
		return 0, err
	}
	return len(domains), transaction.Commit()
}

func (store *axfrStore) activeDomains(ctx context.Context, scanID string) (int, error) {
	var count int
	err := store.database.QueryRowContext(ctx, `SELECT count(*) FROM axfr_delegations WHERE scan_id = ?`, scanID).Scan(&count)
	return count, err
}

func (store *axfrStore) resetRunning(ctx context.Context, scanID string) error {
	_, err := store.database.ExecContext(ctx, `UPDATE axfr_work SET status = 'pending'
		WHERE scan_id = ? AND status = 'running'`, scanID)
	return err
}

func (store *axfrStore) claim(ctx context.Context, scanID string, limit int) ([]probeJob, error) {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer transaction.Rollback()
	rows, err := transaction.QueryContext(ctx, `SELECT root_domain, name_server, name_server_ip
		FROM axfr_work WHERE scan_id = ? AND status = 'pending'
		ORDER BY root_domain, name_server_ip LIMIT ?`, scanID, limit)
	if err != nil {
		return nil, err
	}
	var jobs []probeJob
	for rows.Next() {
		var job probeJob
		if err := rows.Scan(&job.RootDomain, &job.NameServer, &job.NameServerIP); err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, job := range jobs {
		result, err := transaction.ExecContext(ctx, `UPDATE axfr_work SET status = 'running'
			WHERE scan_id = ? AND root_domain = ? AND name_server_ip = ? AND status = 'pending'`,
			scanID, job.RootDomain, job.NameServerIP)
		if err != nil {
			return nil, err
		}
		if updated, _ := result.RowsAffected(); updated != 1 {
			return nil, fmt.Errorf("claim AXFR endpoint %s/%s: concurrent state change", job.RootDomain, job.NameServerIP)
		}
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE axfr_state SET probes_tried = probes_tried + ? WHERE scan_id = ?`, len(jobs), scanID); err != nil {
		return nil, err
	}
	return jobs, transaction.Commit()
}

func (store *axfrStore) commit(ctx context.Context, scanID string, job probeJob, outcome axfrprobe.AXFROutcome) error {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `DELETE FROM axfr_zone_records
		WHERE scan_id = ? AND root_domain = ? AND name_server_ip = ?`, scanID, job.RootDomain, job.NameServerIP); err != nil {
		return err
	}
	for _, record := range outcome.Zone {
		if _, err := transaction.ExecContext(ctx, `INSERT INTO axfr_zone_records
			(scan_id, root_domain, name_server_ip, name, record_type, record_type_code,
			record_class_code, slot, value, rdata_wire, ttl, priority, rcode, name_server, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			scanID, job.RootDomain, job.NameServerIP, record.Name, record.RecordType, record.TypeCode,
			record.ClassCode, record.Slot, record.Value, []byte(record.RDataWire), record.TTL,
			record.Priority, record.Rcode, job.NameServer, formatTime(outcome.ObservedAt)); err != nil {
			return err
		}
	}
	result, err := transaction.ExecContext(ctx, `UPDATE axfr_work SET status = 'ready', verdict = ?,
		reason = ?, records = ?, bytes = ?, truncated = ?, observed_at = ?
		WHERE scan_id = ? AND root_domain = ? AND name_server_ip = ? AND status = 'running'`,
		string(outcome.Verdict), string(outcome.Reason), outcome.Records, outcome.Bytes,
		boolInt(outcome.Truncated), formatTime(outcome.ObservedAt), scanID, job.RootDomain, job.NameServerIP)
	if err != nil {
		return err
	}
	if updated, _ := result.RowsAffected(); updated != 1 {
		return fmt.Errorf("commit AXFR endpoint %s/%s: work is not running", job.RootDomain, job.NameServerIP)
	}
	column := "probes_unknown"
	if outcome.Verdict == axfrprobe.VerdictOpen {
		column = "probes_open"
	} else if outcome.Verdict == axfrprobe.VerdictClosed {
		column = "probes_closed"
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE axfr_state SET `+column+` = `+column+` + 1 WHERE scan_id = ?`, scanID); err != nil {
		return err
	}
	if outcome.IsOpen() && !outcome.Truncated {
		if _, err := transaction.ExecContext(ctx, `UPDATE axfr_state SET probes_successful = probes_successful + 1 WHERE scan_id = ?`, scanID); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func (store *axfrStore) workRemaining(ctx context.Context, scanID string) (int, error) {
	var count int
	err := store.database.QueryRowContext(ctx, `SELECT count(*) FROM axfr_work
		WHERE scan_id = ? AND status IN ('pending', 'running')`, scanID).Scan(&count)
	return count, err
}

func (store *axfrStore) ready(ctx context.Context, scanID string, limit int) ([]readyDomain, error) {
	rows, err := store.database.QueryContext(ctx, `SELECT delegation.root_domain, delegation.endpoints,
		delegation.observed_at FROM axfr_delegations AS delegation
		WHERE delegation.scan_id = ? AND NOT EXISTS (
			SELECT 1 FROM axfr_work AS work WHERE work.scan_id = delegation.scan_id
			AND work.root_domain = delegation.root_domain AND work.status != 'ready'
		) ORDER BY delegation.root_domain LIMIT ?`, scanID, limit)
	if err != nil {
		return nil, err
	}
	var domains []readyDomain
	for rows.Next() {
		var domain readyDomain
		var endpoints, observedAt string
		if err := rows.Scan(&domain.RootDomain, &endpoints, &observedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal([]byte(endpoints), &domain.Endpoints); err != nil {
			rows.Close()
			return nil, err
		}
		domain.DelegationObservedAt = parseTime(observedAt)
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for index := range domains {
		if err := store.populateReady(ctx, scanID, &domains[index]); err != nil {
			return nil, err
		}
	}
	return domains, nil
}

func (store *axfrStore) populateReady(ctx context.Context, scanID string, domain *readyDomain) error {
	rows, err := store.database.QueryContext(ctx, `SELECT name_server, name_server_ip, verdict, reason,
		records, bytes, truncated, observed_at FROM axfr_work
		WHERE scan_id = ? AND root_domain = ? AND status = 'ready'`, scanID, domain.RootDomain)
	if err != nil {
		return err
	}
	for rows.Next() {
		var outcome axfrprobe.AXFROutcome
		var verdict, reason, observedAt string
		var truncated int
		if err := rows.Scan(&outcome.NSHost, &outcome.NSIP, &verdict, &reason, &outcome.Records,
			&outcome.Bytes, &truncated, &observedAt); err != nil {
			rows.Close()
			return err
		}
		outcome.Verdict = axfrprobe.AXFRVerdict(verdict)
		outcome.Reason = axfrprobe.AXFRReason(reason)
		outcome.Truncated = truncated != 0
		outcome.ObservedAt = parseTime(observedAt)
		domain.Probes = append(domain.Probes, outcome)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	recordRows, err := store.database.QueryContext(ctx, `SELECT name, record_type, record_type_code,
		record_class_code, slot, value, rdata_wire, ttl, priority, rcode, name_server, name_server_ip,
		observed_at FROM axfr_zone_records WHERE scan_id = ? AND root_domain = ?`, scanID, domain.RootDomain)
	if err != nil {
		return err
	}
	for recordRows.Next() {
		var record model.DNSRecord
		var wire []byte
		var observedAt string
		if err := recordRows.Scan(&record.Name, &record.RecordType, &record.TypeCode, &record.ClassCode,
			&record.Slot, &record.Value, &wire, &record.TTL, &record.Priority, &record.Rcode,
			&record.NameServer, &record.NameServerIP, &observedAt); err != nil {
			recordRows.Close()
			return err
		}
		record.RDataWire = string(wire)
		record.Source, record.Discovery = "axfr", "axfr"
		domain.Zone = append(domain.Zone, record)
	}
	err = recordRows.Err()
	recordRows.Close()
	return err
}

func (store *axfrStore) acknowledge(ctx context.Context, scanID string, domains []readyDomain) error {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	for _, domain := range domains {
		for _, table := range []string{"axfr_zone_records", "axfr_work", "axfr_delegations"} {
			if _, err := transaction.ExecContext(ctx, `DELETE FROM `+table+` WHERE scan_id = ? AND root_domain = ?`, scanID, domain.RootDomain); err != nil {
				return err
			}
		}
	}
	return transaction.Commit()
}

func (store *axfrStore) stats(ctx context.Context, scanID string) (axfrStats, error) {
	var stats axfrStats
	err := store.database.QueryRowContext(ctx, `SELECT probes_tried, probes_successful, probes_open, probes_closed,
		probes_unknown FROM axfr_state WHERE scan_id = ?`, scanID).Scan(
		&stats.Tried, &stats.Successful, &stats.Open, &stats.Closed, &stats.Unknown,
	)
	return stats, err
}

func (store *axfrStore) checkpoint(ctx context.Context) error {
	_, err := store.database.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func parseTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
