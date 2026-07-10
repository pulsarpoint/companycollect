// Package load bulk-copies the SQLite stage into the two corpscout ClickHouse tables over the
// native protocol.
//
// # Task 7 cutover: legacy aggregate -> retry-safe observations
//
// commoncrawl_domain_dns_records (recordsTable) is an AggregatingMergeTree that folds Scans=1-per-row
// inserts via sum(): a retried load of the SAME scan re-inserts the same rows and double-counts them
// forever, because the table has no way to tell "this scan again" from "a genuine re-observation".
// FromStore/Incremental therefore no longer write to it. They write to
// commoncrawl_domain_dns_record_observations instead (see model.RecordObservationRow): one IMMUTABLE
// row per (identity, source, discovery, scan_id), deduplicated on retry by ReplacingMergeTree(loaded_at)
// keyed on that full identity, so replaying the same scan's load is always safe regardless of how many
// times it is retried. corpscout.commoncrawl_domain_dns_record_summary (populated by a REFRESHABLE
// materialized view — see migration 000114) recomputes the AggregatingMergeTree-shaped summary from
// this table's FINAL, deduplicated rows unioned with recordsTable's frozen pre-cutover totals.
//
// The cutover from one to the other is NOT a code deploy alone — it is an operational boundary:
//  1. Pause the writer (stop the orchestrator, or at minimum stop new `load` invocations).
//  2. Let the currently-active scan cycle finish (or deliberately abandon it) so no in-flight scan_id
//     straddles the cutover.
//  3. The legacy table's accumulated state as of that moment becomes the permanent baseline —
//     recordsTable is never written to again, and is never dropped or re-engined (see its ch tag doc
//     comment on model.RecordRow).
//  4. Resume the writer so it only ever loads records under NEW scan_ids into the observations table.
//
// The one invariant that must never break: a single scan_id must never contribute to BOTH the legacy
// aggregate (recordsTable) AND the raw observations table — that would let the same scan's records be
// counted in `scans` twice, once via the frozen legacy baseline and once via uniqExact(scan_id) in the
// summary view. Because recordsTable is frozen at cutover and never written again, this holds as long
// as the cutover steps above are followed in order.
//
// Historical `scans` values already in recordsTable as of cutover may already include retry inflation
// from before this fix existed. That inflation is not reconstructable — the original retried scan_ids
// that caused it were never recorded — so it is preserved as a legacy fact in the summary rather than
// silently "corrected" to some guessed value.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	recordsTable       = "corpscout.commoncrawl_domain_dns_records" // legacy baseline, Task 7 and earlier; frozen at cutover, kept for loadRecordsLegacy only
	recordObsTable     = "corpscout.commoncrawl_domain_dns_record_observations"
	recordSummaryTable = "corpscout.commoncrawl_domain_dns_record_summary"    // populated only by recordSummaryMV, never written directly
	recordSummaryMV    = "corpscout.commoncrawl_domain_dns_record_summary_mv" // migration 000114, REFRESH EVERY 10 MINUTE
	scanTable          = "corpscout.commoncrawl_domain_dns_scan"
	hostnamesTable     = "corpscout.commoncrawl_domain_hostnames"
	axfrObsTable       = "corpscout.dns_axfr_observations" // legacy log, Task 4 and earlier; kept for BackfillAXFRFromObservations
	axfrLatestTable    = "corpscout.dns_axfr_latest"
	axfrChangesTable   = "corpscout.dns_axfr_state_changes"
)

// legacyBackfillScanID tags every row BackfillAXFRFromObservations writes, so its output is
// distinguishable from a live scan's and re-running the backfill is idempotent (same scan_id, same
// changed_at per legacy row -> same primary key).
const legacyBackfillScanID = "legacy-backfill"

// chColumns returns the ch tag of each field of T in declaration order.
func chColumns[T any]() []string {
	rt := reflect.TypeOf(*new(T))
	cols := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		if c := rt.Field(i).Tag.Get("ch"); c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

func insert[T any](ctx context.Context, conn driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	q := "INSERT INTO " + table + " (" + strings.Join(chColumns[T](), ", ") + ")"
	batch, err := conn.PrepareBatch(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", table, err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append %s row %d: %w", table, i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send %s: %w", table, err)
	}
	return len(rows), nil
}

// toObservationRows converts staged RecordRows (already carrying the record identity, source, and
// discovery fields RecordObservationRow needs) into observation rows for scanID, an event-time
// (ObservedAt) taken from each RecordRow's LastSeen (the scan's resolution time), and a shared
// loadedAt version. ScanID comes from the scanID PARAMETER already threaded through FromStore/
// Incremental — the same value store.CommitBatch wrote into scan_records.scan_id — not from
// RecordRow.LastRunID (a separate field, source_run_id, that happens to equal it in this codebase
// today but is not guaranteed to), so the observation's identity is correct even if that assumption
// ever changes.
func toObservationRows(recs []model.RecordRow, scanID string, loadedAt time.Time) []model.RecordObservationRow {
	if len(recs) == 0 {
		return nil
	}
	loadedAt = loadedAt.UTC()
	out := make([]model.RecordObservationRow, len(recs))
	for i, r := range recs {
		out[i] = model.RecordObservationRow{
			RootDomain: r.RootDomain,
			Name:       r.Name,
			RecordType: r.RecordType,
			Slot:       r.Slot,
			Value:      r.Value,
			Source:     r.Source,
			Discovery:  r.Discovery,
			ScanID:     scanID,
			TTL:        r.TTL,
			Priority:   r.Priority,
			Rcode:      r.Rcode,
			ObservedAt: r.LastSeen,
			LoadedAt:   loadedAt,
		}
	}
	return out
}

// loadRecordsLegacy is the pre-Task-7 record-load path: it wrote RecordRows straight into the legacy
// AggregatingMergeTree recordsTable, whose Scans=1-per-row insert double-counts on a retried load (the
// bug this task fixes). FromStore/Incremental no longer call it — kept only so the cutover documented
// in this package's doc comment is reversible without resurrecting deleted code.
func loadRecordsLegacy(ctx context.Context, conn driver.Conn, recs []model.RecordRow) (int, error) {
	return insert(ctx, conn, recordsTable, recs)
}

// FromStore reads the whole stage for scanID and inserts records + domain summaries into ClickHouse.
// Records go to the retry-safe observations table (see this package's doc comment on the Task 7
// cutover), not the legacy aggregate. Idempotent (the CH tables dedup on merge/FINAL), so it is safe
// to re-run.
func FromStore(ctx context.Context, conn driver.Conn, st *store.Store, scanID string) (int, int, error) {
	recs, err := st.StagedRecords(ctx, scanID)
	if err != nil {
		return 0, 0, err
	}
	nr, err := insert(ctx, conn, recordObsTable, toObservationRows(recs, scanID, time.Now()))
	if err != nil {
		return 0, 0, err
	}
	doms, err := st.StagedDomains(ctx, scanID)
	if err != nil {
		return nr, 0, err
	}
	nd, err := insert(ctx, conn, scanTable, doms)
	return nr, nd, err
}

// Incremental loads every scan_records row committed since the persisted rowid watermark for scanID,
// in batches, advancing the watermark after each batch. Records go to the retry-safe observations
// table (see this package's doc comment on the Task 7 cutover), not the legacy aggregate. It also
// loads the done-summaries for the domains in each batch. Because it walks scan_records by rowid
// (commit order), it never misses a late-finishing domain, and re-runs are safe (CH dedups on
// merge/FINAL). It returns the number of record rows loaded. Call it periodically during a scan and
// once more after the scan finishes.
func Incremental(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, batch int) (int, error) {
	if batch <= 0 {
		batch = 20000
	}
	w, err := st.LoadedRowid(ctx, scanID)
	if err != nil {
		return 0, err
	}
	total := 0
	for {
		recs, maxRowid, domains, err := st.RecordsAfter(ctx, scanID, w, batch)
		if err != nil {
			return total, err
		}
		if len(recs) == 0 {
			return total, nil
		}
		if _, err := insert(ctx, conn, recordObsTable, toObservationRows(recs, scanID, time.Now())); err != nil {
			return total, err
		}
		sums, err := st.SummariesFor(ctx, scanID, domains)
		if err != nil {
			return total, err
		}
		if _, err := insert(ctx, conn, scanTable, sums); err != nil {
			return total, err
		}
		// Advance the watermark only after CH accepted the batch — a crash before this just re-loads
		// the batch next time (idempotent).
		if err := st.SetLoadedRowid(ctx, scanID, maxRowid); err != nil {
			return total, err
		}
		w = maxRowid
		total += len(recs)
		if len(recs) < batch {
			return total, nil
		}
	}
}

// WriteHostnameRegistry upserts this cycle's non-static discovered hosts into the durable hostname
// registry. Blind INSERT — the AggregatingMergeTree folds first_seen=min, last_seen=max,
// last_resolved=max, discovery_source=min — so it is idempotent and needs no read-before-write.
func WriteHostnameRegistry(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, now time.Time) (int, error) {
	rows, err := st.DiscoveredHostnames(ctx, scanID)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	now = now.UTC()
	for i := range rows {
		rows[i].FirstSeen, rows[i].LastSeen, rows[i].LastResolved = now, now, now
	}
	return insert(ctx, conn, hostnamesTable, rows)
}

// LoadAXFRPrior reads every endpoint's last known state from dns_axfr_latest FINAL — so a concurrent
// background merge can never hand back a stale duplicate row — into the shape store.StageAXFRChanges
// and store.AXFRLoadRows need: not just has_definitive_state/axfr_open (for change detection) but every
// other column too, so an endpoint that isn't freshly probed this scan (unknown verdict, or dropped out
// of delegation) can have its latest row rewritten with everything else carried forward untouched. The
// table is one row per endpoint ever probed, so this full read stays small relative to the DNS record
// tables.
func LoadAXFRPrior(ctx context.Context, conn driver.Conn) (map[store.AXFREndpointKey]store.AXFRPriorState, error) {
	rows, err := conn.Query(ctx, `SELECT root_domain, name_server, name_server_ip, has_definitive_state, axfr_open,
		definitive_at, definitive_scan_id, last_probe_verdict, last_probe_reason, last_probed_at,
		delegation_active, delegation_seen_at FROM `+axfrLatestTable+` FINAL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	prior := map[store.AXFREndpointKey]store.AXFRPriorState{}
	for rows.Next() {
		var key store.AXFREndpointKey
		var hasDefinitive, axfrOpen, delegationActive uint8
		var p store.AXFRPriorState
		if err := rows.Scan(&key.RootDomain, &key.NameServer, &key.NameServerIP, &hasDefinitive, &axfrOpen,
			&p.DefinitiveAt, &p.DefinitiveScanID, &p.LastProbeVerdict, &p.LastProbeReason, &p.LastProbedAt,
			&delegationActive, &p.DelegationSeenAt); err != nil {
			return nil, err
		}
		p.HasDefinitive = hasDefinitive != 0
		p.AXFROpen = axfrOpen != 0
		p.DelegationActive = delegationActive != 0
		prior[key] = p
	}
	return prior, rows.Err()
}

// WriteAXFRStateChanges inserts scanID's staged axfr_changes rows into dns_axfr_state_changes.
// Idempotent: ReplacingMergeTree(changed_at) on a key that includes scan_id means re-inserting the same
// rows (a retry) merges away to one copy, while distinct scans' changes never collide.
func WriteAXFRStateChanges(ctx context.Context, conn driver.Conn, st *store.Store, scanID string) (int, error) {
	rows, err := st.StagedAXFRChanges(ctx, scanID)
	if err != nil {
		return 0, err
	}
	return insert(ctx, conn, axfrChangesTable, rows)
}

// BuildAXFRLatestRows turns the AXFR load pass's per-endpoint view (eps, from store.AXFRLoadRows) into
// one dns_axfr_latest row per endpoint. Every field defaults to prior (so an endpoint with no fresh
// probe this scan is written back byte-for-byte except for delegation), and a fresh probe (Verdict !=
// "") then overwrites LastProbe*/LastProbedAt unconditionally and — only when Definitive — also
// HasDefinitiveState/AXFROpen/DefinitiveAt/DefinitiveScanID. This is the one place the carry-forward and
// definitive-preservation rules live, and it touches no I/O, so it is unit-testable without ClickHouse.
func BuildAXFRLatestRows(eps []store.AXFRProbedEndpoint, prior map[store.AXFREndpointKey]store.AXFRPriorState, scanID string, now time.Time) []model.AXFRLatestRow {
	if len(eps) == 0 {
		return nil
	}
	now = now.UTC()
	rows := make([]model.AXFRLatestRow, 0, len(eps))
	for _, e := range eps {
		p := prior[e.AXFREndpointKey] // zero value if never seen before: HasDefinitive/DelegationActive false
		row := model.AXFRLatestRow{
			RootDomain: e.RootDomain, NameServer: e.NameServer, NameServerIP: e.NameServerIP,
			UpdatedAt:          now,
			DelegationActive:   boolToU8(e.DelegationActive),
			DelegationSeenAt:   p.DelegationSeenAt,
			LastProbeVerdict:   p.LastProbeVerdict,
			LastProbeReason:    p.LastProbeReason,
			LastProbedAt:       p.LastProbedAt,
			HasDefinitiveState: boolToU8(p.HasDefinitive),
			AXFROpen:           boolToU8(p.AXFROpen),
			DefinitiveAt:       p.DefinitiveAt,
			DefinitiveScanID:   p.DefinitiveScanID,
		}
		if e.DelegationActive {
			row.DelegationSeenAt = now // re-confirmed as part of the delegation this scan
		}
		if e.Verdict != "" { // a fresh probe landed this scan
			row.LastProbeVerdict = e.Verdict
			row.LastProbeReason = e.Reason
			row.LastProbedAt = e.ObservedAt
			if e.Definitive {
				row.HasDefinitiveState = 1
				row.AXFROpen = boolToU8(e.Verdict == string(resolve.VerdictOpen))
				row.DefinitiveAt = e.ObservedAt
				row.DefinitiveScanID = scanID
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// WriteAXFRLatest reads scanID's per-endpoint view from the store and upserts one dns_axfr_latest row
// per endpoint (see BuildAXFRLatestRows). ReplacingMergeTree(updated_at) makes a retried write safe: it
// recomputes the identical rows from the same stable staged data and simply replaces the prior version.
func WriteAXFRLatest(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, prior map[store.AXFREndpointKey]store.AXFRPriorState, now time.Time) (int, error) {
	eps, err := st.AXFRLoadRows(ctx, scanID, prior)
	if err != nil {
		return 0, err
	}
	return insert(ctx, conn, axfrLatestTable, BuildAXFRLatestRows(eps, prior, scanID, now))
}

// LoadAXFR runs one AXFR ClickHouse load pass for scanID: load prior state, stage this scan's
// definitive changes against it, write the changes, then write the latest row per endpoint considered,
// then mark scanID's AXFR load complete so a resumed run skips straight past it. Order matters for
// crash-safety — changes land before latest — but the whole pass is also safe to blindly re-run: prior
// is re-read fresh each call, StageAXFRChanges/WriteAXFRStateChanges dedup on their keys, and
// WriteAXFRLatest recomputes the same rows from the same stable staged axfr_probes/scan_domains data.
func LoadAXFR(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, now time.Time) error {
	done, err := st.AXFRLoadComplete(ctx, scanID)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	prior, err := LoadAXFRPrior(ctx, conn)
	if err != nil {
		return fmt.Errorf("load axfr prior: %w", err)
	}
	if _, err := st.StageAXFRChanges(ctx, scanID, prior); err != nil {
		return fmt.Errorf("stage axfr changes: %w", err)
	}
	if _, err := WriteAXFRStateChanges(ctx, conn, st, scanID); err != nil {
		return fmt.Errorf("write axfr state changes: %w", err)
	}
	if _, err := WriteAXFRLatest(ctx, conn, st, scanID, prior, now); err != nil {
		return fmt.Errorf("write axfr latest: %w", err)
	}
	return st.MarkAXFRLoadComplete(ctx, scanID)
}

// BackfillAXFRFromObservations is a one-shot migration path from the legacy, less-detailed
// dns_axfr_observations log (root_domain/name_server(=IP, no hostname)/axfr_open/observed_at — Task 4
// and earlier) into the new retry-safe tables: every legacy row becomes a dns_axfr_state_changes row
// (name_server_ip = legacy.name_server, name_server = "" since the legacy log never recorded the NS
// hostname, changed_at = legacy.observed_at), and each endpoint's chronologically-last legacy row also
// becomes its dns_axfr_latest row (has_definitive_state=1 — the legacy log only ever recorded definitive
// flips — delegation_active=0, since the legacy log never tracked current delegation and this backfill
// has no way to confirm one). The legacy table itself is left untouched. Idempotent: every row's key is
// deterministic from the legacy data (scan_id is always legacyBackfillScanID), so re-running it after
// the live loader has since written newer, real dns_axfr_latest rows for the same endpoints is safe —
// ReplacingMergeTree keeps whichever row has the larger updated_at, and this backfill's updated_at is
// always the historical observed_at, never "now".
func BackfillAXFRFromObservations(ctx context.Context, conn driver.Conn) (int, int, error) {
	rows, err := conn.Query(ctx, `SELECT root_domain, name_server, axfr_open, observed_at FROM `+axfrObsTable+`
		ORDER BY root_domain, name_server, observed_at`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	type endpoint struct{ rootDomain, ip string }
	var changes []model.AXFRStateChangeRow
	latestByEndpoint := map[endpoint]model.AXFRLatestRow{}
	for rows.Next() {
		var rd, ip string
		var open bool
		var ts time.Time
		if err := rows.Scan(&rd, &ip, &open, &ts); err != nil {
			return 0, 0, err
		}
		changes = append(changes, model.AXFRStateChangeRow{
			RootDomain: rd, NameServer: "", NameServerIP: ip,
			AXFROpen: boolToU8(open), ScanID: legacyBackfillScanID, ChangedAt: ts,
		})
		verdict := string(resolve.VerdictClosed)
		if open {
			verdict = string(resolve.VerdictOpen)
		}
		// Rows arrive ordered by observed_at ascending per endpoint, so the last write for a given
		// (root_domain, ip) — overwriting the map entry — is always its chronologically-last state.
		latestByEndpoint[endpoint{rd, ip}] = model.AXFRLatestRow{
			RootDomain: rd, NameServer: "", NameServerIP: ip,
			UpdatedAt: ts, DelegationActive: 0,
			LastProbeVerdict: verdict, LastProbedAt: ts,
			HasDefinitiveState: 1, AXFROpen: boolToU8(open),
			DefinitiveAt: ts, DefinitiveScanID: legacyBackfillScanID,
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}

	nc, err := insert(ctx, conn, axfrChangesTable, changes)
	if err != nil {
		return nc, 0, err
	}
	latest := make([]model.AXFRLatestRow, 0, len(latestByEndpoint))
	for _, r := range latestByEndpoint {
		latest = append(latest, r)
	}
	nl, err := insert(ctx, conn, axfrLatestTable, latest)
	return nc, nl, err
}

func boolToU8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
