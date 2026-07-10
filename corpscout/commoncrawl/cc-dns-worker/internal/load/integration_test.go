//go:build integration

package load

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_HOST", "localhost") + ":" + envOr("CLICKHOUSE_NATIVE_PORT", "9000")},
		Auth: clickhouse.Auth{Database: envOr("CLICKHOUSE_DATABASE", "corpscout"), Username: envOr("CLICKHOUSE_USER", "default"), Password: envOr("CLICKHOUSE_PASSWORD", "")},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

// Stages one domain + records in a temp scan.db, loads to CH, reads back. Requires the migrations
// applied to a reachable ClickHouse. As of Task 7, FromStore loads records into the retry-safe
// commoncrawl_domain_dns_record_observations table, not the legacy commoncrawl_domain_dns_records
// aggregate (see this package's doc comment) — this test reads back from the observations table
// accordingly. See integration_test.go's TestObservation* tests for the retry/summary behavior.
func TestLoadFromStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Seed(ctx, "itest", []string{"example.test"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "itest", RootDomain: "example.test", ETLD: "test", Status: "done", SourceRunID: "itest",
		Nameservers: []string{"ns1.example.test"}, NSIPs: []string{"1.1.1.1"},
		QueriesTotal: 2, QueriesOK: 2, ResolvedAt: now,
		Records: []model.DNSRecord{{Name: "example.test", RecordType: "MX", Value: "mail.example.test", Priority: 10, Rcode: "NOERROR", TTL: 300}},
	}}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	// Register cleanup up front so the itest rows are removed from shared ClickHouse even if an
	// assertion below fails via t.Fatalf (which would skip trailing statements). Close last.
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='example.test'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='example.test'")
		_ = conn.Close()
	})
	nr, nd, err := FromStore(ctx, conn, st, "itest")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if nr != 1 || nd != 1 {
		t.Fatalf("loaded records=%d domains=%d, want 1/1", nr, nd)
	}

	var rt, scanID string
	if err := conn.QueryRow(ctx,
		"SELECT record_type, scan_id FROM corpscout.commoncrawl_domain_dns_record_observations FINAL WHERE root_domain='example.test' LIMIT 1").Scan(&rt, &scanID); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if rt != "MX" {
		t.Errorf("record_type = %q, want MX", rt)
	}
	if scanID != "itest" {
		t.Errorf("scan_id = %q, want itest", scanID)
	}
}

// TestLoadAXFRRoundTrip proves the AXFR load pass (LoadAXFRPrior -> StageAXFRChanges ->
// WriteAXFRStateChanges -> WriteAXFRLatest) is retry-safe — re-running it for the same scan's already-
// staged data must not duplicate dns_axfr_latest/dns_axfr_state_changes rows (FINAL counts stay put) —
// and that a later scan resolving the same root domain to a DIFFERENT NS host/IP creates a separate
// endpoint history rather than merging into (or clobbering) the first endpoint's, with the old endpoint
// correctly falling to delegation_active=0 instead of vanishing.
func TestLoadAXFRRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	conn := testConn(t)
	domain := "axfr-itest.test"
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.dns_axfr_latest WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.dns_axfr_state_changes WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	stage := func(scanID, nsHost, ip string, verdict resolve.AXFRVerdict, reason resolve.AXFRReason, at time.Time) {
		t.Helper()
		if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		eps := []model.NameserverEndpoint{{Name: nsHost, IP: ip, Scope: "public", Dialable: true}}
		if err := st.CommitBatch(ctx, []model.DomainResult{{
			ScanID: scanID, RootDomain: domain, Status: "done", ResolvedAt: at,
			NSIPs: []string{ip}, Endpoints: eps,
		}}); err != nil {
			t.Fatalf("commit domain: %v", err)
		}
		if _, err := st.SeedAXFRDomains(ctx, scanID); err != nil {
			t.Fatalf("seed axfr domains: %v", err)
		}
		probes := []resolve.AXFROutcome{{Verdict: verdict, Reason: reason, NSHost: nsHost, NSIP: ip, ObservedAt: at}}
		if err := st.CommitAXFRDomain(ctx, scanID, domain, probes, nil, scanID, at, ""); err != nil {
			t.Fatalf("commit axfr: %v", err)
		}
	}
	// runLoadPass calls the granular pieces directly (bypassing store.AXFRLoadComplete's once-only gate)
	// so re-running it genuinely re-reads prior from ClickHouse and re-derives the same rows, rather than
	// merely proving the SQLite watermark skips a second run.
	runLoadPass := func(scanID string, at time.Time) {
		t.Helper()
		prior, err := LoadAXFRPrior(ctx, conn)
		if err != nil {
			t.Fatalf("load prior: %v", err)
		}
		if _, err := st.StageAXFRChanges(ctx, scanID, prior); err != nil {
			t.Fatalf("stage changes: %v", err)
		}
		if _, err := WriteAXFRStateChanges(ctx, conn, st, scanID); err != nil {
			t.Fatalf("write changes: %v", err)
		}
		if _, err := WriteAXFRLatest(ctx, conn, st, scanID, prior, at); err != nil {
			t.Fatalf("write latest: %v", err)
		}
	}
	countFinal := func(table string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table+" FINAL WHERE root_domain=?", domain).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		return n
	}

	now := time.Now().UTC()
	stage("itest-axfr-1", "ns1."+domain, "203.0.113.10", resolve.VerdictOpen, resolve.ReasonTransferred, now)
	runLoadPass("itest-axfr-1", now)
	runLoadPass("itest-axfr-1", now) // retry over the same staged data: must not duplicate anything

	if n := countFinal("corpscout.dns_axfr_latest"); n != 1 {
		t.Errorf("dns_axfr_latest rows after retry = %d, want 1", n)
	}
	if n := countFinal("corpscout.dns_axfr_state_changes"); n != 1 {
		t.Errorf("dns_axfr_state_changes rows after retry = %d, want 1 (the initial open, not duplicated)", n)
	}

	// A later scan resolves the same root domain behind a DIFFERENT NS host/IP.
	later := now.Add(time.Hour)
	stage("itest-axfr-2", "ns2."+domain, "203.0.113.20", resolve.VerdictOpen, resolve.ReasonTransferred, later)
	runLoadPass("itest-axfr-2", later)

	if n := countFinal("corpscout.dns_axfr_latest"); n != 2 {
		t.Errorf("dns_axfr_latest rows after an NS host/IP change = %d, want 2 (separate endpoint history)", n)
	}
	if n := countFinal("corpscout.dns_axfr_state_changes"); n != 2 {
		t.Errorf("dns_axfr_state_changes rows after an NS host/IP change = %d, want 2", n)
	}

	var ns1Active, ns2Active uint8
	if err := conn.QueryRow(ctx, `SELECT delegation_active FROM corpscout.dns_axfr_latest FINAL
		WHERE root_domain=? AND name_server_ip=?`, domain, "203.0.113.10").Scan(&ns1Active); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT delegation_active FROM corpscout.dns_axfr_latest FINAL
		WHERE root_domain=? AND name_server_ip=?`, domain, "203.0.113.20").Scan(&ns2Active); err != nil {
		t.Fatal(err)
	}
	if ns1Active != 0 {
		t.Errorf("old endpoint (dropped from delegation) delegation_active = %d, want 0", ns1Active)
	}
	if ns2Active != 1 {
		t.Errorf("new endpoint (current delegation) delegation_active = %d, want 1", ns2Active)
	}
}

// refreshSummaryAndWait forces an immediate refresh of the Task 7 summary view (migration 000114)
// rather than waiting out its real REFRESH EVERY 10 MINUTE schedule, then blocks until it completes so
// the summary table reflects whatever was just inserted. It recomputes the WHOLE table (every
// root_domain, not just the one a test cares about) — exactly what happens on the real schedule.
func refreshSummaryAndWait(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	if err := conn.Exec(ctx, "SYSTEM REFRESH VIEW "+recordSummaryMV); err != nil {
		t.Fatalf("refresh %s: %v", recordSummaryMV, err)
	}
	if err := conn.Exec(ctx, "SYSTEM WAIT VIEW "+recordSummaryMV); err != nil {
		t.Fatalf("wait %s: %v", recordSummaryMV, err)
	}
}

// TestObservationRetryDedupAndSummaryScans proves the core Task 7 fix end to end through the public
// FromStore path: loading the SAME staged scan twice (simulating a retry after a crash/timeout between
// ClickHouse accepting the insert and the caller learning about it) must not duplicate observations —
// ReplacingMergeTree(loaded_at) on the full (identity, scan_id) sorting key collapses the retry to one
// row on FINAL — and the refreshed summary's scans for that record must read 1, not 2.
func TestObservationRetryDedupAndSummaryScans(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	domain := "obs-retry.test"
	scanID := "obs-scan-retry"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{{Name: domain, RecordType: "A", Slot: "@", Value: "10.0.1.1",
			Rcode: "NOERROR", TTL: 300, Source: "query", Discovery: "static"}},
	}}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	if _, _, err := FromStore(ctx, conn, st, scanID); err != nil {
		t.Fatalf("first load: %v", err)
	}
	if _, _, err := FromStore(ctx, conn, st, scanID); err != nil {
		t.Fatalf("retried load: %v", err)
	}

	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM corpscout.commoncrawl_domain_dns_record_observations FINAL WHERE root_domain=?", domain).Scan(&n); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if n != 1 {
		t.Errorf("observations FINAL count after a retried load = %d, want 1", n)
	}

	refreshSummaryAndWait(t, ctx, conn)
	var scans uint64
	if err := conn.QueryRow(ctx, "SELECT scans FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain=?", domain).Scan(&scans); err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if scans != 1 {
		t.Errorf("summary scans after a retried load = %d, want 1 (one scan_id, however many times its load was retried)", scans)
	}
}

// TestObservationDualProvenanceRetainsBothSources proves that when the SAME scan observes the SAME
// record identity through two different sources (a direct query and an AXFR zone transfer), the
// summary keeps BOTH provenance values (groupUniqArray) rather than collapsing to whichever anyLast
// picked, and still counts the scan exactly once (uniqExactIf(scan_id, ...) — not once per source row).
func TestObservationDualProvenanceRetainsBothSources(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	domain := "obs-dual.test"
	scanID := "obs-scan-dual"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{
			{Name: domain, RecordType: "A", Slot: "@", Value: "10.0.2.1", Rcode: "NOERROR", TTL: 300, Source: "query", Discovery: "static"},
			{Name: domain, RecordType: "A", Slot: "@", Value: "10.0.2.1", Rcode: "NOERROR", TTL: 300, Source: "axfr", Discovery: "axfr"},
		},
	}}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	if _, _, err := FromStore(ctx, conn, st, scanID); err != nil {
		t.Fatalf("load: %v", err)
	}
	refreshSummaryAndWait(t, ctx, conn)

	var scans uint64
	var sources []string
	if err := conn.QueryRow(ctx, "SELECT scans, sources FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain=?", domain).
		Scan(&scans, &sources); err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if scans != 1 {
		t.Errorf("scans = %d, want 1 (one scan_id, observed via two sources)", scans)
	}
	sort.Strings(sources)
	if len(sources) != 2 || sources[0] != "axfr" || sources[1] != "query" {
		t.Errorf("sources = %v, want [axfr query] (both provenance values retained)", sources)
	}
}

// TestObservationOutOfOrderLoadEventTimeWins proves that when an OLDER observation is loaded (its
// loaded_at is inserted) AFTER a NEWER one already landed, the summary's argMax(ttl/rcode/last_run_id,
// (event_time, version_time)) still reflects whichever observation has the LATER observed_at (event
// time) — not whichever was loaded most recently. Inserted directly at the ClickHouse layer (bypassing
// the SQLite stage) since the scenario is about ClickHouse-side load order, not the scanner.
func TestObservationOutOfOrderLoadEventTimeWins(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	domain := "obs-ooo.test"
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	base := time.Now().UTC().Truncate(time.Millisecond)
	newer := model.RecordObservationRow{
		RootDomain: domain, Name: domain, RecordType: "A", Slot: "@", Value: "10.0.3.1",
		Source: "query", Discovery: "static", ScanID: "obs-scan-newer",
		TTL: 111, Rcode: "NOERROR",
		ObservedAt: base,
		LoadedAt:   base, // loaded FIRST, chronologically
	}
	older := model.RecordObservationRow{
		RootDomain: domain, Name: domain, RecordType: "A", Slot: "@", Value: "10.0.3.1",
		Source: "query", Discovery: "static", ScanID: "obs-scan-older",
		TTL: 222, Rcode: "SERVFAIL",
		ObservedAt: base.Add(-6 * time.Hour), // an OLDER event...
		LoadedAt:   base.Add(1 * time.Hour),  // ...loaded AFTER the newer one
	}
	if _, err := insert(ctx, conn, recordObsTable, []model.RecordObservationRow{newer}); err != nil {
		t.Fatalf("insert newer observation: %v", err)
	}
	if _, err := insert(ctx, conn, recordObsTable, []model.RecordObservationRow{older}); err != nil {
		t.Fatalf("insert older observation out of order: %v", err)
	}

	refreshSummaryAndWait(t, ctx, conn)

	var ttl uint32
	var rcode, lastRunID string
	var scans uint64
	if err := conn.QueryRow(ctx, "SELECT ttl, rcode, last_run_id, scans FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain=?", domain).
		Scan(&ttl, &rcode, &lastRunID, &scans); err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if ttl != 111 || rcode != "NOERROR" || lastRunID != "obs-scan-newer" {
		t.Errorf("event-time metadata must win despite being loaded first: ttl=%d rcode=%s last_run_id=%s, want ttl=111 rcode=NOERROR last_run_id=obs-scan-newer",
			ttl, rcode, lastRunID)
	}
	if scans != 2 {
		t.Errorf("scans = %d, want 2 (two distinct scan_ids)", scans)
	}
}

// TestObservationSummaryPreservesLegacyBaseline proves the cutover-boundary invariant: a record's
// pre-cutover legacy scans/first_seen (written through loadRecordsLegacy, the kept-but-unused legacy
// path — see this package's doc comment) survive into the summary, and a NEW scan against the SAME
// record identity, loaded through the observations path post-cutover, ADDS to the legacy count rather
// than replacing it.
func TestObservationSummaryPreservesLegacyBaseline(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	domain := "obs-legacy.test"
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_records WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain='"+domain+"'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	legacyFirstSeen := time.Now().Add(-30 * 24 * time.Hour).UTC().Truncate(time.Millisecond)
	legacyLastSeen := time.Now().Add(-25 * 24 * time.Hour).UTC().Truncate(time.Millisecond)
	legacyRow := model.RecordRow{
		RootDomain: domain, RecordType: "A", Slot: "@", Name: domain, Value: "10.0.4.1",
		TTL: 300, Rcode: "NOERROR", LastRunID: "legacy-run",
		FirstSeen: legacyFirstSeen, LastSeen: legacyLastSeen, Scans: 5,
		Source: "query", Discovery: "static",
	}
	// Simulate pre-cutover history written by the legacy loader path (kept available, unused by
	// FromStore/Incremental, precisely so this cutover boundary can be reproduced in a test).
	if _, err := loadRecordsLegacy(ctx, conn, []model.RecordRow{legacyRow}); err != nil {
		t.Fatalf("seed legacy baseline: %v", err)
	}

	// A brand-new, post-cutover scan resolves the SAME record identity through the observations path.
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	scanID := "obs-scan-post-cutover"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{{Name: domain, RecordType: "A", Slot: "@", Value: "10.0.4.1",
			Rcode: "NOERROR", TTL: 300, Source: "query", Discovery: "static"}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := FromStore(ctx, conn, st, scanID); err != nil {
		t.Fatalf("load post-cutover scan: %v", err)
	}

	refreshSummaryAndWait(t, ctx, conn)

	var scans uint64
	var firstSeen time.Time
	if err := conn.QueryRow(ctx, "SELECT scans, first_seen FROM corpscout.commoncrawl_domain_dns_record_summary WHERE root_domain=?", domain).
		Scan(&scans, &firstSeen); err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if scans != 6 {
		t.Errorf("scans = %d, want 6 (5 legacy + 1 new scan_id)", scans)
	}
	if !firstSeen.Equal(legacyFirstSeen) {
		t.Errorf("first_seen = %v, want the legacy baseline's first_seen (%v) preserved", firstSeen, legacyFirstSeen)
	}
}
