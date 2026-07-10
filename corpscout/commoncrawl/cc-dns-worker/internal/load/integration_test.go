//go:build integration

package load

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// TestMirrorSeedDomainsStreamsAndResumes proves load.MirrorSeedDomains (Task 11) mirrors EVERY domain
// seeded into a scan's local queue — regardless of status (done/error/pending) — into
// corpscout.dns_scan_seed_domains, that a second call is a no-op once complete, and that an "old" stage
// DB (one whose seed_membership_state row was never set, simulating a DB seeded before this feature
// existed) is rebuilt correctly by streaming straight from the local scan_domains rows rather than
// re-reading ClickHouse's commoncrawl_domains.
func TestMirrorSeedDomainsStreamsAndResumes(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	scanID := "itest-mirror-" + time.Now().UTC().Format("150405.000000000")
	domains := []string{"mirror-a.test", "mirror-b.test", "mirror-c.test"}
	if _, err := st.Seed(ctx, scanID, domains); err != nil {
		t.Fatal(err)
	}
	// One domain reaches 'done', one 'error' — MirrorSeedDomains must include both plus the still-
	// 'pending' third, since it is membership, not queue state.
	if err := st.CommitBatch(ctx, []model.DomainResult{
		{ScanID: scanID, RootDomain: "mirror-a.test", Status: "done", ResolvedAt: time.Now().UTC()},
		{ScanID: scanID, RootDomain: "mirror-b.test", Status: "error", ResolvedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.dns_scan_seed_domains WHERE scan_id='"+scanID+"'")
		_ = conn.Close()
	})

	n, err := MirrorSeedDomains(ctx, conn, st, scanID, 2 /* small batch to force multiple bounded rounds */)
	if err != nil {
		t.Fatalf("MirrorSeedDomains: %v", err)
	}
	if n != len(domains) {
		t.Fatalf("mirrored %d domains, want %d (all statuses included)", n, len(domains))
	}

	var got []string
	rows, err := conn.Query(ctx, "SELECT root_domain FROM corpscout.dns_scan_seed_domains FINAL WHERE scan_id=? ORDER BY root_domain", scanID)
	if err != nil {
		t.Fatalf("query back: %v", err)
	}
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatal(err)
		}
		got = append(got, d)
	}
	rows.Close()
	sort.Strings(got)
	wantSorted := append([]string{}, domains...)
	sort.Strings(wantSorted)
	if fmt.Sprint(got) != fmt.Sprint(wantSorted) {
		t.Fatalf("dns_scan_seed_domains for %s = %v, want %v", scanID, got, wantSorted)
	}

	// A second call is a no-op: the marker is set, so nothing is re-mirrored.
	n2, err := MirrorSeedDomains(ctx, conn, st, scanID, 2)
	if err != nil {
		t.Fatalf("second MirrorSeedDomains call: %v", err)
	}
	if n2 != 0 {
		t.Errorf("second call mirrored %d domains, want 0 (already complete)", n2)
	}

	// Simulate an OLD stage DB: same local scan_domains, but seed_membership_state was never set (as
	// if this scan finished seeding before that table/feature existed) AND the ClickHouse side is
	// missing/incomplete. A fresh scanID2 sharing the same store reproduces this without needing to
	// hand-edit seed_membership_state.
	scanID2 := scanID + "-old"
	if _, err := st.Seed(ctx, scanID2, domains); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(ctx, "DELETE FROM corpscout.dns_scan_seed_domains WHERE scan_id='"+scanID2+"'") })
	done, err := st.SeedMembershipComplete(ctx, scanID2)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("a brand-new scan_id must start with an incomplete membership marker")
	}
	n3, err := MirrorSeedDomains(ctx, conn, st, scanID2, 2)
	if err != nil {
		t.Fatalf("resume-rebuild MirrorSeedDomains: %v", err)
	}
	if n3 != len(domains) {
		t.Fatalf("resume-rebuild mirrored %d domains, want %d (rebuilt from local scan_domains, not ClickHouse)", n3, len(domains))
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

// failingConn wraps a real driver.Conn and forces PrepareBatch to fail whenever the query targets
// failTable, letting a test exercise Incremental's independent-watermark behavior (Task 8) against a
// REAL ClickHouse connection — everything else about the connection is untouched — without corrupting
// any actual data or needing a full hand-written fake of the driver.Conn interface (embedding + one
// method override is enough).
type failingConn struct {
	driver.Conn
	failTable string
}

func (f *failingConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if f.failTable != "" && strings.Contains(query, f.failTable) {
		return nil, fmt.Errorf("forced failure for %s (test double)", f.failTable)
	}
	return f.Conn.PrepareBatch(ctx, query, opts...)
}

// boundedBatch wraps a real driver.Batch and fails the test the moment more than maxRows are appended to
// a single batch — i.e. the moment a caller tries to send ClickHouse one INSERT bigger than the loader's
// configured batch size, which is exactly what a full in-memory slurp (the pre-Task-8 FromStore) would
// produce.
type boundedBatch struct {
	driver.Batch
	t       *testing.T
	maxRows int
	n       int
}

func (b *boundedBatch) AppendStruct(v any) error {
	b.n++
	if b.n > b.maxRows {
		b.t.Errorf("a single batch grew to more than %d rows — the loader slurped more than one batch's worth into memory at once", b.maxRows)
	}
	return b.Batch.AppendStruct(v)
}

// boundedConn wraps a real driver.Conn so every PrepareBatch call returns a boundedBatch, enforcing
// maxRows across the whole test regardless of which table is being inserted into.
type boundedConn struct {
	driver.Conn
	t       *testing.T
	maxRows int
}

func (c *boundedConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	real, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		return nil, err
	}
	return &boundedBatch{Batch: real, t: c.t, maxRows: c.maxRows}, nil
}

// TestZeroRecordDomainSummaryLoads proves the core Task 8 fix through the public Incremental path: a
// domain committed status='done' with ZERO DNS records — which never appears in scan_records, and so
// never triggers the old record-batch-piggybacked summary load — still gets its row in
// commoncrawl_domain_dns_scan, because the domain-summary stream is now driven independently off
// completed_domains (store.CompletedDomainsAfter), not off which domains happened to show up in a batch
// of records.
func TestZeroRecordDomainSummaryLoads(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	domain := "zero-record.test"
	scanID := "itest-zero-record"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		ETLD: "test", Nameservers: []string{"ns1." + domain}, QueriesTotal: 3, QueriesOK: 3,
		// Records intentionally empty.
	}}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	nr, nd, err := Incremental(ctx, conn, st, scanID, 1000)
	if err != nil {
		t.Fatalf("Incremental: %v", err)
	}
	if nr != 0 {
		t.Errorf("records loaded = %d, want 0 (this domain has none)", nr)
	}
	if nd != 1 {
		t.Fatalf("domain summaries loaded = %d, want 1 (the zero-record domain itself)", nd)
	}

	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain=?`,
		domain).Scan(&status); err != nil {
		t.Fatalf("read back: %v (the zero-record domain's summary never landed in ClickHouse)", err)
	}
	if status != "done" {
		t.Errorf("status = %q, want done", status)
	}
}

// TestRecordLoadFailureDoesNotAdvanceDomainsSeq proves the record and domain-summary watermarks are
// independent in the direction that matters most for correctness: when the record stream's ClickHouse
// insert fails, the domain-summary stream must never even attempt to run (Incremental returns before
// calling it), so domains_seq stays put — and a subsequent, successful call loads everything cleanly,
// proving the failure was retryable rather than poisoning the scan_id.
func TestRecordLoadFailureDoesNotAdvanceDomainsSeq(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	domain := "rec-fail.test"
	scanID := "itest-rec-fail"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{{Name: domain, RecordType: "A", Value: "10.5.5.1", Rcode: "NOERROR", Source: "query", Discovery: "static"}},
	}}); err != nil {
		t.Fatal(err)
	}

	real := testConn(t)
	t.Cleanup(func() {
		_ = real.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = real.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = real.Close()
	})

	failing := &failingConn{Conn: real, failTable: recordObsTable}
	if _, _, err := Incremental(ctx, failing, st, scanID, 1000); err == nil {
		t.Fatal("Incremental with a forced record-insert failure: want error, got nil")
	}

	if w, err := st.LoadedRowid(ctx, scanID); err != nil || w != 0 {
		t.Fatalf("records_rowid after the forced failure = %d, err=%v, want 0 (unadvanced)", w, err)
	}
	if w, err := st.LoadedDomainsSeq(ctx, scanID); err != nil || w != 0 {
		t.Fatalf("domains_seq after a RECORD-load failure = %d, err=%v, want 0 (the summary stream must never have run)", w, err)
	}

	// Retry cleanly with a real connection: both streams now succeed.
	nr, nd, err := Incremental(ctx, real, st, scanID, 1000)
	if err != nil {
		t.Fatalf("retry after the forced failure: %v", err)
	}
	if nr != 1 || nd != 1 {
		t.Fatalf("retry loaded records=%d domains=%d, want 1/1", nr, nd)
	}
	if w, _ := st.LoadedDomainsSeq(ctx, scanID); w == 0 {
		t.Error("domains_seq after a successful retry = 0, want advanced")
	}
}

// TestSummaryLoadFailureDoesNotAdvanceRecordsRowid is TestRecordLoadFailureDoesNotAdvanceDomainsSeq's
// mirror image: the record stream succeeds and advances records_rowid on its own, entirely unaffected by
// a subsequent failure in the (separate) domain-summary stream — proving records_rowid's advance is truly
// local to loadRecordsIncremental, not contingent on the rest of Incremental succeeding.
func TestSummaryLoadFailureDoesNotAdvanceRecordsRowid(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	domain := "sum-fail.test"
	scanID := "itest-sum-fail"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{{Name: domain, RecordType: "A", Value: "10.5.5.2", Rcode: "NOERROR", Source: "query", Discovery: "static"}},
	}}); err != nil {
		t.Fatal(err)
	}

	real := testConn(t)
	t.Cleanup(func() {
		_ = real.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = real.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = real.Close()
	})

	failing := &failingConn{Conn: real, failTable: scanTable}
	if _, _, err := Incremental(ctx, failing, st, scanID, 1000); err == nil {
		t.Fatal("Incremental with a forced domain-summary-insert failure: want error, got nil")
	}

	if w, err := st.LoadedRowid(ctx, scanID); err != nil || w == 0 {
		t.Fatalf("records_rowid after a SUMMARY-load failure = %d, err=%v, want advanced (the record stream already succeeded before the failure)", w, err)
	}
	if w, err := st.LoadedDomainsSeq(ctx, scanID); err != nil || w != 0 {
		t.Fatalf("domains_seq after the forced failure = %d, err=%v, want 0 (unadvanced)", w, err)
	}

	// Retry cleanly: records are already loaded (0 new), the summary now lands.
	nr, nd, err := Incremental(ctx, real, st, scanID, 1000)
	if err != nil {
		t.Fatalf("retry after the forced failure: %v", err)
	}
	if nr != 0 {
		t.Errorf("retry re-loaded %d records, want 0 (already loaded before the failure)", nr)
	}
	if nd != 1 {
		t.Fatalf("retry loaded %d domain summaries, want 1", nd)
	}

	var recCount uint64
	if err := real.QueryRow(ctx, "SELECT count() FROM corpscout.commoncrawl_domain_dns_record_observations FINAL WHERE root_domain=?", domain).Scan(&recCount); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if recCount != 1 {
		t.Errorf("observation rows after the failed-then-retried load = %d, want 1 (no double count)", recCount)
	}
}

// TestHostnameRegistryRetryTimestampsStable proves retrying WriteHostnameRegistry (e.g. after a crash
// between ClickHouse accepting the insert and the caller learning about it) never skews the registry's
// first_seen/last_seen/last_resolved: both calls stamp the row from the scan's own persisted resolved_at,
// not the wall-clock time each call happened to run at, so the AggregatingMergeTree's min/max folding
// converges on that single value regardless of how long apart (or how many times) the write-back retries.
func TestHostnameRegistryRetryTimestampsStable(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	domain := "reg-retry.test"
	scanID := "itest-reg-retry"
	resolvedAt := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", ResolvedAt: resolvedAt,
		Records: []model.DNSRecord{{Name: "vpn." + domain, RecordType: "A", Value: "10.9.9.9", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"}},
	}}); err != nil {
		t.Fatal(err)
	}

	conn := testConn(t)
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_hostnames WHERE root_domain='"+domain+"'")
		_ = conn.Close()
	})

	if _, err := WriteHostnameRegistry(ctx, conn, st, scanID); err != nil {
		t.Fatalf("first write-back: %v", err)
	}
	// Simulate a retry happening well after the first attempt, in wall-clock terms.
	time.Sleep(10 * time.Millisecond)
	if _, err := WriteHostnameRegistry(ctx, conn, st, scanID); err != nil {
		t.Fatalf("retried write-back: %v", err)
	}

	var firstSeen, lastSeen, lastResolved time.Time
	if err := conn.QueryRow(ctx, `SELECT first_seen, last_seen, last_resolved FROM corpscout.commoncrawl_domain_hostnames FINAL
		WHERE root_domain=? AND label='vpn'`, domain).Scan(&firstSeen, &lastSeen, &lastResolved); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !firstSeen.Equal(resolvedAt) || !lastSeen.Equal(resolvedAt) || !lastResolved.Equal(resolvedAt) {
		t.Errorf("timestamps = first=%v last=%v last_resolved=%v, want all == the scan's resolved_at (%v) — a retry must not skew them to wall-clock time",
			firstSeen, lastSeen, lastResolved, resolvedAt)
	}
}

// TestIncrementalBoundedMemory proves Incremental never materializes a whole scan's worth of records in
// Go memory before writing them out: it loads a synthetic store with far more rows than one batch, using
// a small batch size, wrapped in a connection (boundedConn) that fails the test the instant ClickHouse
// receives a single INSERT bigger than that batch size — exactly what a full in-memory slurp (the
// pre-Task-8 FromStore, which called StagedRecords/StagedDomains to read the ENTIRE stage at once) would
// produce.
func TestIncrementalBoundedMemory(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	const totalRecords = 25000
	const batchSize = 500
	domain := "bounded-mem.test"
	scanID := "itest-bounded-mem"
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatal(err)
	}
	recs := make([]model.DNSRecord, totalRecords)
	for i := range recs {
		recs[i] = model.DNSRecord{
			Name:       fmt.Sprintf("h%d.%s", i, domain),
			RecordType: "A",
			Value:      fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff),
			Rcode:      "NOERROR", Source: "query", Discovery: "static",
		}
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", SourceRunID: scanID, ResolvedAt: time.Now().UTC(),
		Records: recs,
	}}); err != nil {
		t.Fatal(err)
	}

	real := testConn(t)
	t.Cleanup(func() {
		_ = real.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_record_observations WHERE root_domain='"+domain+"'")
		_ = real.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE root_domain='"+domain+"'")
		_ = real.Close()
	})
	conn := &boundedConn{Conn: real, t: t, maxRows: batchSize}

	nr, nd, err := Incremental(ctx, conn, st, scanID, batchSize)
	if err != nil {
		t.Fatalf("Incremental: %v", err)
	}
	if nr != totalRecords {
		t.Errorf("records loaded = %d, want %d", nr, totalRecords)
	}
	if nd != 1 {
		t.Errorf("domain summaries loaded = %d, want 1", nd)
	}
}
