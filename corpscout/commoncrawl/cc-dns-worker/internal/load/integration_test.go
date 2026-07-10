//go:build integration

package load

import (
	"context"
	"os"
	"path/filepath"
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
// applied to a reachable ClickHouse.
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
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_records WHERE root_domain='example.test'")
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

	var rt, runID string
	var scans uint64
	if err := conn.QueryRow(ctx,
		"SELECT record_type, last_run_id, scans FROM corpscout.commoncrawl_domain_dns_records FINAL WHERE root_domain='example.test' LIMIT 1").Scan(&rt, &runID, &scans); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if rt != "MX" {
		t.Errorf("record_type = %q, want MX", rt)
	}
	if runID != "itest" {
		t.Errorf("last_run_id = %q, want itest", runID)
	}
	if scans != 1 {
		t.Errorf("scans = %d, want 1 (single insert)", scans)
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
