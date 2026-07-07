//go:build integration

package load

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
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
