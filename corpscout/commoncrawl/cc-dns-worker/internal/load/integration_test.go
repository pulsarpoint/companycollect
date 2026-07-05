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
		Addr: []string{envOr("CLICKHOUSE_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{Database: "corpscout", Username: envOr("CLICKHOUSE_USER", "default"), Password: os.Getenv("CLICKHOUSE_PASSWORD")},
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
		ScanID: "itest", RootDomain: "example.test", ETLD: "test", Status: "done",
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
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_records WHERE scan_id='itest'")
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE scan_id='itest'")
		_ = conn.Close()
	})
	nr, nd, err := FromStore(ctx, conn, st, "itest")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if nr != 1 || nd != 1 {
		t.Fatalf("loaded records=%d domains=%d, want 1/1", nr, nd)
	}

	var rt string
	if err := conn.QueryRow(ctx,
		"SELECT record_type FROM corpscout.commoncrawl_domain_dns_records FINAL WHERE scan_id='itest' LIMIT 1").Scan(&rt); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if rt != "MX" {
		t.Errorf("record_type = %q, want MX", rt)
	}
}
