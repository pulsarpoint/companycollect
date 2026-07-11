//go:build integration

package hostsource

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_HOST", "localhost") + ":" + envOr("CLICKHOUSE_NATIVE_PORT", "9000")},
		Auth: clickhouse.Auth{
			Database: envOr("CLICKHOUSE_DATABASE", "corpscout"),
			Username: envOr("CLICKHOUSE_USER", "default"),
			Password: envOr("CLICKHOUSE_PASSWORD", ""),
		},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

func insertRegistry(t *testing.T, ctx context.Context, conn driver.Conn, rootDomain, label, source string, lastNotAfter time.Time) {
	t.Helper()
	now := time.Now().UTC()
	if err := conn.Exec(ctx, `INSERT INTO corpscout.commoncrawl_domain_hostnames
		(root_domain, label, discovery_source, first_seen, last_seen, last_resolved, last_not_after)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, rootDomain, label, source, now, now, now, lastNotAfter); err != nil {
		t.Fatalf("insert registry row for %s/%s: %v", rootDomain, label, err)
	}
}

func TestFetchReturnsOnlyRequestedRoots(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	suffix := time.Now().UTC().Format("150405000000000")
	requested := []string{"a-" + suffix + ".test", "b-" + suffix + ".test", "c-" + suffix + ".test"}
	unrequested := "d-" + suffix + ".test"
	all := append(append([]string{}, requested...), unrequested)
	t.Cleanup(func() {
		for _, rootDomain := range all {
			_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_hostnames WHERE root_domain = ?", rootDomain)
		}
		_ = conn.Close()
	})
	for _, rootDomain := range all {
		insertRegistry(t, ctx, conn, rootDomain, "www", "ct", time.Now().Add(time.Hour))
	}

	got, err := Fetch(ctx, conn, requested, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, rootDomain := range requested {
		if len(got[rootDomain]) != 1 || got[rootDomain][0].Label != "www" {
			t.Errorf("%s rows = %+v, want one www label", rootDomain, got[rootDomain])
		}
	}
	if _, exists := got[unrequested]; exists {
		t.Errorf("unrequested root leaked into result: %+v", got[unrequested])
	}
}

func TestFetchCapKeepsAXFRBeforeCT(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	rootDomain := "cap-" + time.Now().UTC().Format("150405000000000") + ".test"
	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_hostnames WHERE root_domain = ?", rootDomain)
		_ = conn.Close()
	})
	for index := range 3 {
		insertRegistry(t, ctx, conn, rootDomain, fmt.Sprintf("axfr-%d", index), "axfr", time.Time{})
	}
	for index := range 20 {
		insertRegistry(t, ctx, conn, rootDomain, fmt.Sprintf("ct-%d", index), "ct", time.Now().Add(time.Hour))
	}

	got, err := Fetch(ctx, conn, []string{rootDomain}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[rootDomain]) != 5 {
		t.Fatalf("rows = %d, want cap of 5", len(got[rootDomain]))
	}
	axfrCount := 0
	for _, hostname := range got[rootDomain] {
		if hostname.DiscoverySource == "axfr" {
			axfrCount++
		}
	}
	if axfrCount != 3 {
		t.Errorf("AXFR rows kept = %d, want all 3: %+v", axfrCount, got[rootDomain])
	}
}
