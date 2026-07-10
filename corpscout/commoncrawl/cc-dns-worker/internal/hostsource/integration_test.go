//go:build integration

package hostsource

import (
	"context"
	"os"
	"testing"
	"time"

	"cc-dns-worker/internal/model"

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

// mirrorSeed inserts one dns_scan_seed_domains row per domain for scanID — the same shape
// load.MirrorSeedDomains produces, reproduced directly here so this package's integration test doesn't
// need to import internal/load (which would create an import cycle risk / unnecessary coupling) or spin
// up a SQLite stage.
func mirrorSeed(t *testing.T, ctx context.Context, conn driver.Conn, scanID string, domains []string) {
	t.Helper()
	rows := make([]model.ScanSeedDomainRow, len(domains))
	now := time.Now().UTC()
	for i, d := range domains {
		rows[i] = model.ScanSeedDomainRow{ScanID: scanID, RootDomain: d, SeededAt: now}
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO corpscout.dns_scan_seed_domains (scan_id, root_domain, seeded_at)")
	if err != nil {
		t.Fatalf("prepare seed batch: %v", err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			t.Fatalf("append seed row: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send seed batch: %v", err)
	}
}

// insertCT inserts one ctlogs.hostnames row (a live, non-wildcard certificate observation).
func insertCT(t *testing.T, ctx context.Context, conn driver.Conn, registeredDomain, fqdn string) {
	t.Helper()
	future := time.Now().Add(365 * 24 * time.Hour).UTC()
	if err := conn.Exec(ctx, `INSERT INTO ctlogs.hostnames
		(registered_domain, fqdn, is_wildcard, first_seen, last_seen, last_not_after, source_logs)
		VALUES (?, ?, 0, now(), now(), ?, [])`, registeredDomain, fqdn, future); err != nil {
		t.Fatalf("insert ctlogs row for %s: %v", fqdn, err)
	}
}

// insertRegistry inserts one commoncrawl_domain_hostnames row.
func insertRegistry(t *testing.T, ctx context.Context, conn driver.Conn, rootDomain, label, source string) {
	t.Helper()
	now := time.Now().UTC()
	if err := conn.Exec(ctx, `INSERT INTO corpscout.commoncrawl_domain_hostnames
		(root_domain, label, discovery_source, first_seen, last_seen, last_resolved)
		VALUES (?, ?, ?, ?, ?, ?)`, rootDomain, label, source, now, now, now); err != nil {
		t.Fatalf("insert registry row for %s/%s: %v", rootDomain, label, err)
	}
}

// collectAllShards runs ShardStream across every hash shard and returns every row produced, keyed by
// (root_domain, label) -> source, for straightforward assertions in tests that don't want to compute
// cityHash64 themselves to find which shard a given domain lands in.
func collectAllShards(t *testing.T, ctx context.Context, conn driver.Conn, scanID string, capN int) map[string]model.ScanHostnameRow {
	t.Helper()
	out := map[string]model.ScanHostnameRow{}
	for shard := 0; shard < CTPartitions; shard++ {
		err := ShardStream(ctx, conn, scanID, shard, CTPartitions, capN, 500, func(batch []model.ScanHostnameRow) error {
			for _, r := range batch {
				out[r.RootDomain+"\x00"+r.Label] = r
			}
			return nil
		})
		if err != nil {
			t.Fatalf("ShardStream shard %d: %v", shard, err)
		}
	}
	return out
}

// TestShardStreamScopedToSeedMembership is the end-to-end scope proof (Task 11): a scan that seeded
// only 3 domains must enrich EXACTLY those 3 — a 4th domain with real CT/registry data, but never
// mirrored into dns_scan_seed_domains for this scan_id, must produce ZERO rows.
func TestShardStreamScopedToSeedMembership(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	scanID := "itest-scope-" + time.Now().UTC().Format("150405.000000000")
	seeded := []string{"seeded-a.test", "seeded-b.test", "seeded-c.test"}
	unseeded := "unseeded-d.test"

	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.dns_scan_seed_domains WHERE scan_id = ?", scanID)
		for _, d := range append(append([]string{}, seeded...), unseeded) {
			_ = conn.Exec(ctx, "DELETE FROM ctlogs.hostnames WHERE registered_domain = ?", d)
			_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_hostnames WHERE root_domain = ?", d)
		}
		_ = conn.Close()
	})

	mirrorSeed(t, ctx, conn, scanID, seeded)
	for _, d := range seeded {
		insertCT(t, ctx, conn, d, "www."+d)
	}
	// The unseeded domain has real CT data too, but was never mirrored for this scan_id.
	insertCT(t, ctx, conn, unseeded, "www."+unseeded)

	got := collectAllShards(t, ctx, conn, scanID, 100)

	for _, d := range seeded {
		if _, ok := got[d+"\x00www"]; !ok {
			t.Errorf("seeded domain %s: expected a www label, got none — result: %+v", d, got)
		}
	}
	for key := range got {
		if len(key) >= len(unseeded) && key[:len(unseeded)] == unseeded {
			t.Errorf("unseeded domain %s leaked a row into the scan-scoped result: %+v", unseeded, got)
		}
	}
	if len(got) != len(seeded) {
		t.Errorf("total rows = %d, want exactly %d (one per seeded domain, none for the unseeded 4th)", len(got), len(seeded))
	}
}

// TestShardStreamAXFRBeatsCTOnDuplicateLabel proves that when the SAME label is discoverable via both
// AXFR (recorded in the durable registry) and CT, the explicit numeric priority — not lexical order —
// makes axfr win.
func TestShardStreamAXFRBeatsCTOnDuplicateLabel(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	scanID := "itest-dup-" + time.Now().UTC().Format("150405.000000000")
	domain := "dup-label.test"

	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.dns_scan_seed_domains WHERE scan_id = ?", scanID)
		_ = conn.Exec(ctx, "DELETE FROM ctlogs.hostnames WHERE registered_domain = ?", domain)
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_hostnames WHERE root_domain = ?", domain)
		_ = conn.Close()
	})

	mirrorSeed(t, ctx, conn, scanID, []string{domain})
	insertCT(t, ctx, conn, domain, "www."+domain)       // CT sees "www"
	insertRegistry(t, ctx, conn, domain, "www", "axfr") // registry ALSO has "www", via axfr

	got := collectAllShards(t, ctx, conn, scanID, 100)
	row, ok := got[domain+"\x00www"]
	if !ok {
		t.Fatalf("expected a www row for %s, got none: %+v", domain, got)
	}
	if row.DiscoverySource != "axfr" {
		t.Errorf("duplicate label source = %q, want axfr (explicit numeric priority axfr(%d) > ct(%d))", row.DiscoverySource, axfrPriority, ctPriority)
	}
}

// TestShardStreamCapNeverEvictsAXFRForCT proves that when a domain has more than host-cap CT-only
// labels PLUS a small number of durable AXFR labels, the cap always keeps every AXFR label and trims
// only from the CT-only surplus — an AXFR label can never be evicted by CT labels regardless of count.
func TestShardStreamCapNeverEvictsAXFRForCT(t *testing.T) {
	ctx := context.Background()
	conn := testConn(t)
	scanID := "itest-cap-" + time.Now().UTC().Format("150405.000000000")
	domain := "cap-test.test"
	const capN = 5
	const axfrLabels = 3
	const ctLabels = 20 // far more than capN - axfrLabels

	t.Cleanup(func() {
		_ = conn.Exec(ctx, "DELETE FROM corpscout.dns_scan_seed_domains WHERE scan_id = ?", scanID)
		_ = conn.Exec(ctx, "DELETE FROM ctlogs.hostnames WHERE registered_domain = ?", domain)
		_ = conn.Exec(ctx, "DELETE FROM corpscout.commoncrawl_domain_hostnames WHERE root_domain = ?", domain)
		_ = conn.Close()
	})

	mirrorSeed(t, ctx, conn, scanID, []string{domain})
	for i := 0; i < axfrLabels; i++ {
		insertRegistry(t, ctx, conn, domain, label("axfr", i), "axfr")
	}
	for i := 0; i < ctLabels; i++ {
		insertCT(t, ctx, conn, domain, label("ct", i)+"."+domain)
	}

	got := collectAllShards(t, ctx, conn, scanID, capN)
	var axfrCount, ctCount int
	for key, row := range got {
		if len(key) < len(domain) || key[:len(domain)] != domain {
			continue
		}
		switch row.DiscoverySource {
		case "axfr":
			axfrCount++
		case "ct":
			ctCount++
		}
	}
	if len(got) != capN {
		t.Errorf("total rows for capped domain = %d, want exactly %d (the cap)", len(got), capN)
	}
	if axfrCount != axfrLabels {
		t.Errorf("axfr rows survived = %d, want %d (ALL of them — a durable AXFR label must never be evicted by CT volume)", axfrCount, axfrLabels)
	}
	if ctCount != capN-axfrLabels {
		t.Errorf("ct rows survived = %d, want %d (fills only the remaining cap slots)", ctCount, capN-axfrLabels)
	}
}

func label(prefix string, i int) string {
	return prefix + "-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}
