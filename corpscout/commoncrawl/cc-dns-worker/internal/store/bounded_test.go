package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
)

func openBoundedTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "dns.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestAddDomainPageCommitsWorkAndCursorTogether(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan", time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	added, err := store.AddDomainPage(ctx, "scan", []string{"a.test", "b.test", "c.test"}, false, 2)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.SourceState(ctx, "scan")
	if err != nil {
		t.Fatal(err)
	}
	if added != 2 || state.Cursor != "b.test" || !state.SourceExhausted || state.DomainsFetched != 2 {
		t.Fatalf("added=%d state=%+v", added, state)
	}
}

func TestCommitDNSPreservesUniversalRecordFields(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddDomainPage(ctx, "scan", []string{"example.com"}, true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimDNS(ctx, "scan", 1); err != nil {
		t.Fatal(err)
	}
	wire := string([]byte{0xde, 0xad, 0, 0xbe, 0xef})
	if err := store.CommitDNS(ctx, model.DomainResult{
		ScanID: "scan", RootDomain: "example.com", Status: model.DomainStatusDone,
		SourceRunID: "scan", ResolvedAt: time.Unix(200, 0),
		Records: []model.DNSRecord{{
			Name: "unknown.example.com", RecordType: "TYPE65400", TypeCode: 65400,
			ClassCode: 65280, Value: `\# 5 DEAD00BEEF`, RDataWire: wire, Discovery: "ct",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	ready, err := store.ReadyDNS(ctx, "scan", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.Records) != 1 || ready.Records[0].TypeCode != 65400 ||
		ready.Records[0].ClassCode != 65280 || ready.Records[0].RDataWire != wire {
		t.Fatalf("ready records = %+v", ready.Records)
	}
	stats, err := store.CumulativeStats(ctx, "scan")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Domains != 1 || stats.Records != 1 || stats.DNSChecks != 0 {
		t.Fatalf("cumulative stats = %+v", stats)
	}
	if err := store.SaveQueryStats(ctx, "scan", 100, 7, 5); err != nil {
		t.Fatal(err)
	}
	stats, err = store.CumulativeStats(ctx, "scan")
	if err != nil || stats.Queries != 100 || stats.QueryErrors != 7 || stats.QueryTimeouts != 5 {
		t.Fatalf("persisted query stats = %+v, err=%v", stats, err)
	}
}

func TestResetRunningDoesNotTouchReadyWork(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddDomainPage(ctx, "scan", []string{"a.test", "b.test"}, true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimDNS(ctx, "scan", 2); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitDNS(ctx, model.DomainResult{
		ScanID: "scan", RootDomain: "a.test", Status: model.DomainStatusDone, ResolvedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetRunning(ctx, "scan"); err != nil {
		t.Fatal(err)
	}
	counts, err := store.DNSWorkCounts(ctx, "scan")
	if err != nil || counts.Pending != 1 || counts.Running != 0 || counts.Ready != 1 {
		t.Fatalf("counts=%+v err=%v", counts, err)
	}
}

func TestDNSQueueNeverExceedsCapacity(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	const capacity = 10
	if err := store.BeginCycle(ctx, "scan", time.Now()); err != nil {
		t.Fatal(err)
	}
	for next := 0; next < 100; {
		active, _ := store.DNSWorkCount(ctx, "scan")
		page := make([]string, min(capacity-active, 100-next))
		for index := range page {
			page[index] = fmt.Sprintf("%03d.test", next+index)
		}
		next += len(page)
		if _, err := store.AddDomainPage(ctx, "scan", page, next == 100, 0); err != nil {
			t.Fatal(err)
		}
		if active, _ := store.DNSWorkCount(ctx, "scan"); active > capacity {
			t.Fatalf("active DNS work = %d, capacity = %d", active, capacity)
		}
		roots, err := store.ClaimDNS(ctx, "scan", capacity)
		if err != nil {
			t.Fatal(err)
		}
		for _, root := range roots {
			if err := store.CommitDNS(ctx, model.DomainResult{
				ScanID: "scan", RootDomain: root, Status: model.DomainStatusDone, ResolvedAt: time.Now(),
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.AcknowledgeDNS(ctx, "scan", roots); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenAddsQueryCountersToActiveDNSDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE scan_state (
		scan_id TEXT PRIMARY KEY, domain_cursor TEXT NOT NULL DEFAULT '',
		source_exhausted INTEGER NOT NULL DEFAULT 0, domains_fetched INTEGER NOT NULL DEFAULT 0,
		domains_processed INTEGER NOT NULL DEFAULT 0, domain_errors INTEGER NOT NULL DEFAULT 0,
		records_observed INTEGER NOT NULL DEFAULT 0, dns_checks INTEGER NOT NULL DEFAULT 0,
		dns_checks_ok INTEGER NOT NULL DEFAULT 0, started_at TEXT NOT NULL
	);
	INSERT INTO scan_state (scan_id, started_at) VALUES ('scan', '2026-07-11T00:00:00Z');`)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	columns, err := sqliteColumns(context.Background(), store.db, "scan_state")
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"dns_queries", "dns_query_errors", "dns_query_timeouts"} {
		if !columns[column] {
			t.Errorf("scan_state.%s was not added", column)
		}
	}
	if _, err := store.CumulativeStats(context.Background(), "scan"); err != nil {
		t.Fatalf("read upgraded counters: %v", err)
	}
}
