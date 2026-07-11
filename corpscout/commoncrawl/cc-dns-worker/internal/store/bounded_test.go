package store

import (
	"context"
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
	if stats.Domains != 1 || stats.Records != 1 || stats.DNSChecks != 0 || stats.DomainErrors != 0 {
		t.Fatalf("cumulative stats = %+v", stats)
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
