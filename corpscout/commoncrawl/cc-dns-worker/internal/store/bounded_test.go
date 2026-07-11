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
	store, err := Open(filepath.Join(t.TempDir(), "bounded.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestAddDomainPageCommitsWorkAndCursorTogether(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	startedAt := time.Unix(100, 0).UTC()
	if err := store.BeginCycle(ctx, "scan-1", startedAt); err != nil {
		t.Fatal(err)
	}
	added, err := store.AddDomainPage(ctx, "scan-1", []string{"a.test", "b.test", "c.test"}, false, 2)
	if err != nil {
		t.Fatal(err)
	}
	if added != 2 {
		t.Fatalf("added = %d, want 2", added)
	}
	state, err := store.SourceState(ctx, "scan-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Cursor != "b.test" || !state.SourceExhausted || state.DomainsFetched != 2 {
		t.Errorf("state = %+v, want cursor b.test, exhausted, fetched 2", state)
	}
	if count, err := store.DNSWorkCount(ctx, "scan-1"); err != nil || count != 2 {
		t.Fatalf("work count = %d, err = %v, want 2", count, err)
	}
}

func TestResetRunningDoesNotTouchReadyWork(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddDomainPage(ctx, "scan-1", []string{"a.test", "b.test"}, true, 0); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDNS(ctx, "scan-1", 2)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim = %v, err = %v", claimed, err)
	}
	if err := store.CommitDNS(ctx, model.DomainResult{
		ScanID: "scan-1", RootDomain: "a.test", Status: model.DomainStatusDone,
		SourceRunID: "scan-1", ResolvedAt: time.Now().UTC(),
	}, false); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetRunning(ctx, "scan-1"); err != nil {
		t.Fatal(err)
	}
	counts, err := store.DNSWorkCounts(ctx, "scan-1")
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 1 || counts.Running != 0 || counts.Ready != 1 {
		t.Errorf("counts after reset = %+v", counts)
	}
}

func TestCommitDNSAtomicallyCreatesAXFRWorkAndReadyOutbox(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddDomainPage(ctx, "scan-1", []string{"a.test"}, true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimDNS(ctx, "scan-1", 1); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Unix(200, 0).UTC()
	result := model.DomainResult{
		ScanID: "scan-1", RootDomain: "a.test", Status: model.DomainStatusDone,
		SourceRunID: "scan-1", ResolvedAt: observedAt,
		Endpoints: []model.NameserverEndpoint{
			{Name: "ns.a.test", IP: "1.1.1.1", Scope: "public", Dialable: true},
			{Name: "ns.a.test", IP: "10.0.0.1", Scope: "private", Dialable: false},
		},
		Records: []model.DNSRecord{{Name: "www.a.test", RecordType: "A", Value: "1.2.3.4", Discovery: "ct"}},
	}
	if err := store.CommitDNS(ctx, result, true); err != nil {
		t.Fatal(err)
	}
	dnsCounts, _ := store.DNSWorkCounts(ctx, "scan-1")
	axfrCounts, _ := store.AXFRWorkCounts(ctx, "scan-1")
	if dnsCounts.Ready != 1 || axfrCounts.Pending != 1 {
		t.Fatalf("DNS counts = %+v, AXFR counts = %+v", dnsCounts, axfrCounts)
	}
	targets, err := store.ClaimAXFR(ctx, "scan-1", 1)
	if err != nil || len(targets) != 1 {
		t.Fatalf("targets = %+v, err = %v", targets, err)
	}
	if len(targets[0].Endpoints) != 2 || targets[0].Endpoints[1].Dialable {
		t.Errorf("private endpoint evidence was not preserved: %+v", targets[0].Endpoints)
	}
	ready, err := store.ReadyDNS(ctx, "scan-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.Records) != 1 || len(ready.Summaries) != 1 || len(ready.Hostnames) != 1 {
		t.Errorf("ready batch = %+v", ready)
	}
}

func TestAcknowledgeDNSDeletesOnlySelectedReadyWork(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddDomainPage(ctx, "scan-1", []string{"a.test", "b.test"}, true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimDNS(ctx, "scan-1", 2); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"a.test", "b.test"} {
		if err := store.CommitDNS(ctx, model.DomainResult{
			ScanID: "scan-1", RootDomain: root, Status: model.DomainStatusDone,
			SourceRunID: "scan-1", ResolvedAt: time.Now().UTC(),
		}, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AcknowledgeDNS(ctx, "scan-1", []string{"a.test"}); err != nil {
		t.Fatal(err)
	}
	counts, _ := store.DNSWorkCounts(ctx, "scan-1")
	if counts.Ready != 1 {
		t.Errorf("ready count = %d, want 1", counts.Ready)
	}
}

func TestCorpusTenTimesCapacityCompletesWithoutExceedingBound(t *testing.T) {
	ctx := context.Background()
	localStore := openBoundedTestStore(t)
	const capacity = 10
	const corpusSize = capacity * 10
	if err := localStore.BeginCycle(ctx, "scan-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	nextDomain := 0
	peak := 0
	for nextDomain < corpusSize {
		active, err := localStore.DNSWorkCount(ctx, "scan-1")
		if err != nil {
			t.Fatal(err)
		}
		available := capacity - active
		page := make([]string, available)
		for index := range page {
			page[index] = fmt.Sprintf("%03d.test", nextDomain+index)
		}
		nextDomain += len(page)
		if _, err := localStore.AddDomainPage(ctx, "scan-1", page, nextDomain == corpusSize, 0); err != nil {
			t.Fatal(err)
		}
		active, _ = localStore.DNSWorkCount(ctx, "scan-1")
		peak = max(peak, active)
		roots, err := localStore.ClaimDNS(ctx, "scan-1", capacity)
		if err != nil {
			t.Fatal(err)
		}
		for _, root := range roots {
			if err := localStore.CommitDNS(ctx, model.DomainResult{
				ScanID: "scan-1", RootDomain: root, Status: model.DomainStatusDone,
				SourceRunID: "scan-1", ResolvedAt: time.Now().UTC(),
			}, false); err != nil {
				t.Fatal(err)
			}
		}
		if err := localStore.AcknowledgeDNS(ctx, "scan-1", roots); err != nil {
			t.Fatal(err)
		}
	}
	if peak > capacity {
		t.Errorf("peak active DNS work = %d, capacity = %d", peak, capacity)
	}
	state, err := localStore.SourceState(ctx, "scan-1")
	if err != nil {
		t.Fatal(err)
	}
	if !state.SourceExhausted || state.DomainsFetched != corpusSize {
		t.Errorf("final source state = %+v", state)
	}
}
