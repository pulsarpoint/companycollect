package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
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
		Records: []model.DNSRecord{{
			Name: "www.a.test", RecordType: "A", TypeCode: 1, ClassCode: 1,
			Value: "1.2.3.4", RDataWire: string([]byte{1, 2, 3, 4, 0}), Discovery: "ct",
		}},
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
	if record := ready.Records[0]; record.TypeCode != 1 || record.ClassCode != 1 || record.RDataWire != string([]byte{1, 2, 3, 4, 0}) {
		t.Errorf("protocol metadata did not survive SQLite: %+v wire=%x", record, record.RDataWire)
	}
}

func TestOpenUpgradesLegacyRecordOutboxesWithoutLosingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `
		CREATE TABLE dns_records (scan_id TEXT, root_domain TEXT, name TEXT, record_type TEXT,
			slot TEXT, value TEXT, ttl INTEGER, priority INTEGER, source TEXT, discovery TEXT,
			rcode TEXT, finding TEXT, source_run_id TEXT, resolved_at TEXT);
		CREATE TABLE axfr_zone_records (scan_id TEXT, root_domain TEXT, name TEXT, record_type TEXT,
			slot TEXT, value TEXT, ttl INTEGER, priority INTEGER, rcode TEXT, discovery TEXT,
			observed_at TEXT);
		INSERT INTO dns_records VALUES ('scan','example.com','www.example.com','A','www',
			'192.0.2.1',60,0,'query','static','NOERROR','','run','2026-07-11T00:00:00Z');
		INSERT INTO axfr_zone_records VALUES ('scan','example.com','example.com','SOA','',
			'ns.example.com. hostmaster.example.com. 1 2 3 4 5',60,0,'NOERROR','axfr',
			'2026-07-11T00:00:00Z');`
	if _, err := database.Exec(legacy); err != nil {
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
	for _, table := range []string{"dns_records", "axfr_zone_records"} {
		columns, err := sqliteColumns(context.Background(), store.db, table)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"record_type_code", "record_class_code", "rdata_wire", "name_server", "name_server_ip"} {
			if !columns[name] {
				t.Errorf("%s.%s was not added", table, name)
			}
		}
		var count int
		if err := store.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Errorf("%s row count = %d, err=%v", table, count, err)
		}
	}
}

func TestAXFRRecordProtocolFieldsRoundTripThroughSQLite(t *testing.T) {
	ctx := context.Background()
	store := openBoundedTestStore(t)
	if err := store.BeginCycle(ctx, "scan", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddDomainPage(ctx, "scan", []string{"example.com"}, true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimDNS(ctx, "scan", 1); err != nil {
		t.Fatal(err)
	}
	endpoint := model.NameserverEndpoint{Name: "ns.example.com", IP: "192.0.2.53", Scope: "public", Dialable: true}
	if err := store.CommitDNS(ctx, model.DomainResult{
		ScanID: "scan", RootDomain: "example.com", Status: model.DomainStatusDone,
		Endpoints: []model.NameserverEndpoint{endpoint}, ResolvedAt: time.Now().UTC(),
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimAXFR(ctx, "scan", 1); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Unix(100, 0).UTC()
	probe := resolve.AXFROutcome{
		Verdict: resolve.VerdictOpen, Reason: resolve.ReasonTransferred,
		NSHost: endpoint.Name, NSIP: endpoint.IP, ObservedAt: observedAt,
	}
	wire := string([]byte{0xde, 0xad, 0, 0xbe, 0xef})
	zone := []model.DNSRecord{{
		Name: "unknown.example.com", RecordType: "TYPE65400", TypeCode: 65400,
		ClassCode: 65280, Value: `\# 5 DEAD00BEEF`, RDataWire: wire,
		Source: "axfr", Discovery: "axfr", Rcode: "NOERROR",
		NameServer: endpoint.Name, NameServerIP: endpoint.IP,
	}}
	if err := store.CommitAXFR(ctx, "scan", "example.com", []resolve.AXFROutcome{probe}, zone, ""); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.ReadyAXFR(ctx, "scan", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || len(jobs[0].Zone) != 1 {
		t.Fatalf("ready AXFR jobs = %+v", jobs)
	}
	record := jobs[0].Zone[0]
	if record.TypeCode != 65400 || record.ClassCode != 65280 || record.RDataWire != wire ||
		record.NameServer != endpoint.Name || record.NameServerIP != endpoint.IP {
		t.Errorf("AXFR protocol fields did not survive SQLite: %+v wire=%x", record, record.RDataWire)
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
