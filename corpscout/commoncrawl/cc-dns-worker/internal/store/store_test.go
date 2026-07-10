package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "scan.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSeedAndPending(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	n, err := s.Seed(ctx, "sc", []string{"a.com", "b.com", "c.com"})
	if err != nil || n != 3 {
		t.Fatalf("seed n=%d err=%v", n, err)
	}
	// Re-seeding is idempotent.
	n2, _ := s.Seed(ctx, "sc", []string{"a.com", "d.com"})
	if n2 != 1 {
		t.Errorf("re-seed added %d, want 1 (only d.com)", n2)
	}
	pend, _ := s.Pending(ctx, "sc")
	if len(pend) != 4 {
		t.Fatalf("pending = %d, want 4", len(pend))
	}
}

func TestSeedCompleteMarker(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	// Unmarked scan-id reads as not complete (so a fresh/interrupted seed re-streams).
	if done, err := s.SeedComplete(ctx, "sc"); err != nil || done {
		t.Fatalf("fresh SeedComplete = %v, err=%v; want false", done, err)
	}
	if err := s.MarkSeedComplete(ctx, "sc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if done, err := s.SeedComplete(ctx, "sc"); err != nil || !done {
		t.Fatalf("after mark SeedComplete = %v, err=%v; want true", done, err)
	}
	// The marker is per scan-id; a different scan-id is unaffected.
	if done, _ := s.SeedComplete(ctx, "other"); done {
		t.Errorf("unrelated scan-id reported seeded")
	}
	// Marking twice is idempotent (upsert), not an error.
	if err := s.MarkSeedComplete(ctx, "sc"); err != nil {
		t.Errorf("re-mark: %v", err)
	}
}

func TestCommitBatchAndResume(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "scan.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	err = s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", ETLD: "com", Status: "done",
		Nameservers: []string{"ns1.a.com"}, NSIPs: []string{"1.1.1.1"},
		QueriesTotal: 10, QueriesOK: 9, ResolvedAt: now, SourceRunID: "run-xyz",
		Records: []model.DNSRecord{{Name: "a.com", RecordType: "MX", Value: "mail.a.com", Priority: 10, Rcode: "NOERROR"}},
	}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: a.com is done, so only b.com remains pending (the resume contract).
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	pend, _ := s2.Pending(ctx, "sc")
	if len(pend) != 1 || pend[0] != "b.com" {
		t.Fatalf("pending after resume = %v, want [b.com]", pend)
	}
	recs, _ := s2.StagedRecords(ctx, "sc")
	if len(recs) != 1 || recs[0].RecordType != "MX" || recs[0].Priority != 10 {
		t.Fatalf("staged records = %+v", recs)
	}
	// last_run_id carries the run-id threaded through DomainResult, not the scan_id (they can
	// legitimately differ: --run-id lets multiple scan-ids share one logical run).
	if recs[0].LastRunID != "run-xyz" {
		t.Errorf("staged record last_run_id = %q, want %q (not scan_id %q)", recs[0].LastRunID, "run-xyz", "sc")
	}
	// Distinct model: first_seen == last_seen == resolved_at, scans == 1 on a fresh insert.
	if recs[0].Scans != 1 || !recs[0].FirstSeen.Equal(recs[0].LastSeen) {
		t.Errorf("staged record scans/first/last wrong: %+v", recs[0])
	}
	rows, _ := s2.StagedDomains(ctx, "sc")
	if len(rows) != 1 || rows[0].RootDomain != "a.com" || len(rows[0].Nameservers) != 1 {
		t.Fatalf("staged domains = %+v", rows)
	}
	if rows[0].LastRunID != "run-xyz" {
		t.Errorf("staged domain last_run_id = %q, want %q (not scan_id %q)", rows[0].LastRunID, "run-xyz", "sc")
	}
}

func TestIncrementalStaging(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com", "c.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	commit := func(d string) {
		if err := s.CommitBatch(ctx, []model.DomainResult{{
			ScanID: "sc", RootDomain: d, Status: "done", SourceRunID: "run1", ResolvedAt: now,
			Nameservers: []string{"ns." + d}, NSIPs: []string{"1.2.3.4"},
			Records: []model.DNSRecord{{Name: d, RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR"}},
		}}); err != nil {
			t.Fatalf("commit %s: %v", d, err)
		}
	}

	if w, _ := s.LoadedRowid(ctx, "sc"); w != 0 {
		t.Fatalf("initial watermark = %d, want 0", w)
	}
	commit("a.com")
	commit("b.com")

	recs, maxRowid, domains, err := s.RecordsAfter(ctx, "sc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || maxRowid == 0 || len(domains) != 2 {
		t.Fatalf("RecordsAfter(0): recs=%d maxRowid=%d domains=%v", len(recs), maxRowid, domains)
	}
	if recs[0].Scans != 1 || !recs[0].FirstSeen.Equal(recs[0].LastSeen) || recs[0].LastRunID != "run1" {
		t.Errorf("record shape wrong: %+v", recs[0])
	}

	if err := s.SetLoadedRowid(ctx, "sc", maxRowid); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.LoadedRowid(ctx, "sc"); w != maxRowid {
		t.Fatalf("watermark after set = %d, want %d", w, maxRowid)
	}
	// Nothing new past the watermark yet.
	if recs2, _, _, _ := s.RecordsAfter(ctx, "sc", maxRowid, 100); len(recs2) != 0 {
		t.Fatalf("RecordsAfter(watermark) = %d, want 0", len(recs2))
	}
	// A late-committing domain shows up next — the watermark walks commit order, never misses it.
	commit("c.com")
	recs3, max3, doms3, _ := s.RecordsAfter(ctx, "sc", maxRowid, 100)
	if len(recs3) != 1 || recs3[0].RootDomain != "c.com" || max3 <= maxRowid || len(doms3) != 1 {
		t.Fatalf("incremental after watermark: recs=%d (%+v) max=%d", len(recs3), recs3, max3)
	}

	if n, _ := s.PendingCount(ctx, "sc"); n != 0 {
		t.Errorf("PendingCount = %d, want 0 (all done)", n)
	}
	if sums, _ := s.SummariesFor(ctx, "sc", []string{"a.com", "c.com"}); len(sums) != 2 {
		t.Errorf("SummariesFor = %d, want 2", len(sums))
	}
}

func TestCommitBatchIsIdempotentPerDomain(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	_, _ = s.Seed(ctx, "sc", []string{"a.com"})
	now := time.Unix(0, 0).UTC()
	res := model.DomainResult{ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: now,
		Records: []model.DNSRecord{{Name: "a.com", RecordType: "A", Slot: "@", Value: "1.2.3.4", Rcode: "NOERROR"}}}
	_ = s.CommitBatch(ctx, []model.DomainResult{res})
	_ = s.CommitBatch(ctx, []model.DomainResult{res}) // re-commit must not duplicate
	recs, _ := s.StagedRecords(ctx, "sc")
	if len(recs) != 1 {
		t.Fatalf("records after double-commit = %d, want 1", len(recs))
	}
}

func TestPendingExcludesErrorStatus(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "error", Error: "timeout", ResolvedAt: now,
	}})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	pend, err := s.Pending(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(pend) != 1 || pend[0] != "b.com" {
		t.Fatalf("pending = %v, want [b.com] (a.com is status=error, must be excluded)", pend)
	}

	// Error domains are NOT staged for the CH summary (done-only), so a failed re-scan can never
	// clobber a domain's last-good state — a.com errored, so it produces no summary row.
	rows, err := s.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("staged domains = %+v, want none (error domains are not loaded to the summary)", rows)
	}
}

func TestCommitBatchUnseededDomainErrors(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	// Seed a different domain; "unseeded.com" is never seeded.
	if _, err := s.Seed(ctx, "sc", []string{"seeded.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	res := model.DomainResult{
		ScanID: "sc", RootDomain: "unseeded.com", Status: "done", ResolvedAt: now,
		Records: []model.DNSRecord{{Name: "unseeded.com", RecordType: "A", Slot: "@", Value: "1.2.3.4", Rcode: "NOERROR"}},
	}
	if err := s.CommitBatch(ctx, []model.DomainResult{res}); err == nil {
		t.Fatal("CommitBatch on an unseeded domain: want error, got nil")
	}

	recs, err := s.StagedRecords(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.RootDomain == "unseeded.com" {
			t.Fatalf("found record for unseeded.com after failed commit was rolled back: %+v", r)
		}
	}
	if len(recs) != 0 {
		t.Fatalf("staged records = %d, want 0 (rollback should have undone the insert)", len(recs))
	}
}

func TestCommitBatchPersistsSource(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.Seed(ctx, "s1", []string{"example.com"}); err != nil {
		t.Fatal(err)
	}
	res := model.DomainResult{
		ScanID: "s1", RootDomain: "example.com", Status: "done", ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{
			{Name: "example.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR", Source: "query"},
			{Name: "vpn.example.com", RecordType: "A", Value: "5.6.7.8", Rcode: "NOERROR", Source: "axfr"},
		},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedRecords(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Name] = r.Source
	}
	if got["example.com"] != "query" || got["vpn.example.com"] != "axfr" {
		t.Fatalf("source not round-tripped: %+v", got)
	}
}

func TestCommitBatchPersistsDiscovery(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.Seed(ctx, "s1", []string{"example.com"}); err != nil {
		t.Fatal(err)
	}
	res := model.DomainResult{
		ScanID: "s1", RootDomain: "example.com", Status: "done", ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{
			{Name: "www.example.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR", Source: "query", Discovery: "static"},
			{Name: "jenkins.example.com", RecordType: "A", Value: "10.0.0.5", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"},
		},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedRecords(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Name] = r.Discovery
	}
	if got["www.example.com"] != "static" || got["jenkins.example.com"] != "axfr" {
		t.Fatalf("discovery not round-tripped: %+v", got)
	}
}

func TestDiscoveredHostnames(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.Seed(ctx, "s1", []string{"example.com"}); err != nil {
		t.Fatal(err)
	}
	res := model.DomainResult{
		ScanID: "s1", RootDomain: "example.com", Status: "done", ResolvedAt: time.Now().UTC(),
		Records: []model.DNSRecord{
			{Name: "example.com", RecordType: "A", Value: "1.1.1.1", Rcode: "NOERROR", Source: "query", Discovery: "static"},             // apex — excluded
			{Name: "www.example.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR", Source: "query", Discovery: "static"},         // static — excluded
			{Name: "jenkins.example.com", RecordType: "A", Value: "10.0.0.5", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"},       // captured
			{Name: "vpn.example.com", RecordType: "CNAME", Value: "gw.example.net", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"}, // captured
			{Name: "a.b.example.com", RecordType: "A", Value: "10.0.0.6", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"},           // deep — captured as label "a.b"
			{Name: "*.example.com", RecordType: "A", Value: "10.0.0.7", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"},             // wildcard — excluded
		},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.DiscoveredHostnames(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Label] = r.DiscoverySource
	}
	if _, ok := got["*"]; ok {
		t.Fatalf("wildcard label '*' must be excluded, got %+v", got)
	}
	if len(got) != 3 || got["jenkins"] != "axfr" || got["vpn"] != "axfr" || got["a.b"] != "axfr" {
		t.Fatalf("want {jenkins:axfr, vpn:axfr, a.b:axfr}, got %+v", got)
	}
}

func TestPendingBatchCursor(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	// seed a..e; mark b done and c error, so the not-done set (in order) is a, d, e.
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com", "c.com", "d.com", "e.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	mark := func(domain, status string) {
		if err := s.CommitBatch(ctx, []model.DomainResult{{ScanID: "sc", RootDomain: domain, Status: status, ResolvedAt: now}}); err != nil {
			t.Fatalf("commit %s: %v", domain, err)
		}
	}
	mark("b.com", "done")
	mark("c.com", "error")

	// Walk the not-done set two at a time via the cursor.
	b1, err := s.PendingBatch(ctx, "sc", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(b1) != 2 || b1[0] != "a.com" || b1[1] != "d.com" {
		t.Fatalf("batch1 = %v, want [a.com d.com]", b1)
	}
	b2, _ := s.PendingBatch(ctx, "sc", b1[len(b1)-1], 2)
	if len(b2) != 1 || b2[0] != "e.com" {
		t.Fatalf("batch2 = %v, want [e.com]", b2)
	}
	b3, _ := s.PendingBatch(ctx, "sc", b2[len(b2)-1], 2)
	if len(b3) != 0 {
		t.Fatalf("batch3 = %v, want empty (terminates)", b3)
	}
}

func TestHostnamesRoundTrip(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.InsertHostnamesMap(ctx, "s1", map[string][]model.HostLabel{"example.com": {
		{Label: "jenkins", DiscoverySource: "axfr", LiveCert: false},
		{Label: "api", DiscoverySource: "ct", LiveCert: true},
	}}); err != nil {
		t.Fatal(err)
	}
	m, err := st.HostnamesForBatch(ctx, "s1", []string{"example.com", "other.com"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m["example.com"]) != 2 {
		t.Fatalf("want 2 labels for example.com, got %d", len(m["example.com"]))
	}
	got := map[string]string{}
	for _, h := range m["example.com"] {
		got[h.Label] = h.DiscoverySource
	}
	if got["jenkins"] != "axfr" || got["api"] != "ct" {
		t.Fatalf("labels not round-tripped: %+v", got)
	}
}

// Host-load is sharded: each shard is tracked complete independently (a restart re-runs only the
// unfinished shards) and re-inserting a shard's rows is idempotent (INSERT OR IGNORE dedups).
func TestHostLoadShardStateAndIdempotency(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	// A shard is not complete until marked; marking is observable and per-shard.
	if done, _ := st.HostLoadShardComplete(ctx, "s1", 3); done {
		t.Fatal("shard 3 should start incomplete")
	}
	if err := st.MarkHostLoadShardComplete(ctx, "s1", 3); err != nil {
		t.Fatal(err)
	}
	if done, _ := st.HostLoadShardComplete(ctx, "s1", 3); !done {
		t.Fatal("shard 3 should be complete after marking")
	}
	if done, _ := st.HostLoadShardComplete(ctx, "s1", 4); done {
		t.Fatal("shard 4 must remain incomplete (per-shard, not global)")
	}

	// First insert adds all rows; re-inserting the same map adds nothing (idempotent resume).
	m := map[string][]model.HostLabel{
		"a.com": {{Label: "www", DiscoverySource: "ct", LiveCert: true}, {Label: "api", DiscoverySource: "ct"}},
		"b.com": {{Label: "vpn", DiscoverySource: "axfr"}},
	}
	n1, err := st.InsertHostnamesMap(ctx, "s1", m)
	if err != nil || n1 != 3 {
		t.Fatalf("first insert added %d (want 3), err=%v", n1, err)
	}
	n2, err := st.InsertHostnamesMap(ctx, "s1", m)
	if err != nil || n2 != 0 {
		t.Fatalf("re-insert added %d (want 0 — idempotent), err=%v", n2, err)
	}
}

// axfrDone(t)(scanID, domain) is a tiny helper: seeds a resolved scan_domains row with one dialable
// endpoint and stages it into the axfr_domains queue, returning nothing — callers commit or inspect it.
func seedResolvedAXFRDomain(t *testing.T, st *Store, scanID, domain, ip string) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
		t.Fatalf("seed %s: %v", domain, err)
	}
	eps := []model.NameserverEndpoint{{Name: "ns1." + domain, IP: ip, Scope: "public", Dialable: true}}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: scanID, RootDomain: domain, Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		NSIPs: []string{ip}, Endpoints: eps,
	}}); err != nil {
		t.Fatalf("commit %s: %v", domain, err)
	}
	if _, err := st.SeedAXFRDomains(ctx, scanID); err != nil {
		t.Fatalf("SeedAXFRDomains: %v", err)
	}
}

// TestSeedAXFRDomainsIdempotent proves SeedAXFRDomains only stages resolved (status='done') domains,
// re-seeding adds nothing new, and — critically — an already-'done' axfr_domains row is never reset back
// to 'pending' by a later re-seed (e.g. after the scan re-resolves the same domain in a later cycle).
func TestSeedAXFRDomainsIdempotent(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	seedResolvedAXFRDomain(t, st, "sc", "a.com", "9.9.9.9")
	seedResolvedAXFRDomain(t, st, "sc", "b.com", "9.9.9.9")

	// Re-seeding is idempotent: no new rows, same pending set.
	n, err := st.SeedAXFRDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("re-seed added %d rows, want 0 (idempotent)", n)
	}
	targets, err := st.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("pending after seed = %d, want 2", len(targets))
	}

	// Mark a.com done, then re-seed: the done row must NOT be reset to pending.
	if err := st.CommitAXFRDomain(ctx, "sc", "a.com", nil, nil, "run1", time.Unix(0, 0).UTC(), ""); err != nil {
		t.Fatalf("CommitAXFRDomain: %v", err)
	}
	if _, err := st.SeedAXFRDomains(ctx, "sc"); err != nil {
		t.Fatal(err)
	}
	after, err := st.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0].RootDomain != "b.com" {
		t.Fatalf("pending after marking a.com done and re-seeding = %+v, want only b.com (done rows must not reset to pending)", after)
	}
}

// TestPendingAXFRDomainsResume proves PendingAXFRDomains returns only the not-yet-'done' rows: marking
// some domains done via CommitAXFRDomain removes them from the pending set, exactly what lets a resumed
// run pick up only the unfinished work.
func TestPendingAXFRDomainsResume(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for _, d := range []string{"a.com", "b.com", "c.com"} {
		seedResolvedAXFRDomain(t, st, "sc", d, "9.9.9.9")
	}
	if err := st.CommitAXFRDomain(ctx, "sc", "b.com", nil, nil, "run1", time.Unix(0, 0).UTC(), ""); err != nil {
		t.Fatalf("CommitAXFRDomain: %v", err)
	}

	pend, err := st.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, target := range pend {
		got[target.RootDomain] = true
	}
	if len(got) != 2 || !got["a.com"] || !got["c.com"] || got["b.com"] {
		t.Fatalf("pending = %+v, want {a.com, c.com} (b.com is done, excluded)", got)
	}
}

// TestCommitAXFRDomainAtomicity proves a commit failure rolls back EVERYTHING it touched in that one
// transaction — including the axfr_probes rows and zone scan_records the failing call itself inserted —
// not just the final status flip. Domain "unseeded.com" was never staged into axfr_domains (SeedAXFRDomains
// never ran for it), so the terminal UPDATE affects zero rows and CommitAXFRDomain must fail; the earlier
// INSERTs in that same transaction must not survive.
func TestCommitAXFRDomainAtomicity(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	probes := []resolve.AXFROutcome{{Verdict: resolve.VerdictOpen, Reason: resolve.ReasonTransferred, NSHost: "ns1.unseeded.com", NSIP: "9.9.9.9", Records: 3}}
	zone := []model.DNSRecord{{Name: "vpn.unseeded.com", RecordType: "A", Value: "10.0.0.1", Source: "axfr", Discovery: "axfr"}}
	err = st.CommitAXFRDomain(ctx, "sc", "unseeded.com", probes, zone, "run1", time.Unix(0, 0).UTC(), "")
	if err == nil {
		t.Fatal("CommitAXFRDomain on a domain never staged into axfr_domains: want error, got nil")
	}

	var probeN, recN int
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_probes WHERE scan_id='sc' AND root_domain='unseeded.com'`).Scan(&probeN); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM scan_records WHERE scan_id='sc' AND root_domain='unseeded.com'`).Scan(&recN); err != nil {
		t.Fatal(err)
	}
	if probeN != 0 || recN != 0 {
		t.Fatalf("rollback left rows behind: axfr_probes=%d scan_records=%d, want 0 each", probeN, recN)
	}
}

// TestCommitAXFRDomainAtomicityCanceledContext is a second, independent atomicity check: a context
// that's already canceled fails CommitAXFRDomain at BeginTx (before any write), so a domain otherwise
// eligible to commit is left untouched and still 'pending'.
func TestCommitAXFRDomainAtomicityCanceledContext(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seedResolvedAXFRDomain(t, st, "sc", "a.com", "9.9.9.9")

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	probes := []resolve.AXFROutcome{{Verdict: resolve.VerdictClosed, Reason: resolve.ReasonRefused, NSHost: "ns1.a.com", NSIP: "9.9.9.9"}}
	if err := st.CommitAXFRDomain(canceled, "sc", "a.com", probes, nil, "run1", time.Unix(0, 0).UTC(), ""); err == nil {
		t.Fatal("CommitAXFRDomain with a canceled context: want error, got nil")
	}

	pend, err := st.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pend) != 1 || pend[0].RootDomain != "a.com" {
		t.Fatalf("a.com must still be pending after the failed commit, got %+v", pend)
	}
	var n int
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_probes WHERE scan_id='sc' AND root_domain='a.com'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("axfr_probes rows leaked from the failed commit: %d, want 0", n)
	}
}

// TestAXFRQueueKillAfterDispatchBeforeCommit simulates a crash between a domain being dispatched to a
// worker and its result being committed: after SeedAXFRDomains, with NO CommitAXFRDomain call at all,
// every seeded domain must still show up as pending on the next read — the queue itself (not an
// in-memory dispatch position) is the durable resume state, so nothing is silently skipped.
func TestAXFRQueueKillAfterDispatchBeforeCommit(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for _, d := range []string{"a.com", "b.com"} {
		seedResolvedAXFRDomain(t, st, "sc", d, "9.9.9.9")
	}
	// Simulate dispatch by just reading a page (a real feeder would send these to workers); no commit
	// follows, mirroring a process killed mid-probe.
	if _, err := st.PendingAXFRDomains(ctx, "sc", "", 100); err != nil {
		t.Fatal(err)
	}

	pend, err := st.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pend) != 2 {
		t.Fatalf("pending after dispatch-without-commit = %d, want 2 (both still pending, none skipped)", len(pend))
	}
	if done, _ := st.AXFRQueueComplete(ctx, "sc"); done {
		t.Fatal("AXFRQueueComplete = true, want false (nothing has been committed yet)")
	}
}

// TestAXFRQueueCompleteAfterAllDone proves that once every seeded domain is committed, the queue is
// reported complete and re-running finds zero pending work — the state a re-run of an already-finished
// phase must observe to make no further network requests.
func TestAXFRQueueCompleteAfterAllDone(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for _, d := range []string{"a.com", "b.com"} {
		seedResolvedAXFRDomain(t, st, "sc", d, "9.9.9.9")
	}
	if done, _ := st.AXFRQueueComplete(ctx, "sc"); done {
		t.Fatal("AXFRQueueComplete = true before anything committed, want false")
	}
	for _, d := range []string{"a.com", "b.com"} {
		if err := st.CommitAXFRDomain(ctx, "sc", d, nil, nil, "run1", time.Unix(0, 0).UTC(), ""); err != nil {
			t.Fatalf("commit %s: %v", d, err)
		}
	}

	done, err := st.AXFRQueueComplete(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("AXFRQueueComplete = false after all domains committed, want true")
	}
	pend, err := st.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(pend) != 0 {
		t.Fatalf("pending after full completion = %+v, want none", pend)
	}

	// A brand-new scan-id that was never seeded also reads as complete (a legitimate no-op).
	if done, _ := st.AXFRQueueComplete(ctx, "never-seeded"); !done {
		t.Error("AXFRQueueComplete for an unseeded scan-id = false, want true (zero rows == complete)")
	}
}

// TestCommitAXFRDomainWritesProbesAndZone proves a successful commit lands both the per-endpoint probe
// rows (verdict/reason/records/bytes/truncated round-tripped) and the selected zone's records tagged
// source='axfr', and flips the domain to 'done'.
func TestCommitAXFRDomainWritesProbesAndZone(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seedResolvedAXFRDomain(t, st, "sc", "open.com", "9.9.9.9")

	probes := []resolve.AXFROutcome{
		{Verdict: resolve.VerdictOpen, Reason: resolve.ReasonTransferred, NSHost: "ns1.open.com", NSIP: "9.9.9.9", Records: 5, Bytes: 512, ObservedAt: time.Unix(100, 0).UTC()},
		{Verdict: resolve.VerdictClosed, Reason: resolve.ReasonRefused, NSHost: "ns2.open.com", NSIP: "8.8.8.8", ObservedAt: time.Unix(100, 0).UTC()},
	}
	zone := []model.DNSRecord{{Name: "vpn.open.com", RecordType: "A", Value: "10.0.0.1", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"}}
	if err := st.CommitAXFRDomain(ctx, "sc", "open.com", probes, zone, "run1", time.Unix(200, 0).UTC(), ""); err != nil {
		t.Fatalf("CommitAXFRDomain: %v", err)
	}

	rows, err := st.db.QueryContext(ctx, `SELECT name_server, name_server_ip, verdict, reason, records, bytes
		FROM axfr_probes WHERE scan_id='sc' AND root_domain='open.com' ORDER BY name_server_ip`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string][2]string{}
	n := 0
	for rows.Next() {
		var ns, ip, verdict, reason string
		var recs, bytesN int
		if err := rows.Scan(&ns, &ip, &verdict, &reason, &recs, &bytesN); err != nil {
			t.Fatal(err)
		}
		got[ip] = [2]string{verdict, reason}
		n++
	}
	if n != 2 {
		t.Fatalf("axfr_probes rows = %d, want 2", n)
	}
	if got["9.9.9.9"] != [2]string{"open", "transferred"} {
		t.Errorf("probe for 9.9.9.9 = %+v, want open/transferred", got["9.9.9.9"])
	}
	if got["8.8.8.8"] != [2]string{"closed", "refused"} {
		t.Errorf("probe for 8.8.8.8 = %+v, want closed/refused", got["8.8.8.8"])
	}

	var recN int
	var source, runID string
	if err := st.db.QueryRowContext(ctx, `SELECT count(*), source, source_run_id FROM scan_records
		WHERE scan_id='sc' AND root_domain='open.com' AND name='vpn.open.com'`).Scan(&recN, &source, &runID); err != nil {
		t.Fatal(err)
	}
	if recN != 1 || source != "axfr" || runID != "run1" {
		t.Fatalf("zone record = count=%d source=%q run=%q, want 1/axfr/run1", recN, source, runID)
	}

	var status string
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM axfr_domains WHERE scan_id='sc' AND root_domain='open.com'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("axfr_domains.status = %q, want done", status)
	}
}

// TestCommitAXFRDomainReplacesOnRecommit proves re-committing a domain (resume, or a deliberate re-probe)
// replaces its probes/zone records wholesale rather than accumulating duplicates.
func TestCommitAXFRDomainReplacesOnRecommit(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seedResolvedAXFRDomain(t, st, "sc", "open.com", "9.9.9.9")

	first := []resolve.AXFROutcome{{Verdict: resolve.VerdictOpen, Reason: resolve.ReasonTransferred, NSHost: "ns1.open.com", NSIP: "9.9.9.9"}}
	firstZone := []model.DNSRecord{{Name: "old.open.com", RecordType: "A", Value: "10.0.0.1", Source: "axfr", Discovery: "axfr"}}
	if err := st.CommitAXFRDomain(ctx, "sc", "open.com", first, firstZone, "run1", time.Unix(0, 0).UTC(), ""); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	second := []resolve.AXFROutcome{{Verdict: resolve.VerdictClosed, Reason: resolve.ReasonRefused, NSHost: "ns1.open.com", NSIP: "9.9.9.9"}}
	if err := st.CommitAXFRDomain(ctx, "sc", "open.com", second, nil, "run2", time.Unix(0, 0).UTC(), ""); err != nil {
		t.Fatalf("second commit: %v", err)
	}

	var probeN int
	var verdict string
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_probes WHERE scan_id='sc' AND root_domain='open.com'`).Scan(&probeN); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT verdict FROM axfr_probes WHERE scan_id='sc' AND root_domain='open.com' AND name_server_ip='9.9.9.9'`).Scan(&verdict); err != nil {
		t.Fatal(err)
	}
	if probeN != 1 || verdict != "closed" {
		t.Fatalf("after re-commit: %d probe rows, verdict=%q, want 1 row verdict=closed (replaced, not accumulated)", probeN, verdict)
	}

	var recN int
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM scan_records WHERE scan_id='sc' AND root_domain='open.com' AND source='axfr'`).Scan(&recN); err != nil {
		t.Fatal(err)
	}
	if recN != 0 {
		t.Fatalf("stale zone record from the first commit survived the second (zone=nil) commit: %d rows, want 0", recN)
	}
}

// RecordsAfter must page by walking the rowid primary key up from the watermark. If the query
// planner instead picks the (scan_id, root_domain) covering index, every batch re-reads and
// re-sorts the scan's entire record set (temp B-tree), which collapses incremental-load
// throughput on multi-GB stages.
func TestRecordsAfterUsesRowidScan(t *testing.T) {
	s := openTemp(t)
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+recordsAfterQuery, "sc", 0, 10)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	joined := strings.Join(plan, "; ")
	if !strings.Contains(joined, "USING INTEGER PRIMARY KEY (rowid>?)") {
		t.Errorf("plan does not walk the rowid primary key: %s", joined)
	}
	if strings.Contains(joined, "TEMP B-TREE") {
		t.Errorf("plan sorts with a temp B-tree instead of reading in rowid order: %s", joined)
	}
}

// TestNameserverEndpointsRoundTrip proves the ns_endpoints JSON column round-trips a mix of IPv4 and
// IPv6 endpoints intact: Name, canonical IP, Scope, and Dialable must all survive a commit + reopen.
func TestNameserverEndpointsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scan.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Seed(context.Background(), "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	want := []model.NameserverEndpoint{
		{Name: "ns1.a.com", IP: "1.1.1.1", Scope: "public", Dialable: true},
		{Name: "ns1.a.com", IP: "10.0.0.1", Scope: "private", Dialable: false},
		{Name: "ns2.a.com", IP: "2606:4700:4700::1111", Scope: "public", Dialable: true},
	}
	res := model.DomainResult{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		Nameservers: []string{"ns1.a.com", "ns2.a.com"},
		NSIPs:       []string{"1.1.1.1", "10.0.0.1", "2606:4700:4700::1111"},
		Endpoints:   want,
	}
	if err := s.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — the round trip must survive a fresh connection, not just an in-memory read-back.
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	rows, err := s2.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("staged domains = %d, want 1", len(rows))
	}
	got := rows[0].Endpoints
	if len(got) != len(want) {
		t.Fatalf("Endpoints = %+v, want %+v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("Endpoints[%d] = %+v, want %+v", i, got[i], w)
		}
	}
}

// TestNameserverEndpointsChangeOnRecommit proves that re-committing a domain whose delegation changed
// (an endpoint removed, another added) is distinguishable in storage: CommitBatch replaces the whole
// scan_domains row, so the stored ns_endpoints must reflect only the NEW delegation, not a union of old
// and new.
func TestNameserverEndpointsChangeOnRecommit(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	first := []model.NameserverEndpoint{
		{Name: "ns1.a.com", IP: "1.1.1.1", Scope: "public", Dialable: true},
		{Name: "ns2.a.com", IP: "2.2.2.2", Scope: "public", Dialable: true},
	}
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: now,
		NSIPs: []string{"1.1.1.1", "2.2.2.2"}, Endpoints: first,
	}}); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	rows, err := s.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].Endpoints) != 2 {
		t.Fatalf("after first commit: %+v", rows)
	}

	// Re-resolve: ns2.a.com's address changed and a third NS/IP was added — a re-delegation, not a
	// superset.
	second := []model.NameserverEndpoint{
		{Name: "ns1.a.com", IP: "1.1.1.1", Scope: "public", Dialable: true},
		{Name: "ns2.a.com", IP: "3.3.3.3", Scope: "public", Dialable: true},
		{Name: "ns3.a.com", IP: "4.4.4.4", Scope: "public", Dialable: true},
	}
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: now,
		NSIPs: []string{"1.1.1.1", "3.3.3.3", "4.4.4.4"}, Endpoints: second,
	}}); err != nil {
		t.Fatalf("second commit: %v", err)
	}

	rows2, err := s.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows2) != 1 {
		t.Fatalf("after second commit: %d rows, want 1", len(rows2))
	}
	got := rows2[0].Endpoints
	if len(got) != len(second) {
		t.Fatalf("Endpoints after re-commit = %+v, want the new delegation %+v (not a union with the old one)", got, second)
	}
	for i, w := range second {
		if got[i] != w {
			t.Errorf("Endpoints[%d] = %+v, want %+v", i, got[i], w)
		}
	}
	for _, ep := range got {
		if ep.IP == "2.2.2.2" {
			t.Errorf("stale endpoint %+v from the old delegation survived the re-commit", ep)
		}
	}
}

// TestPendingAXFRDomainsUsesEndpoints proves the AXFR feeder reads back the same per-(NS,IP) identity
// that was committed to scan_domains, endpoints intact (not just a flattened IP list), once the domain
// has been staged into the axfr_domains queue via SeedAXFRDomains.
func TestPendingAXFRDomainsUsesEndpoints(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	eps := []model.NameserverEndpoint{
		{Name: "ns1.a.com", IP: "9.9.9.9", Scope: "public", Dialable: true},
		{Name: "ns2.a.com", IP: "10.0.0.1", Scope: "private", Dialable: false},
	}
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		NSIPs: []string{"9.9.9.9", "10.0.0.1"}, Endpoints: eps,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SeedAXFRDomains(ctx, "sc"); err != nil {
		t.Fatal(err)
	}

	targets, err := s.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].RootDomain != "a.com" {
		t.Fatalf("targets = %+v, want one target for a.com", targets)
	}
	if len(targets[0].Endpoints) != len(eps) {
		t.Fatalf("target Endpoints = %+v, want %+v", targets[0].Endpoints, eps)
	}
	for i, w := range eps {
		if targets[0].Endpoints[i] != w {
			t.Errorf("target Endpoints[%d] = %+v, want %+v", i, targets[0].Endpoints[i], w)
		}
	}
}

// TestPendingAXFRDomainsFallsBackToLegacyNSIPs proves a domain committed before ns_endpoints existed
// (so the column reads back as its '[]' default) still yields an AXFR target, synthesized from ns_ips
// and reclassified via resolve.ClassifyString — a private/loopback/etc. legacy address must come back
// non-dialable, never defaulted to true, so the AXFR phase's !Dialable skip still holds for old rows.
func TestPendingAXFRDomainsFallsBackToLegacyNSIPs(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"legacy.com"}); err != nil {
		t.Fatal(err)
	}
	// Simulate a pre-task-2 commit: NSIPs populated (full evidence, as query.go always wrote), but no
	// Endpoints — CommitBatch marshals a nil slice to JSON "null", exactly the shape ns_endpoints takes
	// on a real pre-existing row.
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "legacy.com", Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		NSIPs: []string{"8.8.8.8", "192.168.1.1"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SeedAXFRDomains(ctx, "sc"); err != nil {
		t.Fatal(err)
	}

	targets, err := s.PendingAXFRDomains(ctx, "sc", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets = %+v, want one target synthesized from legacy ns_ips", targets)
	}
	got := map[string]model.NameserverEndpoint{}
	for _, ep := range targets[0].Endpoints {
		got[ep.IP] = ep
	}
	pub, ok := got["8.8.8.8"]
	if !ok || !pub.Dialable || pub.Scope != string(resolve.ScopePublic) {
		t.Errorf("legacy public IP endpoint = %+v, want Dialable=true Scope=public", pub)
	}
	priv, ok := got["192.168.1.1"]
	if !ok || priv.Dialable || priv.Scope != string(resolve.ScopePrivate) {
		t.Errorf("legacy private IP endpoint = %+v, want Dialable=false Scope=private (never default to dialable)", priv)
	}
}

// TestStageAXFRChangesUnseenClosedNoChangeButLatestRow proves an endpoint the pipeline has NEVER seen
// before (prior is nil/empty) whose first-ever probe is definitively closed produces NO axfr_changes
// row — closed is the assumed baseline for an unseen endpoint, not a change — while AXFRLoadRows still
// surfaces it (definitive, delegation-active) so the load pass writes a latest row for it.
func TestStageAXFRChangesUnseenClosedNoChangeButLatestRow(t *testing.T) {
	ctx := context.Background()
	st := openTemp(t)
	seedResolvedAXFRDomain(t, st, "sc", "closed.com", "9.9.9.9")

	probes := []resolve.AXFROutcome{{Verdict: resolve.VerdictClosed, Reason: resolve.ReasonRefused,
		NSHost: "ns1.closed.com", NSIP: "9.9.9.9", ObservedAt: time.Unix(100, 0).UTC()}}
	if err := st.CommitAXFRDomain(ctx, "sc", "closed.com", probes, nil, "run1", time.Unix(100, 0).UTC(), ""); err != nil {
		t.Fatalf("commit: %v", err)
	}

	n, err := st.StageAXFRChanges(ctx, "sc", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("StageAXFRChanges added = %d, want 0 (a first-ever closed observation is the baseline, not a change)", n)
	}
	var changeN int
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_changes WHERE scan_id='sc'`).Scan(&changeN); err != nil {
		t.Fatal(err)
	}
	if changeN != 0 {
		t.Fatalf("axfr_changes rows = %d, want 0", changeN)
	}

	eps, err := st.AXFRLoadRows(ctx, "sc", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 1 {
		t.Fatalf("AXFRLoadRows = %+v, want 1 endpoint (no change does not mean no latest row)", eps)
	}
	if eps[0].Verdict != string(resolve.VerdictClosed) || !eps[0].Definitive || !eps[0].DelegationActive {
		t.Errorf("endpoint = %+v, want verdict=closed, definitive=true, delegation-active=true", eps[0])
	}
}

// TestStageAXFRChangesOpenRefusedOpenThreeChanges walks one endpoint through open -> closed -> open
// across three separate scans, feeding StageAXFRChanges the prior definitive state a real load pass
// would have loaded from ClickHouse after each step, and proves every one of the three transitions
// (including the very first, unseen -> open) lands its own axfr_changes row.
func TestStageAXFRChangesOpenRefusedOpenThreeChanges(t *testing.T) {
	ctx := context.Background()
	st := openTemp(t)
	domain, ip, nsHost := "flap.com", "9.9.9.9", "ns1.flap.com"
	key := AXFREndpointKey{RootDomain: domain, NameServer: nsHost, NameServerIP: ip}

	stageScan := func(scanID string, verdict resolve.AXFRVerdict, reason resolve.AXFRReason, prior map[AXFREndpointKey]AXFRPriorState) int {
		t.Helper()
		if _, err := st.Seed(ctx, scanID, []string{domain}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		eps := []model.NameserverEndpoint{{Name: nsHost, IP: ip, Scope: "public", Dialable: true}}
		if err := st.CommitBatch(ctx, []model.DomainResult{{
			ScanID: scanID, RootDomain: domain, Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
			NSIPs: []string{ip}, Endpoints: eps,
		}}); err != nil {
			t.Fatalf("commit domain: %v", err)
		}
		if _, err := st.SeedAXFRDomains(ctx, scanID); err != nil {
			t.Fatalf("seed axfr domains: %v", err)
		}
		probes := []resolve.AXFROutcome{{Verdict: verdict, Reason: reason, NSHost: nsHost, NSIP: ip, ObservedAt: time.Unix(0, 0).UTC()}}
		if err := st.CommitAXFRDomain(ctx, scanID, domain, probes, nil, "run", time.Unix(0, 0).UTC(), ""); err != nil {
			t.Fatalf("commit axfr: %v", err)
		}
		n, err := st.StageAXFRChanges(ctx, scanID, prior)
		if err != nil {
			t.Fatalf("stage: %v", err)
		}
		return n
	}

	if n := stageScan("sc1", resolve.VerdictOpen, resolve.ReasonTransferred, nil); n != 1 {
		t.Fatalf("scan1 (unseen -> open) added %d, want 1", n)
	}
	prior2 := map[AXFREndpointKey]AXFRPriorState{key: {HasDefinitive: true, AXFROpen: true}}
	if n := stageScan("sc2", resolve.VerdictClosed, resolve.ReasonRefused, prior2); n != 1 {
		t.Fatalf("scan2 (open -> closed) added %d, want 1", n)
	}
	prior3 := map[AXFREndpointKey]AXFRPriorState{key: {HasDefinitive: true, AXFROpen: false}}
	if n := stageScan("sc3", resolve.VerdictOpen, resolve.ReasonTransferred, prior3); n != 1 {
		t.Fatalf("scan3 (closed -> open) added %d, want 1", n)
	}

	var total int
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_changes WHERE root_domain = ?`, domain).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("total axfr_changes rows = %d, want 3 (open, closed, open — including the initial open)", total)
	}
}

// TestStageAXFRChangesIdempotentRepeat proves re-staging the SAME scan's probes against the SAME prior
// adds nothing new: the axfr_changes row it would produce has an identical primary key (same scan_id,
// same changed_at, sourced from the same unchanged axfr_probes.observed_at), so INSERT OR IGNORE dedups.
func TestStageAXFRChangesIdempotentRepeat(t *testing.T) {
	ctx := context.Background()
	st := openTemp(t)
	seedResolvedAXFRDomain(t, st, "sc", "idem.com", "9.9.9.9")
	probes := []resolve.AXFROutcome{{Verdict: resolve.VerdictOpen, Reason: resolve.ReasonTransferred,
		NSHost: "ns1.idem.com", NSIP: "9.9.9.9", ObservedAt: time.Unix(50, 0).UTC()}}
	if err := st.CommitAXFRDomain(ctx, "sc", "idem.com", probes, nil, "run1", time.Unix(50, 0).UTC(), ""); err != nil {
		t.Fatal(err)
	}

	n1, err := st.StageAXFRChanges(ctx, "sc", nil)
	if err != nil || n1 != 1 {
		t.Fatalf("first stage: n=%d err=%v, want n=1", n1, err)
	}
	n2, err := st.StageAXFRChanges(ctx, "sc", nil)
	if err != nil || n2 != 0 {
		t.Fatalf("second stage (repeat, same scan/prior): n=%d err=%v, want n=0", n2, err)
	}
	var total int
	if err := st.db.QueryRowContext(ctx, `SELECT count(*) FROM axfr_changes WHERE scan_id='sc' AND root_domain='idem.com'`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("axfr_changes rows after repeat = %d, want 1 (no duplicate)", total)
	}
}

// TestAXFRLoadRowsRemovedFromDelegation proves an endpoint that prior (ClickHouse) state says was last
// seen active, but whose root domain resolved THIS scan to a different NS delegation that no longer
// includes it, is surfaced by AXFRLoadRows as delegation_active=false with no fresh probe (Verdict=""),
// and that StageAXFRChanges never turns a delegation removal into an axfr_changes row (it only ever
// looks at axfr_probes, and the removed endpoint has none this scan).
func TestAXFRLoadRowsRemovedFromDelegation(t *testing.T) {
	ctx := context.Background()
	st := openTemp(t)
	domain := "moved.com"
	staleKey := AXFREndpointKey{RootDomain: domain, NameServer: "ns-old." + domain, NameServerIP: "1.1.1.1"}
	prior := map[AXFREndpointKey]AXFRPriorState{
		staleKey: {HasDefinitive: true, AXFROpen: true, DelegationActive: true},
	}

	// This scan resolves the domain to a brand-new (non-dialable) NS — ns-old is gone entirely — and
	// nothing gets probed.
	if _, err := st.Seed(ctx, "sc", []string{domain}); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: domain, Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		NSIPs: []string{"10.0.0.1"},
		Endpoints: []model.NameserverEndpoint{
			{Name: "ns-new." + domain, IP: "10.0.0.1", Scope: "private", Dialable: false},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SeedAXFRDomains(ctx, "sc"); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitAXFRDomain(ctx, "sc", domain, nil, nil, "run1", time.Unix(0, 0).UTC(), ""); err != nil {
		t.Fatal(err)
	}

	n, err := st.StageAXFRChanges(ctx, "sc", prior)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("StageAXFRChanges added = %d, want 0 (a delegation removal is never a change)", n)
	}

	eps, err := st.AXFRLoadRows(ctx, "sc", prior)
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 1 {
		t.Fatalf("AXFRLoadRows = %+v, want exactly the one removed endpoint", eps)
	}
	if eps[0].AXFREndpointKey != staleKey || eps[0].DelegationActive || eps[0].Verdict != "" {
		t.Errorf("removed endpoint = %+v, want key=%+v delegation_active=false verdict=\"\"", eps[0], staleKey)
	}
}

// TestCompletedDomainsZeroRecordDone proves the Task 8 fix at its core: a domain that resolves
// successfully but produces ZERO scan_records rows still gets a completed_domains row (and so still
// surfaces from CompletedDomainsAfter) — the exact case the old record-batch-piggybacked summary load
// could never see, because such a domain never appears in scan_records at all.
func TestCompletedDomainsZeroRecordDone(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"empty.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "empty.com", Status: "done", ResolvedAt: now,
		// Records intentionally empty: a domain can resolve successfully (NXDOMAIN-free, NS found) yet
		// have nothing worth recording, e.g. every query returned NODATA.
	}}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// It never appears in scan_records...
	recs, err := s.StagedRecords(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("staged records = %+v, want none (this domain has zero records by construction)", recs)
	}
	// ...but it DOES get a completed_domains row, and CompletedDomainsAfter surfaces its summary.
	sums, maxSeq, scanned, err := s.CompletedDomainsAfter(ctx, "sc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 1 || maxSeq == 0 {
		t.Fatalf("CompletedDomainsAfter scanned=%d maxSeq=%d, want scanned=1 maxSeq>0", scanned, maxSeq)
	}
	if len(sums) != 1 || sums[0].RootDomain != "empty.com" {
		t.Fatalf("CompletedDomainsAfter sums = %+v, want one row for empty.com", sums)
	}
}

// TestCompletedDomainsExcludesErrorStatus mirrors TestPendingExcludesErrorStatus for the completed_domains
// stream: a domain committed with Status="error" must not produce a completed_domains row, since it never
// gets a scan-summary (StagedDomains/SummariesFor are done-only for the same reason).
func TestCompletedDomainsExcludesErrorStatus(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	if err := s.CommitBatch(ctx, []model.DomainResult{
		{ScanID: "sc", RootDomain: "a.com", Status: "error", Error: "timeout", ResolvedAt: now},
		{ScanID: "sc", RootDomain: "b.com", Status: "done", ResolvedAt: now},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	sums, _, scanned, err := s.CompletedDomainsAfter(ctx, "sc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 1 || len(sums) != 1 || sums[0].RootDomain != "b.com" {
		t.Fatalf("CompletedDomainsAfter = sums=%+v scanned=%d, want only b.com (a.com errored)", sums, scanned)
	}
}

// TestCompletedDomainsExcludesPartialStatus is TestCompletedDomainsExcludesErrorStatus's Task 9
// sibling: a domain committed Status="partial" (authoritative contact succeeded, but the done-bar
// wasn't met — see model.DomainResult's Status doc comment) must ALSO be excluded from
// completed_domains/StagedDomains/SummariesFor, exactly like "error". Only "done" may ever reach the
// last-good summary tables — this is what "don't write a summary row at all for non-done" means in
// practice, and it is what keeps a partial result from clobbering a prior done scan's summary.
func TestCompletedDomainsExcludesPartialStatus(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	if err := s.CommitBatch(ctx, []model.DomainResult{
		{ScanID: "sc", RootDomain: "a.com", Status: "partial", ResolvedAt: now,
			Records: []model.DNSRecord{{Name: "a.com", RecordType: "A", Value: "1.2.3.4", Rcode: "NOERROR"}}},
		{ScanID: "sc", RootDomain: "b.com", Status: "done", ResolvedAt: now},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	sums, _, scanned, err := s.CompletedDomainsAfter(ctx, "sc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 1 || len(sums) != 1 || sums[0].RootDomain != "b.com" {
		t.Fatalf("CompletedDomainsAfter = sums=%+v scanned=%d, want only b.com (a.com is partial)", sums, scanned)
	}

	staged, err := s.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != 1 || staged[0].RootDomain != "b.com" {
		t.Fatalf("StagedDomains = %+v, want only b.com", staged)
	}

	sumsFor, err := s.SummariesFor(ctx, "sc", []string{"a.com", "b.com"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sumsFor) != 1 || sumsFor[0].RootDomain != "b.com" {
		t.Fatalf("SummariesFor = %+v, want only b.com", sumsFor)
	}
}

// TestPartialStatusPreservesObservedRecords proves the other half of the Task 9 last-good policy: even
// though a partial domain's SUMMARY is never staged (TestCompletedDomainsExcludesPartialStatus), its
// actually-observed Records still flow through the normal record stage — a partial result must not lose
// what it did manage to see.
func TestPartialStatusPreservesObservedRecords(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "partial", ResolvedAt: now,
		Records: []model.DNSRecord{
			{Name: "a.com", RecordType: "A", Slot: "@", Value: "1.2.3.4", Rcode: "NOERROR"},
			{Name: "a.com", RecordType: "MX", Value: "10 mail.a.com", Rcode: "NOERROR"},
		},
	}}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	recs, err := s.StagedRecords(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("staged records = %+v, want 2 (a partial result must still preserve observed records)", recs)
	}
}

// TestCommitBatchPersistsFinding proves DNSRecord.Finding (Task 9's public_dns_private_address
// classification) round-trips through the SQLite stage.
func TestCommitBatchPersistsFinding(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: now,
		Records: []model.DNSRecord{
			{Name: "a.com", RecordType: "A", Slot: "@", Value: "10.20.30.40", Rcode: "NOERROR", Finding: "public_dns_private_address"},
			{Name: "a.com", RecordType: "A", Slot: "www", Value: "93.184.216.34", Rcode: "NOERROR"},
		},
	}}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	recs, err := s.StagedRecords(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range recs {
		got[r.Value] = r.Finding
	}
	if got["10.20.30.40"] != "public_dns_private_address" {
		t.Errorf("finding for private address = %q, want %q", got["10.20.30.40"], "public_dns_private_address")
	}
	if got["93.184.216.34"] != "" {
		t.Errorf("finding for public address = %q, want empty", got["93.184.216.34"])
	}
}

// TestCommitBatchPersistsOutcomeFields proves DSOutcome/DNSKEYOutcome (Task 9) round-trip through the
// SQLite stage and out through StagedDomains, alongside the existing DSPresent/DNSSECSigned booleans.
func TestCommitBatchPersistsOutcomeFields(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	if err := s.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: now,
		DSPresent: true, DSOutcome: "present",
		DNSSECSigned: true, DNSKEYOutcome: "present",
	}}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	rows, err := s.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("staged domains = %+v, want 1", rows)
	}
	r := rows[0]
	if r.DSOutcome != "present" || r.DNSKEYOutcome != "present" {
		t.Errorf("outcome fields = %+v, want both present", r)
	}
	if r.DSPresent != 1 || r.DNSSECSigned != 1 {
		t.Errorf("boolean fields = %+v, want both 1", r)
	}
}

// TestCompletedDomainsAfterResumeBySeq proves the domain-summary stream pages by seq exactly like
// RecordsAfter pages by rowid: a caller that persists the returned maxSeq and resumes from it sees only
// domains completed after that point, and a late-finishing domain (committed after the first page was
// read) is picked up on the next call rather than being missed.
func TestCompletedDomainsAfterResumeBySeq(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)
	if _, err := s.Seed(ctx, "sc", []string{"a.com", "b.com", "c.com"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	commit := func(d string) {
		if err := s.CommitBatch(ctx, []model.DomainResult{{ScanID: "sc", RootDomain: d, Status: "done", ResolvedAt: now}}); err != nil {
			t.Fatalf("commit %s: %v", d, err)
		}
	}
	commit("a.com")
	commit("b.com")

	sums1, maxSeq1, scanned1, err := s.CompletedDomainsAfter(ctx, "sc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if scanned1 != 2 || len(sums1) != 2 {
		t.Fatalf("first page: sums=%+v scanned=%d, want 2/2", sums1, scanned1)
	}

	// Nothing new past the watermark yet.
	if sums2, _, scanned2, _ := s.CompletedDomainsAfter(ctx, "sc", maxSeq1, 100); scanned2 != 0 || len(sums2) != 0 {
		t.Fatalf("CompletedDomainsAfter(watermark) = sums=%+v scanned=%d, want none", sums2, scanned2)
	}

	// A late-completing domain shows up next — the watermark walks completion order, never misses it.
	commit("c.com")
	sums3, maxSeq3, scanned3, err := s.CompletedDomainsAfter(ctx, "sc", maxSeq1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if scanned3 != 1 || len(sums3) != 1 || sums3[0].RootDomain != "c.com" || maxSeq3 <= maxSeq1 {
		t.Fatalf("resume after watermark: sums=%+v scanned=%d maxSeq=%d", sums3, scanned3, maxSeq3)
	}
}

// TestLoadWatermarksAreIndependent proves records_rowid and domains_seq (Task 8's two independent
// watermarks) never clobber each other: advancing one via its setter leaves the other's getter reading
// exactly what it read before, in both directions.
func TestLoadWatermarksAreIndependent(t *testing.T) {
	ctx := context.Background()
	s := openTemp(t)

	if w, err := s.LoadedRowid(ctx, "sc"); err != nil || w != 0 {
		t.Fatalf("initial LoadedRowid = %d, err=%v, want 0", w, err)
	}
	if w, err := s.LoadedDomainsSeq(ctx, "sc"); err != nil || w != 0 {
		t.Fatalf("initial LoadedDomainsSeq = %d, err=%v, want 0", w, err)
	}

	if err := s.SetLoadedRowid(ctx, "sc", 42); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.LoadedDomainsSeq(ctx, "sc"); w != 0 {
		t.Fatalf("LoadedDomainsSeq after SetLoadedRowid = %d, want 0 (unaffected)", w)
	}

	if err := s.SetLoadedDomainsSeq(ctx, "sc", 7); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.LoadedRowid(ctx, "sc"); w != 42 {
		t.Fatalf("LoadedRowid after SetLoadedDomainsSeq = %d, want 42 (unaffected)", w)
	}
	if w, _ := s.LoadedDomainsSeq(ctx, "sc"); w != 7 {
		t.Fatalf("LoadedDomainsSeq = %d, want 7", w)
	}

	// Advancing loaded_rowid again must still leave domains_seq untouched, and vice versa.
	if err := s.SetLoadedRowid(ctx, "sc", 100); err != nil {
		t.Fatal(err)
	}
	if w, _ := s.LoadedDomainsSeq(ctx, "sc"); w != 7 {
		t.Fatalf("LoadedDomainsSeq after a second SetLoadedRowid = %d, want 7 (still unaffected)", w)
	}
}

// TestCompletedDomainsAfterUsesSeqIndex proves the domains-summary watermark query walks the
// (scan_id, seq) index directly rather than falling back to a temp B-tree sort, mirroring
// TestRecordsAfterUsesRowidScan's concern for scan_records — the same throughput cliff would hit a
// multi-million-domain scan otherwise.
func TestCompletedDomainsAfterUsesSeqIndex(t *testing.T) {
	s := openTemp(t)
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+completedDomainsAfterQuery, "sc", 0, 10)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	joined := strings.Join(plan, "; ")
	if strings.Contains(joined, "TEMP B-TREE") {
		t.Errorf("plan sorts with a temp B-tree instead of using the seq index: %s", joined)
	}
}

// TestDiscoveredHostnamesTimestampsFromResolvedAt proves each returned row's timestamps come from the
// domain's own persisted resolved_at (stamped by CommitBatch), not from the caller's wall-clock time —
// so two calls to DiscoveredHostnames (simulating a retried registry write-back) return byte-identical
// timestamps, and those timestamps match the scan's actual resolution time rather than "now".
func TestDiscoveredHostnamesTimestampsFromResolvedAt(t *testing.T) {
	ctx := context.Background()
	st := openTemp(t)
	if _, err := st.Seed(ctx, "s1", []string{"example.com"}); err != nil {
		t.Fatal(err)
	}
	resolvedAt := time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)
	res := model.DomainResult{
		ScanID: "s1", RootDomain: "example.com", Status: "done", ResolvedAt: resolvedAt,
		Records: []model.DNSRecord{
			{Name: "jenkins.example.com", RecordType: "A", Value: "10.0.0.5", Rcode: "NOERROR", Source: "axfr", Discovery: "axfr"},
		},
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}

	first, err := st.DiscoveredHostnames(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("DiscoveredHostnames = %+v, want 1 row", first)
	}
	if !first[0].FirstSeen.Equal(resolvedAt) || !first[0].LastSeen.Equal(resolvedAt) || !first[0].LastResolved.Equal(resolvedAt) {
		t.Errorf("timestamps = %+v, want first_seen=last_seen=last_resolved=%v (the scan's resolved_at)", first[0], resolvedAt)
	}

	// A second call (simulating a retried write-back, potentially much later in wall-clock time) must
	// return byte-identical timestamps — they come from the stored resolved_at, not time.Now().
	time.Sleep(2 * time.Millisecond)
	retry, err := st.DiscoveredHostnames(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || !retry[0].FirstSeen.Equal(first[0].FirstSeen) || !retry[0].LastSeen.Equal(first[0].LastSeen) || !retry[0].LastResolved.Equal(first[0].LastResolved) {
		t.Errorf("retry timestamps = %+v, want identical to first call's %+v", retry, first[0])
	}
}
