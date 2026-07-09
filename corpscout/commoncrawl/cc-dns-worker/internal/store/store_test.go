package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
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

func TestSummaryPersistsAXFRFlags(t *testing.T) {
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
		AXFROpen: true, AXFRRecords: 42, AXFRTruncated: true,
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedDomains(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 summary, got %d", len(rows))
	}
	if rows[0].AXFROpen != 1 || rows[0].AXFRRecords != 42 || rows[0].AXFRTruncated != 1 {
		t.Fatalf("axfr flags not round-tripped: %+v", rows[0])
	}
}

func TestSummaryPersistsAXFRServer(t *testing.T) {
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
		AXFROpen: true, AXFRServer: "203.0.113.9",
	}
	if err := st.CommitBatch(ctx, []model.DomainResult{res}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StagedDomains(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AXFRServer != "203.0.113.9" {
		t.Fatalf("axfr_server not round-tripped: %+v", rows)
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
