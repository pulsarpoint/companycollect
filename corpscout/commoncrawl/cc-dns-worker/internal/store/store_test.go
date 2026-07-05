package store

import (
	"context"
	"path/filepath"
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
	// source_run_id must be the run-id threaded through DomainResult, not the scan_id (they can
	// legitimately differ: --run-id lets multiple scan-ids share one logical run).
	if recs[0].SourceRunID != "run-xyz" {
		t.Errorf("staged record source_run_id = %q, want %q (not scan_id %q)", recs[0].SourceRunID, "run-xyz", "sc")
	}
	rows, _ := s2.StagedDomains(ctx, "sc")
	if len(rows) != 1 || rows[0].RootDomain != "a.com" || len(rows[0].Nameservers) != 1 {
		t.Fatalf("staged domains = %+v", rows)
	}
	if rows[0].SourceRunID != "run-xyz" {
		t.Errorf("staged domain source_run_id = %q, want %q (not scan_id %q)", rows[0].SourceRunID, "run-xyz", "sc")
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

	rows, err := s.StagedDomains(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].RootDomain != "a.com" || rows[0].Status != "error" {
		t.Fatalf("staged domains = %+v, want a.com with status=error", rows)
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
