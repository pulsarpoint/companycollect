package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"
)

// fakeAXFRProber never touches the network: it answers every probe as a definitive close, instantly.
// Tests use it to exercise the feeder/worker/committer plumbing without a real DNS server.
type fakeAXFRProber struct{}

func (fakeAXFRProber) ProbeServer(ctx context.Context, zone, nsHost, nsIP string) resolve.AXFROutcome {
	return resolve.AXFROutcome{
		Verdict: resolve.VerdictClosed, Reason: resolve.ReasonRefused,
		NSHost: nsHost, NSIP: nsIP, ObservedAt: time.Now().UTC(),
	}
}

// fakePendingFrom returns an axfrPendingFunc that serves pages out of a fixed, lexically-sorted domain
// list — a stand-in for store.PendingAXFRDomains that needs no real SQLite queue. Each domain gets one
// dialable, non-hyperscaler endpoint so every worker actually calls the fake prober once.
func fakePendingFrom(domains []string) axfrPendingFunc {
	return func(ctx context.Context, scanID, after string, limit int) ([]store.AXFRTarget, error) {
		var out []store.AXFRTarget
		for _, d := range domains {
			if d <= after {
				continue
			}
			out = append(out, store.AXFRTarget{
				RootDomain: d,
				Endpoints:  []model.NameserverEndpoint{{Name: "ns1." + d, IP: "203.0.113.9", Scope: "public", Dialable: true}},
			})
			if len(out) >= limit {
				break
			}
		}
		return out, nil
	}
}

// failAfterN returns an axfrCommitFunc that succeeds on its first n-1 calls and fails on the nth — and
// every call after that, since runAXFRQueue's committer stops invoking it once it has seen one error.
// The committer is the only goroutine that ever calls this, so the plain closure counter needs no lock.
func failAfterN(n int) axfrCommitFunc {
	count := 0
	return func(ctx context.Context, scanID, domain string, probes []resolve.AXFROutcome, zone []model.DNSRecord, runID string, observedAt time.Time, errMsg string) error {
		count++
		if count == n {
			return errors.New("injected commit failure")
		}
		return nil
	}
}

// TestRunAXFRQueueCommitErrorCancelsAndJoinsAllGoroutines proves the shutdown contract: once the
// committer sees a commit error, runAXFRQueue must cancel its context and return promptly with every
// feeder/worker/committer/stats goroutine joined — not hang with a worker blocked trying to send a
// result nobody drains anymore. The test's only real assertion is that the call returns (via a bounded
// select) with the injected error; a hang here means a goroutine leaked. Run this test with -race: the
// shutdown path touches feedErr/commitErr from multiple goroutines, and a race there would mean the
// happens-before edges the done-channels are supposed to provide are broken.
func TestRunAXFRQueueCommitErrorCancelsAndJoinsAllGoroutines(t *testing.T) {
	domains := make([]string, 300)
	for i := range domains {
		domains[i] = fmt.Sprintf("d%04d.example.com", i)
	}
	deps := axfrQueueDeps{
		scanID: "sc", runID: "run1", workers: 8, statsInterval: 0,
		pending: fakePendingFrom(domains),
		prober:  fakeAXFRProber{},
		commit:  failAfterN(5),
	}

	errCh := make(chan error, 1)
	go func() { errCh <- runAXFRQueue(context.Background(), deps) }()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "injected commit failure") {
			t.Fatalf("runAXFRQueue error = %v, want an error wrapping %q", err, "injected commit failure")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runAXFRQueue did not return within 10s of a commit error — a goroutine leaked/deadlocked")
	}
}

// TestRunAXFRQueueNoCommitError proves the happy path also joins cleanly: every domain the fake feeder
// serves gets probed and committed with no error, and the call returns nil promptly.
func TestRunAXFRQueueNoCommitError(t *testing.T) {
	domains := []string{"a.example.com", "b.example.com", "c.example.com"}
	committed := 0
	deps := axfrQueueDeps{
		scanID: "sc", runID: "run1", workers: 4, statsInterval: 0,
		pending: fakePendingFrom(domains),
		prober:  fakeAXFRProber{},
		commit: func(ctx context.Context, scanID, domain string, probes []resolve.AXFROutcome, zone []model.DNSRecord, runID string, observedAt time.Time, errMsg string) error {
			committed++
			return nil
		},
	}

	errCh := make(chan error, 1)
	go func() { errCh <- runAXFRQueue(context.Background(), deps) }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runAXFRQueue = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runAXFRQueue did not return within 10s")
	}
	if committed != len(domains) {
		t.Errorf("committed %d domains, want %d", committed, len(domains))
	}
}

// TestRunAXFRPipelineSkipsWhenQueueComplete proves the real, unfaked entry point: once every domain
// SeedAXFRDomains stages is already 'done', runAXFRPipeline must return immediately without building a
// scheduler/prober or dispatching any work — dispatching zero work means making zero network requests,
// since the endpoint below (203.0.113.9, a reserved TEST-NET-3 address) would otherwise hang or fail
// slowly rather than fast if actually dialed.
func TestRunAXFRPipelineSkipsWhenQueueComplete(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Seed(ctx, "sc", []string{"a.com"}); err != nil {
		t.Fatal(err)
	}
	eps := []model.NameserverEndpoint{{Name: "ns1.a.com", IP: "203.0.113.9", Scope: "public", Dialable: true}}
	if err := st.CommitBatch(ctx, []model.DomainResult{{
		ScanID: "sc", RootDomain: "a.com", Status: "done", ResolvedAt: time.Unix(0, 0).UTC(),
		NSIPs: []string{"203.0.113.9"}, Endpoints: eps,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SeedAXFRDomains(ctx, "sc"); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitAXFRDomain(ctx, "sc", "a.com", nil, nil, "run1", time.Unix(0, 0).UTC(), ""); err != nil {
		t.Fatal(err)
	}

	cfg := scanConfig{
		scanID: "sc", runID: "run1",
		axfrWorkers: 4, axfrQPS: 5, axfrInflight: 5,
		axfrMaxRecords: 100, axfrMaxBytes: 1000, axfrTimeout: time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- runAXFRPipeline(context.Background(), st, cfg) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runAXFRPipeline on an already-complete queue: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runAXFRPipeline hung on an already-complete queue — it must skip dispatch entirely")
	}
	if doneQ, err := st.AXFRQueueComplete(ctx, "sc"); err != nil || !doneQ {
		t.Errorf("AXFRQueueComplete = %v, err=%v; want true (unchanged)", doneQ, err)
	}
}
