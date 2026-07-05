# cc-dns-worker Streaming Input Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `cc-dns-worker scan` process the full ~33.6M-domain corpus with bounded memory by streaming the ClickHouse seed in batches and dispatching the pending queue in ordered, committed batches — instead of materializing the whole domain list and the whole pending set in RAM.

**Architecture:** Two changes, no new packages. (1) `input.StreamClickHouse` pulls `root_domain`s from ClickHouse and invokes a callback per batch, so seeding never holds the full list. (2) `store.PendingBatch` cursor-paginates the not-done queue on the primary key, and `runScan` loops over it in a **batch-barrier**: each batch is fully resolved *and committed* before the next is fetched, so peak memory is one `--dispatch-batch` and in-flight domains are never re-dispatched. The schedulers, resolver, worker pool, `CommitBatch`, `load`, and the resume contract are unchanged.

**Tech Stack:** Go 1.25 (existing module), `modernc.org/sqlite`, `github.com/ClickHouse/clickhouse-go/v2`.

**Spec:** `docs/superpowers/specs/2026-07-06-cc-dns-streaming-input-design.md`

## Global Constraints
- Module `cc-dns-worker`; branch `feat/cc-dns-streaming`; work from `commoncrawl/cc-dns-worker/`.
- go.mod floor is `go 1.25.0` — do NOT change it. **Do NOT run `go mod tidy`** (it strips pre-fetched deps and is reverted by the controller). Do not edit go.mod/go.sum. Do not commit any binary or `.db` file (module `.gitignore` covers `/bin/`, `/cc-dns-worker`, `scan.db*`).
- Follow Conventional Commits; run `go fmt ./...` and `go vet ./...` before each commit.
- Commit only the paths named in each task's commit step.
- ClickHouse for the e2e: `CLICKHOUSE_ADDR=companycollect:9002 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=password123 CLICKHOUSE_DB=corpscout` (native port 9002; migrations already applied). `corpscout.commoncrawl_domains` has ~33.6M distinct `root_domain`s.
- Resume contract is unchanged and must stay intact: a re-run resolves only the not-`done`/not-`error` domains; committing a domain is one transaction; `--limit` + `ORDER BY root_domain` stay deterministic.

---

### Task 1: `store.PendingBatch` — cursor-paginated pending fetch

**Files:**
- Modify: `commoncrawl/cc-dns-worker/internal/store/store.go` (add one method)
- Test: `commoncrawl/cc-dns-worker/internal/store/store_test.go` (add one test)

**Interfaces:**
- Consumes: existing `Store`, `Seed`, `CommitBatch`, `model.DomainResult`.
- Produces: `(*Store).PendingBatch(ctx context.Context, scanID, afterRootDomain string, limit int) ([]string, error)` — used by `runScan` (Task 3). `afterRootDomain=""` starts at the beginning.

- [ ] **Step 1: Write the failing test**

Add to `commoncrawl/cc-dns-worker/internal/store/store_test.go`:
```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/store/ -run TestPendingBatchCursor`
Expected: FAIL — `PendingBatch` undefined.

- [ ] **Step 3: Add the method**

In `commoncrawl/cc-dns-worker/internal/store/store.go`, add after `Pending`:
```go
// PendingBatch returns up to limit not-yet-terminal domains for scanID whose root_domain is greater
// than afterRootDomain, ordered by root_domain. afterRootDomain="" starts at the beginning.
// Cursor-paginating on the (scan_id, root_domain) primary key keeps streaming dispatch ~O(n) instead
// of re-walking the growing done/error prefix on every call.
func (s *Store) PendingBatch(ctx context.Context, scanID, afterRootDomain string, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT root_domain FROM scan_domains
		 WHERE scan_id = ? AND root_domain > ? AND status NOT IN ('done','error')
		 ORDER BY root_domain LIMIT ?`, scanID, afterRootDomain, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/store/`
Expected: PASS (all store tests, including the new one).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./internal/store/
git add commoncrawl/cc-dns-worker/internal/store/store.go commoncrawl/cc-dns-worker/internal/store/store_test.go
git commit -m "feat(dns): cursor-paginated store.PendingBatch for streaming dispatch"
```

---

### Task 2: `input.StreamClickHouse` — batch streamer

**Files:**
- Modify: `commoncrawl/cc-dns-worker/internal/input/input.go` (add `drainRows` + `StreamClickHouse`)
- Test: `commoncrawl/cc-dns-worker/internal/input/input_test.go` (add `drainRows` tests)

**Interfaces:**
- Consumes: existing `DefaultQuery`, `applyLimit`, `clickhouse-go/v2/lib/driver`.
- Produces: `input.StreamClickHouse(ctx, conn driver.Conn, query string, limit, batchSize int, fn func(batch []string) error) error` — used by `runScan` (Task 3). The batching core is the unexported `drainRows`, unit-tested without ClickHouse; the real-CH path is exercised by Task 3's e2e.

- [ ] **Step 1: Write the failing test**

Add to `commoncrawl/cc-dns-worker/internal/input/input_test.go`:
```go
func TestDrainRows(t *testing.T) {
	// Fake row source: yields the slice one at a time; empty strings must be dropped.
	src := []string{"a", "", "b", "c", "d"}
	run := func(batchSize int) [][]string {
		i := -1
		var got [][]string
		_ = drainRows(
			func() bool { i++; return i < len(src) },
			func(p *string) error { *p = src[i]; return nil },
			func() error { return nil },
			batchSize,
			func(b []string) error { got = append(got, append([]string(nil), b...)); return nil },
		)
		return got
	}
	// batchSize 2 over [a,b,c,d] (empty dropped) -> [a,b],[c,d]
	if got := run(2); len(got) != 2 || got[0][0] != "a" || got[0][1] != "b" || got[1][0] != "c" || got[1][1] != "d" {
		t.Fatalf("batchSize 2 -> %v", got)
	}
	// batchSize 3 -> [a,b,c],[d]  (final partial batch flushed)
	if got := run(3); len(got) != 2 || len(got[0]) != 3 || len(got[1]) != 1 || got[1][0] != "d" {
		t.Fatalf("batchSize 3 -> %v", got)
	}
}

func TestDrainRowsEmptyAndError(t *testing.T) {
	// Empty source -> fn never called.
	called := false
	_ = drainRows(func() bool { return false }, func(*string) error { return nil }, func() error { return nil }, 2,
		func([]string) error { called = true; return nil })
	if called {
		t.Errorf("fn should not be called for empty source")
	}
	// fn error propagates.
	i := -1
	src := []string{"a", "b"}
	err := drainRows(func() bool { i++; return i < len(src) }, func(p *string) error { *p = src[i]; return nil }, func() error { return nil }, 1,
		func([]string) error { return errTest })
	if err != errTest {
		t.Errorf("want errTest, got %v", err)
	}
}

var errTest = errorString("boom")

type errorString string

func (e errorString) Error() string { return string(e) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/input/ -run TestDrainRows`
Expected: FAIL — `drainRows` undefined.

- [ ] **Step 3: Add `drainRows` and `StreamClickHouse`**

In `commoncrawl/cc-dns-worker/internal/input/input.go`, add after `FromClickHouse`:
```go
// StreamClickHouse runs query (default DefaultQuery) and invokes fn once per batch of up to
// batchSize non-empty root_domains, plus once for the final partial batch. It never materializes the
// whole result set — peak memory is one batch. fn must consume the batch before returning; the
// backing slice is reused across calls.
func StreamClickHouse(ctx context.Context, conn driver.Conn, query string, limit, batchSize int, fn func(batch []string) error) error {
	if query == "" {
		query = DefaultQuery
	}
	rows, err := conn.Query(ctx, applyLimit(query, limit))
	if err != nil {
		return fmt.Errorf("query domains: %w", err)
	}
	defer rows.Close()
	return drainRows(rows.Next, func(p *string) error { return rows.Scan(p) }, rows.Err, batchSize, fn)
}

// drainRows is the ClickHouse-free batching core of StreamClickHouse: it pulls values via
// next()/scan, drops empty strings, groups into batches of batchSize, and calls fn per full batch
// plus a final partial batch. The batch slice is reused, so fn must not retain it.
func drainRows(next func() bool, scan func(*string) error, rowsErr func() error, batchSize int, fn func([]string) error) error {
	if batchSize <= 0 {
		batchSize = 5000
	}
	batch := make([]string, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for next() {
		var d string
		if err := scan(&d); err != nil {
			return err
		}
		if d == "" {
			continue
		}
		batch = append(batch, d)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rowsErr(); err != nil {
		return err
	}
	return flush()
}
```
(`context`, `fmt`, and the `driver` import are already present in `input.go` from `FromClickHouse`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/input/`
Expected: PASS (all input tests).

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./internal/input/
git add commoncrawl/cc-dns-worker/internal/input/input.go commoncrawl/cc-dns-worker/internal/input/input_test.go
git commit -m "feat(dns): input.StreamClickHouse batch streamer (bounded-memory seed)"
```

---

### Task 3: Rewire `runScan` to streaming seed + batch-barrier dispatch

**Files:**
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` (replace the whole file)

**Interfaces:**
- Consumes: `input.StreamClickHouse` (Task 2), `store.PendingBatch` (Task 1), existing `resolve.Discoverer`/`Resolver`, `store.CommitBatch`, `records.DefaultConfig`.
- Produces: the new `runScan` behavior + `--dispatch-batch` flag; helpers `resolveBatch`, `resolveDomain`. No unit test (needs live CH+DNS); gated by build/vet + the e2e in Step 4.

- [ ] **Step 1: Replace `scan.go`**

Replace the entire contents of `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` with:
```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/input"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"
)

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan batch id (default today, UTC)")
	runID := fs.String("run-id", "", "source run id (defaults to scan-id)")
	dbPath := fs.String("db", "scan.db", "SQLite stage path")
	query := fs.String("query", input.DefaultQuery, "ClickHouse query returning root_domain")
	limit := fs.Int("limit", 0, "cap number of domains (0 = all)")
	resolvers := fs.String("resolvers", strings.Join(resolve.DefaultResolvers, ","), "comma-separated recursive resolvers for NS discovery (use 127.0.0.1:53 for a local unbound)")
	discoveryQPS := fs.Float64("discovery-qps", 50, "max queries/sec per recursive resolver (bump high for local unbound)")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per authoritative NS IP")
	inflight := fs.Int("per-server-inflight", 3, "max concurrent queries per NS IP")
	workers := fs.Int("workers", 4000, "max domains resolved concurrently")
	batchN := fs.Int("commit-batch", 200, "domains per SQLite commit")
	seedChunk := fs.Int("seed-chunk", 5000, "domains per SQLite seed transaction")
	dispatchBatch := fs.Int("dispatch-batch", 20000, "domains fetched from the queue and resolved per barrier iteration (bounds memory)")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	_ = fs.Parse(args)
	if *runID == "" {
		*runID = *scanID
	}
	if *seedChunk <= 0 {
		*seedChunk = 5000
	}
	if *workers <= 0 {
		*workers = 1
	}
	if *dispatchBatch <= 0 {
		*dispatchBatch = 20000
	}
	if *batchN <= 0 {
		*batchN = 200
	}
	resolverList := cleanResolvers(strings.Split(*resolvers, ","))
	if len(resolverList) == 0 {
		return fmt.Errorf("--resolvers is empty")
	}
	ctx := context.Background()

	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	// 1) Stream the queue seed from ClickHouse in batches — never materialize the whole domain list.
	conn, err := chConn()
	if err != nil {
		return err
	}
	added, total := 0, 0
	err = input.StreamClickHouse(ctx, conn, *query, *limit, *seedChunk, func(batch []string) error {
		n, serr := st.Seed(ctx, *scanID, batch)
		if serr != nil {
			return serr
		}
		added += n
		total += len(batch)
		return nil
	})
	conn.Close()
	if err != nil {
		return err
	}
	log.Printf("scan_id=%s: seeded %d domains from CH (%d new)", *scanID, total, added)

	// 2) Two schedulers + resolver (unchanged).
	discSched := scheduler.New(scheduler.Config{PerServerQPS: *discoveryQPS, Burst: max(1, int(*discoveryQPS)), MaxInFlight: *inflight})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: max(1, int(*qps)), MaxInFlight: *inflight})
	disc := resolve.NewDiscoverer(resolve.NewExchanger(discSched, *timeout), resolverList)
	rec := resolve.NewResolver(resolve.NewExchanger(authSched, *timeout))
	cfg := records.DefaultConfig()

	// 3) Dispatch the pending queue in ordered batches. Each batch is fully resolved AND committed
	// before the next is fetched (commit barrier), so peak memory is one dispatch-batch and in-flight
	// domains are never re-fetched. The cursor advances by root_domain to keep this ~O(n).
	cursor := ""
	resolved := 0
	for {
		batch, err := st.PendingBatch(ctx, *scanID, cursor, *dispatchBatch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		committed, err := resolveBatch(ctx, st, disc, rec, cfg, batch, *scanID, *runID, *workers, *batchN)
		if err != nil {
			return err
		}
		if committed == 0 {
			return fmt.Errorf("no progress: batch of %d domains committed 0 (wedged writer?)", len(batch))
		}
		cursor = batch[len(batch)-1]
		resolved += committed
		log.Printf("scan_id=%s: resolved %d domains (cursor=%q)", *scanID, resolved, cursor)
	}
	log.Printf("scan_id=%s: done (%d domains resolved this run)", *scanID, resolved)
	return nil
}

// resolveBatch resolves one dispatch-batch of domains concurrently (bounded by workers), collects
// their DomainResults, and commits them to SQLite in commit-batch chunks. It returns the number of
// domains committed. Peak memory is the batch's results, so the caller's cursor loop stays bounded.
func resolveBatch(ctx context.Context, st *store.Store, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, batch []string, scanID, runID string, workers, commitBatch int) (int, error) {
	results := make(chan model.DomainResult, commitBatch*2)
	collected := make([]model.DomainResult, 0, len(batch))
	var collectWG sync.WaitGroup
	collectWG.Add(1)
	go func() {
		defer collectWG.Done()
		for r := range results {
			collected = append(collected, r)
		}
	}()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, d := range batch {
		sem <- struct{}{}
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			defer func() { <-sem }()
			results <- resolveDomain(ctx, disc, rec, cfg, domain, scanID, runID)
		}(d)
	}
	wg.Wait()
	close(results)
	collectWG.Wait()

	committed := 0
	for i := 0; i < len(collected); i += commitBatch {
		end := min(i+commitBatch, len(collected))
		if err := st.CommitBatch(ctx, collected[i:end]); err != nil {
			return committed, fmt.Errorf("commit batch: %w", err)
		}
		committed += end - i
	}
	return committed, nil
}

// resolveDomain discovers a domain's authoritative NS then resolves its Tier-2 records into a
// DomainResult; on discovery failure or no NS IPs it returns a status="error" result.
func resolveDomain(ctx context.Context, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, domain, scanID, runID string) model.DomainResult {
	now := time.Now().UTC()
	del, derr := disc.DiscoverNS(ctx, domain)
	if derr != nil || len(del.NSIPs) == 0 {
		msg := "no authoritative NS IPs"
		if derr != nil {
			msg = derr.Error()
		}
		return model.DomainResult{
			ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
			Nameservers: del.NS, DSPresent: len(del.DS) > 0,
			Status: "error", Error: msg, SourceRunID: runID, ResolvedAt: now,
		}
	}
	return rec.Resolve(ctx, domain, scanID, runID, del, cfg, now)
}

// cleanResolvers drops empty/whitespace-only tokens (e.g. from a trailing comma or blank
// --resolvers flag) so a malformed flag can't silently produce a zero-length resolver list that then
// fails every domain's discovery one at a time.
func cleanResolvers(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if t := strings.TrimSpace(r); t != "" {
			out = append(out, t)
		}
	}
	return out
}
```

- [ ] **Step 2: Verify build + full unit suite**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go vet ./... && go test ./...`
Expected: build + vet clean; all unit tests PASS. (`input.FromClickHouse` and `store.Pending` are now unused by `scan.go` but remain exported and covered by their own tests — that's fine.)

- [ ] **Step 3: Build the binary**

Run: `cd commoncrawl/cc-dns-worker && go build -o bin/cc-dns-worker ./cmd/cc-dns-worker`
Expected: exit 0; binary at `bin/` (gitignored).

- [ ] **Step 4: Real e2e — forces multiple dispatch batches, verifies bounded/complete/resumable**

Run (from `commoncrawl/cc-dns-worker`, CH env set):
```bash
export CLICKHOUSE_ADDR=companycollect:9002 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=password123 CLICKHOUSE_DB=corpscout
rm -f /tmp/dns-stream.db*
# 300 domains, dispatch-batch 100 -> at least 3 barrier iterations
./bin/cc-dns-worker scan --limit 300 --dispatch-batch 100 --db /tmp/dns-stream.db --scan-id streamchk
```
Expected: a "seeded N domains" line, then several "resolved …" lines (≥3, one per batch of 100), then "done (N domains resolved this run)".

Verify every seeded domain reached a terminal status exactly once (no dup rows, no missed domains) via SQLite:
```bash
# sqlite3 may not be installed; if not, this assertion is covered by the CH load counts below.
python3 - <<'PY'
import sqlite3
c=sqlite3.connect("/tmp/dns-stream.db")
seeded=c.execute("SELECT count(*) FROM scan_domains WHERE scan_id='streamchk'").fetchone()[0]
terminal=c.execute("SELECT count(*) FROM scan_domains WHERE scan_id='streamchk' AND status IN('done','error')").fetchone()[0]
print("seeded",seeded,"terminal",terminal)
assert seeded==terminal and seeded>0, "every seeded domain must reach a terminal status"
PY
```
Expected: `seeded N terminal N` with `N>0`.

- [ ] **Step 5: Resume proof + load + cleanup**

Run:
```bash
# rerun same command -> 0 resolved (all terminal)
./bin/cc-dns-worker scan --limit 300 --dispatch-batch 100 --db /tmp/dns-stream.db --scan-id streamchk 2>&1 | grep -E 'seeded|done'
# load into CH and verify counts
./bin/cc-dns-worker load --db /tmp/dns-stream.db --scan-id streamchk
CH="http://companycollect:8123/?user=default&password=password123"
curl -s "$CH" --data-binary "SELECT count(), uniqExact(root_domain) FROM corpscout.commoncrawl_domain_dns_scan FINAL WHERE scan_id='streamchk'"
# cleanup
curl -s "$CH" --data-binary "DELETE FROM corpscout.commoncrawl_domain_dns_records WHERE scan_id='streamchk'"
curl -s "$CH" --data-binary "DELETE FROM corpscout.commoncrawl_domain_dns_scan WHERE scan_id='streamchk'"
rm -f /tmp/dns-stream.db*
```
Expected: rerun logs `done (0 domains resolved this run)`; CH scan count == uniqExact == the seeded N (each domain once, no dups). Cleanup leaves no `streamchk` rows.

- [ ] **Step 6: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go
git commit -m "feat(dns): streaming seed + batch-barrier dispatch (bounded-memory full-corpus scan)"
```

---

### Task 4: README — document streaming + `--dispatch-batch`

**Files:**
- Modify: `commoncrawl/cc-dns-worker/README.md`

**Interfaces:** none new — documentation only.

- [ ] **Step 1: Update the README**

In `commoncrawl/cc-dns-worker/README.md`:
- In the `scan` flags list, add `--dispatch-batch` (default 20000) — "domains fetched from the queue and resolved per barrier iteration; bounds peak memory independently of corpus size."
- In the section describing how `scan` works (the SQLite stage / pipeline), replace any statement that says the domain list is read in full / `Pending()` returns all with a description of streaming: the seed is streamed from ClickHouse in `--seed-chunk` batches, and the pending queue is dispatched in ordered `--dispatch-batch` batches, each fully committed before the next is fetched — so a full-corpus (~33.6M) scan runs with **bounded memory** (~one dispatch-batch in flight), not by materializing the whole list.
- Update the "deferred" list: memory is now bounded; what remains deferred for full corpus is **disk** (the `scan_records` SQLite file still grows to ~tens of GB before `load`) — the follow-ups are incremental CH flush or partitioned runs. Keep the circuit-breaker deferred note.
Keep the README's existing tone/structure; do not invent flags — every documented flag must exist in `cmd/cc-dns-worker/scan.go`.

- [ ] **Step 2: Sanity-check flag accuracy**

Run: `cd commoncrawl/cc-dns-worker && ./bin/cc-dns-worker scan -h 2>&1 | grep dispatch-batch`
Expected: the `-dispatch-batch` flag appears with its default, matching the README.

- [ ] **Step 3: Commit**

```bash
git add commoncrawl/cc-dns-worker/README.md
git commit -m "docs(dns): document streaming seed/dispatch and --dispatch-batch"
```

---

## Self-review notes
- Spec §2.2 `PendingBatch` → Task 1 (+ cursor test). ✓
- Spec §2.1 `StreamClickHouse` → Task 2 (+ `drainRows` unit tests; real-CH path via Task 3 e2e). ✓
- Spec §3 streaming seed + batch-barrier dispatch + commit-barrier + no-progress guard + `--dispatch-batch` → Task 3. ✓
- Spec §3 "commit before next fetch" correctness → Task 3 uses per-batch synchronous collect+commit before advancing the cursor (a batch is durably committed before `PendingBatch` is called again); e2e Step 4 asserts every seeded domain reaches terminal status exactly once. ✓
- Spec §6 testing (PendingBatch cursor, drainRows batching, e2e multi-batch + resume) → Tasks 1/2/3. ✓
- Spec §4 "what does not change" (schedulers/resolver/CommitBatch/load/resume) → Task 3 leaves them intact; `resolveDomain`/`resolveBatch` only relocate the existing worker body. ✓
- Type consistency: `PendingBatch(ctx, scanID, afterRootDomain, limit)` (Task 1) called with those args in Task 3; `StreamClickHouse(ctx, conn, query, limit, batchSize, fn)` (Task 2) called with those in Task 3; `resolveBatch`/`resolveDomain` signatures self-consistent within Task 3. ✓
```
