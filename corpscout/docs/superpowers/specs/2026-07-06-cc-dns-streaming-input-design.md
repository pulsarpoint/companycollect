# cc-dns-worker Streaming Input — Design

**Date:** 2026-07-06
**Status:** Draft for review
**Extends:** `2026-07-05-commoncrawl-dns-scanner-design.md` (§3.6 concurrency, §8 scale notes)

## 1. Problem

`runScan` today has two full-corpus memory ceilings that block scanning all ~33.6M
`commoncrawl_domains` in one run:

1. **Seed materialization** — `input.FromClickHouse` appends every `root_domain` into one
   `[]string` (hundreds of MB) before the chunked `Seed` loop runs.
2. **Pending materialization** — `store.Pending(scanID)` returns *every* not-done domain as one
   slice, and the worker pool ranges over it. Even after streaming the seed, this re-materializes
   the whole corpus.

The fix is to stream both: seed the queue in batches as rows arrive from ClickHouse, and dispatch
the pending queue in ordered batches so the worker pool never holds more than one batch at a time.

### Scope (decided)
- **In:** bound **memory** — a full run starts and runs without OOM. Keeps the current
  SQLite-stage-then-`load` model and the resume contract unchanged.
- **Out (deferred):** bounding **disk**. A full run's `scan_records` still accumulates in SQLite
  (~20–40 GB) and `load` runs once at the end. If disk becomes the wall, incremental CH flush or
  partitioned runs are the follow-ups (noted in the spec's §8). Not built here.
- **Out:** the per-server circuit breaker (separate deferred item, measure-first).

## 2. Components

### 2.1 `input.StreamClickHouse` — batch streamer
```go
func StreamClickHouse(ctx context.Context, conn driver.Conn, query string, limit, batchSize int,
    fn func(batch []string) error) error
```
Runs `applyLimit(query, limit)`, iterates `rows.Next()`, accumulates up to `batchSize` non-empty
`root_domain`s, calls `fn(batch)`, resets, and flushes a final partial batch at EOF. Peak memory is
one batch. `FromClickHouse` is kept as a thin wrapper (`StreamClickHouse` + append) so existing
callers/tests are unaffected, or is retired if no longer used — implementer's choice, but the
streaming primitive is the one `scan` uses.

### 2.2 `store.PendingBatch` — cursor-paginated pending fetch
```go
func (s *Store) PendingBatch(ctx context.Context, scanID, afterRootDomain string, limit int) ([]string, error)
// SELECT root_domain FROM scan_domains
// WHERE scan_id=? AND root_domain > ? AND status NOT IN ('done','error')
// ORDER BY root_domain LIMIT ?
```
Cursor by `root_domain` on the existing PRIMARY KEY `(scan_id, root_domain)` — the index makes each
fetch a forward range scan, so total dispatch cost stays ~O(n), not O(n²) (a plain repeated
`LIMIT`-from-start would re-walk the growing done-prefix every batch). `afterRootDomain=""` starts at
the beginning. `Pending()` (whole-set) stays for small runs / tests and back-compat.

## 3. Orchestration change in `runScan`

**Seed (streaming):**
```go
err := input.StreamClickHouse(ctx, conn, *query, *limit, *seedChunk, func(batch []string) error {
    n, err := st.Seed(ctx, *scanID, batch)
    added += n
    total += len(batch)
    return err
})
```
No full domain slice is ever held.

**Dispatch (batch-barrier over PendingBatch):**
```go
// one persistent single writer goroutine drains `results` (as today)
cursor := ""
for {
    batch, err := st.PendingBatch(ctx, *scanID, cursor, *dispatchBatch)
    if err != nil { return err }
    if len(batch) == 0 { break }
    dispatchAndAwait(batch)              // bounded worker pool; wg.Wait() -> all results SENT
    flushAndAwaitWriter()                // BARRIER: writer commits this batch to SQLite
    cursor = batch[len(batch)-1]         // advance past the range we just processed
}
close(results); writerWG.Wait()
```

### The one correctness subtlety — commit before next fetch
`PendingBatch` excludes only `done`/`error` rows. After `wg.Wait()`, a batch's results have been
**sent** to the writer channel but may not yet be **committed**. If we advance the cursor and fetch
the next batch immediately, the cursor moves us forward so we won't re-see them — BUT if a batch's
commit *fails* (logged, domains stay `pending`) and the cursor has advanced, those stragglers are
skipped **this run** and picked up on the next run (cursor restarts at `""`). That is acceptable and
resume-consistent. What must NOT happen is re-dispatching in-flight domains within the same run; the
**flush-and-await-writer barrier** after each batch guarantees the batch is durably committed before
the cursor advances, so a crash mid-run also leaves a consistent DB. Do not drop the barrier for
pipelining — that reintroduces the double-dispatch/lease problem the design avoids.

### No-progress guard
If `PendingBatch` returns a non-empty batch but the barrier reports **zero** domains committed to a
terminal state (e.g. persistent SQLite write error), return an error instead of looping forever.

### New flag
`--dispatch-batch` (int, default `20000`) — domains fetched + resolved per barrier iteration.
Separate from `--commit-batch` (SQLite rows/transaction, default 200) and `--seed-chunk` (5000).
Peak in-flight memory ≈ `dispatch-batch` domains + their `results` buffer, independent of corpus size.

## 4. What does NOT change
- The two schedulers, `Discoverer`/`Resolver`, per-worker semaphore, single writer goroutine, and
  `CommitBatch` are unchanged. Only *how domains enter* (streamed batches vs one slice) changes.
- Resume contract: unchanged — a re-run starts the cursor at `""`, `PendingBatch` returns the
  not-done set, done domains are skipped. `--limit` + `ORDER BY` determinism still holds.
- `load` is unchanged (still one bulk SQLite→CH copy at the end).

## 5. Error handling
- Seed streaming: a `Seed` error aborts the stream and returns it (partial seed is fine — resume).
- `PendingBatch` / barrier errors abort the run; already-committed domains persist (resumable).
- No-progress guard prevents an infinite loop on a wedged writer.

## 6. Testing
- **`store.PendingBatch` (unit):** seed N domains, mark some `done`/`error`, walk `PendingBatch`
  with a small limit advancing the cursor; assert it returns exactly the not-done domains in
  `root_domain` order, in the right batches, and terminates (empty batch) — and that a done domain
  below the cursor is correctly skipped.
- **`input.StreamClickHouse` (integration, real CH):** stream a small `--limit` from
  `commoncrawl_domains` with `batchSize` small enough to force ≥3 `fn` calls; assert total count and
  that no batch exceeds `batchSize`. (Unit-level: the batching boundary logic can also be checked
  with a tiny fake `driver.Rows` if cheap; the real-CH path is the meaningful test.)
- **e2e (real CH + DNS):** `scan --limit 300 --dispatch-batch 100` (forces ≥3 barrier iterations),
  then assert every seeded domain reached a terminal status exactly once (no dup rows, no missed
  domains) and `load` counts match; re-run to confirm "0 pending". Watch RSS stays flat across
  batches (bounded memory is the whole point).

## 7. Key decisions
1. **Option A — bound memory only.** Keep SQLite-stage-then-`load`; disk (records volume) deferred.
2. **Cursor-paginated `PendingBatch`** on the existing PK, not repeated `LIMIT`-from-start (O(n) vs O(n²)).
3. **Batch-barrier dispatch with a commit barrier** — each batch durably committed before the cursor
   advances; no continuous/lease-based dispatch (keeps the resume model simple).
4. **`--dispatch-batch` flag** decouples in-flight memory from corpus size.

## 8. Deferred (unchanged from parent spec)
- Bound disk at full corpus: incremental CH flush during scan (SQLite holds queue+status only), or
  partitioned runs via `--offset`/domain-range. Decide when disk actually becomes the wall.
- Per-server circuit breaker (measure-first).
