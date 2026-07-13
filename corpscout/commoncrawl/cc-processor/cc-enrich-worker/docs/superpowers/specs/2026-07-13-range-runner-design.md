# Range runner: worker-native part orchestration — design

Date: 2026-07-13
Status: approved in discussion (this document is the written record)

## Goal

cc-enrich-worker processes a RANGE of WARC parts natively, split into two explicit workload
lanes (local = whole-WARC download, remote = S3 range reads), with a self-contained
produce/verify marker state machine and a fully decoupled ClickHouse loader. cc-crawl stays
untouched and usable; it retires in a follow-up only after the new path completes a verified
crawl (or a meaningful part range) with matching outputs.

## Why

- cc-crawl's value is its state machine, but it enforces it by regex-scraping worker logs
  across an exec boundary — fragile. In-process, the same contracts become function calls.
- The local/remote split lets two runner processes with lane-appropriate dynamics saturate a
  box: the local lane alternates download/CPU (and prefetches the next download), the remote
  lane streams small range reads continuously. No intra-part streaming complexity needed.
- Producers must run on remote machines with NO ClickHouse dependency: parquet is the durable
  output; loading is a separate, colocated-with-CH concern that can lag and catch up.

## Decisions (from design discussion)

1. `--mode local|remote` is REQUIRED — no default, no `all`. A runner invocation states
   exactly how it fetches. For tech/both, covering a whole range = explicitly launching both
   lanes. (Classification is advisory for lane assignment; a part still derives its true
   fetch strategy at open time, so a stale plan can only mis-assign a lane, never
   mis-process.)
   industry and embed accept ONLY `--mode remote`: their primary-pages-only selections are
   sparse, so there is no local lane for them — the remote runner takes EVERY non-empty part
   in the range (no classification filtering), and `--mode local` is rejected with an error.
2. Producer never touches ClickHouse. No credentials, no connectivity, no load step.
3. Load-state tracking = one marker file per part, NOT a shared DuckDB table (single-writer
   file shared across machines = sync races; corruption loses all state; markers are
   per-part-atomic, rsync-friendly, and already the operator vocabulary). DuckDB appears only
   as an optional READ-ONLY snapshot emitted by `status`.
4. Failure policy: continue + summary. A failed part is recorded (no marker) and the range
   continues; the runner exits non-zero listing failed parts; rerunning retries only unmarked
   parts. Circuit breaker: 5 CONSECUTIVE produce failures abort the run (protects unattended
   machines from e.g. expired credentials burning hours). Setup failures (bad base dir,
   missing catalog, bad reference/model for industry) abort immediately as today.
5. Per-part marker files, not per-parquet-file: the part is the retry and verify unit.

## Components

### 1. Classifier + `plan` subcommand

- DuckDB (existing dependency) queries the catalog parquet for parts A–B: per-part page
  counts and selected compressed bytes. Catalog remains local under OUT_BASE_DIR (S3-hosted
  catalog via httpfs is out of scope).
- Concurrent HeadObject sweep fetches each WARC's total size (~200 in flight; the fleet pays
  these HEADs at open time anyway). Cached in `<base>/<crawl-id>/warc/<selection>/plan.json`
  keyed by (crawl, selection, part) → {object_bytes, selected_bytes, pages}; reruns reuse it.
- coverage = selected_bytes / object_bytes; coverage >= --whole-warc-threshold → local,
  else remote; zero pages → empty (skipped, reported).
- `cc-enrich-worker plan --crawl-id C --selection S --parts A-B [--whole-warc-threshold N]
  [--estimate]` prints: counts per class, download total (local lane GiB), selected total,
  average coverage, and a threshold sweep (e.g. 30/50/70%). `--estimate` skips HEADs and
  assumes ~1 GiB per WARC for an instant preview.

### 2. Range producer

- Existing commands (tech, industry, embed, both) gain `--parts A-B --mode local|remote`.
  Single-part `--part N` invocation remains unchanged for debugging and targeted retries.
- Loop over the parts of the requested class, ascending:
  - `.produced` marker exists → skip (resume).
  - output dir exists WITHOUT `.produced` → crashed/killed produce: the runner removes the
    dir and reproduces the part (cc-crawl's wipe-before-retry behavior, now in-process).
    This replaces the single-part invocation's "output dir not empty" fatal for range runs.
  - embed keeps its existing verify-and-skip on the embeddings file as an inner safety net;
    the `.produced` marker is what governs range-level skipping for all commands.
  - produce: the existing single-part pipeline, refactored so run() becomes
    runPart(...) error — all per-part log.Fatalf paths become returned errors. Existing
    per-part contracts unchanged: empty-output-dir rule, >50% fetch-error refusal,
    zero-domains refusal, streamer abort-on-failure.
  - verify: row counts taken in-process from the streamer/result (no log parsing).
  - write the `.produced` marker as a SIBLING of the output dir
    (`out_<cmd>_<part>.produced`, matching cc-crawl's `.loaded` placement), containing JSON
    {part, cmd, rows per kind, duration, source_run_id, finished_at}. Written via
    temp+rename.
- Lane dynamics:
  - `--mode local`: prefetch — download part N+1's WARC (through the existing plan.Open
    whole-WARC path) while part N processes. Exactly one ahead; disk bound = 2 WARCs.
  - `--mode remote`: parts strictly sequential; the continuous range-fetch pipeline has no
    download bubble, and memory stays O(chunk).
- Failure handling per Decisions #4. Summary at end: produced / skipped / failed(list) /
  wall-clock; exit 0 only if no produce failures.
- Industry/embed: single lane per Decisions #1 — `--mode remote` processes every non-empty
  part in the range; `--mode local` errors. If two GPU-feeding runners ever run at once,
  --embed-concurrency must be split manually (documented; no automation in this cycle).

### 3. Loader

- `cc-enrich-worker load --scan <root> [--loop 5m] [--parallel K, default 1]`
  (existing `load --dir/--file` forms unchanged).
- Walk `<root>` recursively for `out_*.produced` markers lacking a sibling `.loaded`;
  for each: load every fixed-name parquet in the dir via the existing native-driver kind
  mapping, compare loaded row counts against the counts recorded in `.produced`, then write
  `.loaded` (temp+rename). Mismatch or CH error → log, leave pending, continue; the next
  sweep retries. `--loop` re-sweeps forever (the catch-up daemon); without it, one sweep.
- Idempotent: ReplacingMergeTree natural keys absorb retried partial loads (verified against
  migrations in the 2026-07-12 review).
- Runs wherever ClickHouse is reachable; producers never depend on it.

### 4. Status

- `cc-enrich-worker status --root <dir> [--duckdb out.db]`: scan markers → per-class counts
  (produced / loaded / pending / missing), oldest pending age, failed-part list from the most
  recent runner summaries if present. `--duckdb` emits a regenerable snapshot table for ad-hoc
  SQL; never load-bearing.

## State machine (per part)

```
(no marker)            --produce+verify-->   .produced (JSON: row counts)
.produced (no .loaded) --loader load+verify-->  .produced + .loaded
.produced + .loaded    --> terminal; skipped by both producer and loader
produce failure        --> no marker; retried on next producer run; counted by breaker
load failure           --> stays pending; retried each loader sweep
```

## Out of scope

- cc-crawl changes or removal (follow-up after a verified crawl; includes ansible/deploy).
- S3-hosted catalog (DuckDB httpfs).
- Automatic GPU budget splitting across runners.
- Compaction of per-part parquet into larger files.

## Testing

- Unit: classifier SQL over a fixture catalog (DuckDB in-memory); threshold sweep math;
  marker read/write transitions incl. temp+rename atomicity; breaker (5 consecutive vs
  interleaved failures); parts-range parsing.
- Integration (fake getter WARC fixtures, as existing worker tests): a 4-part range with one
  part failing → continue + summary + exit code + resume-on-rerun behavior; local-lane
  prefetch produces identical output to sequential.
- Loader integration against REAL local ClickHouse (skip if unreachable): produce two parts,
  run `load --scan`, verify rows + `.loaded`; kill CH → parts stay pending → restart CH →
  next sweep catches up. Per the real-integration-tests rule.
- Parity gate for cc-crawl retirement (follow-up): one selection range run both ways,
  identical row counts per table.
