# Range runner: worker-native part orchestration — design

Date: 2026-07-13
Status: implemented (`plan`, range `--parts --mode local|remote`, `load --scan/--watch`, `status`; see
cc-enrich-worker/README.md for the operator-facing docs). The optional `--duckdb` snapshot under
"4. Status" was not built (YAGNI) — `status` is markers-only, no ClickHouse or DuckDB dependency.
Amendment (2026-07-13): the `plan` sizing report was retired and replaced by `sync-db`, an explicit
"pull the catalog from S3/RustFS and cache it locally" command (idempotent; pre-warms the catalog on a
new host before the first range run).

> **Status update (2026-07-13, post-implementation):** the local lane and whole-WARC download mode were
> removed after implementation. Measuring the real `pages25` catalog showed a uniform distribution —
> ~3,500 selected pages / ~12% byte coverage per part — so range reads always win and the whole-file
> path never did. `--parts` now runs a single strategy (range reads over every non-empty part); the
> `--mode`, `--remote-max-pages`, `--download-parallel`, `--process-parallel`, and `--max-warc-files`
> flags, the coverage-threshold decision, and `plan`'s lane split + threshold sweep are gone. `plan` is
> now a pure sizing report. The body below is retained unedited as the original design record.

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

1. `--mode local|remote` is REQUIRED — no default, no `all`. The mode determines BOTH what
   the runner claims from the catalog AND how those parts are fetched:
   - At startup the runner queries the catalog (DuckDB) for parts A–B and selects the WARCs
     of its class: selected pages <= X → remote class, > X → local class (X =
     `--remote-max-pages`, the split threshold: the most pages a WARC may have and still be
     worth fetching page-by-page; see Classifier).
   - The lane then FORCES the fetch strategy — local always downloads the whole WARC to a
     temp file, processes it, and deletes the file; remote always uses S3 range reads. No
     open-time re-derivation: the runner is fully explicit about what it will do.
   - For tech/both, covering a whole range = explicitly launching both lanes (on different
     servers — see Deployment).
   industry and embed accept ONLY `--mode remote`: their primary-pages-only selections are
   sparse, so there is no local lane for them — the remote runner takes EVERY non-empty part
   in the range (no classification filtering), and `--mode local` is rejected with an error.
2. Producer never WRITES to ClickHouse — no load step, and tech/embed producers need no CH
   connectivity at all. Clarification: the industry producer still READS the NACE reference
   matrix from ClickHouse once at startup (as today); that is a fail-fast setup dependency,
   not part of the per-part pipeline, and does not weaken the "processing never blocks on
   CH" guarantee (an unreachable CH aborts before any part starts, never mid-range).
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

- DuckDB (existing dependency) queries the catalog parquet for parts A–B: per-part selected
  page count and selected compressed bytes. Catalog remains local under OUT_BASE_DIR
  (S3-hosted catalog via httpfs is out of scope).
- Split criterion is catalog-only — NO S3 HEAD sweep, no plan.json cache needed:
  selected pages <= `--remote-max-pages` X → remote class; > X → local; zero pages → empty
  (skipped, reported). The same X must be passed to both lanes' runners for a disjoint,
  complete partition of the range (each runner prints its X and class counts at startup so a
  mismatch is visible immediately).
- Rationale: page count is a direct proxy for when whole-WARC download beats per-record
  range reads (request-count economics), and it removes an entire network dependency from
  classification. The old byte-coverage criterion required per-WARC HeadObject calls; it
  survives only inside `plan` stats as an OPTIONAL `--head-sizes` enrichment.
- `cc-enrich-worker plan --crawl-id C --selection S --parts A-B [--remote-max-pages X]`
  prints: counts per class, selected pages/bytes totals per class, estimated download volume
  for the local class (parts x ~1 GiB, or exact with --head-sizes), and a threshold sweep
  over several X values so the split can be tuned from the real distribution.

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
- Lane dynamics — each lane is a two-pool pipeline with explicit, configurable parallelism:
  - `--mode remote`: `cc-enrich-worker tech --parts 1-100000 --mode remote` must just work.
    One part-pool processes `--warc-parallel X` (default 4) parts concurrently — each part
    runs the existing per-part pipeline (pages fetched by the existing page pool, aggregated,
    streamed to its own output dir) — all sharing one S3 transport sized to
    X * concurrency * PageConcurrency. Memory is O(chunk) * X.
  - `--mode local`: two pools connected by a bounded buffer of downloaded WARCs:
    - DOWNLOAD pool: `--download-parallel D` (default 2) whole-WARC downloads run
      continuously — as soon as one finishes, the next queued part starts — until every part
      in the runner's class is fetched. Backpressure is ONE explicit knob:
      `--max-warc-files N` (REQUIRED for --mode local, no default; recommended 5): the count
      of WARC files on disk — in-flight partial downloads PLUS downloaded-but-unprocessed —
      may never exceed N. A new download starts only when the count is below N; processing
      completing a part deletes its file and unblocks the pool. Disk sizing is therefore
      deterministic: the runner needs ~N x 1 GiB of temp space, independent of anything else
      on the volume. Validation: D <= N. (No free-space watermark: it would couple the
      runner's behavior to unrelated disk tenants.)
    - PROCESS pool: `--process-parallel P` (default 2) parts processed concurrently from the
      buffer, each feeding (local warc path, offset, length) page entries to the existing
      CPU-bound page workers; total page workers sized to the machine's cores. A part's
      temp WARC is deleted as soon as its part completes (produce + verify + marker).
    Download of later parts overlaps processing of earlier ones for the entire run — the
    continuous-batch behavior, not just prefetch-one.
- Failure handling per Decisions #4. Summary at end: produced / skipped / failed(list) /
  wall-clock; exit 0 only if no produce failures.
- Industry/embed: single lane per Decisions #1 — `--mode remote` processes every non-empty
  part in the range; `--mode local` errors. If two GPU-feeding runners ever run at once,
  --embed-concurrency must be split manually (documented; no automation in this cycle).

### 3. Loader

- `cc-enrich-worker load --scan <root> [--watch] [--parallel K, default 1]`
  (existing `load --dir/--file` forms unchanged).
- Without `--watch`: one sweep, then exit (cron-friendly). With `--watch`: stay running —
  fsnotify (inotify/kqueue) on marker creation for near-instant pickup, PLUS an unconditional
  fallback re-sweep every 5 minutes, because inotify events can be dropped on queue overflow
  and never arrive for files that appear via rsync/NFS from producer machines. The sweep is
  the correctness mechanism; inotify is the latency optimization.
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

## Deployment

- Local-lane and remote-lane runners are intended for DIFFERENT servers: the local lane is
  disk-write + CPU heavy (sustained ~D concurrent 1 GiB downloads, then pure local reads),
  the remote lane is S3-request-rate heavy with modest CPU. Separating them keeps each box's
  bottleneck singular and the S3 request budget accountable per host. Nothing in the code
  enforces this; it is the documented operating model.
- Both lanes write to the same OUT_BASE_DIR layout and marker vocabulary, so the loader and
  status commands see one uniform tree regardless of which host produced a part.

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
  part failing → continue + summary + exit code + resume-on-rerun behavior; remote lane with
  --warc-parallel 2 produces byte-identical outputs to sequential; local lane's
  download/process pools produce identical output to one-at-a-time, temp WARCs deleted after
  their part completes, buffer bound respected.
- Loader integration against REAL local ClickHouse (skip if unreachable): produce two parts,
  run `load --scan`, verify rows + `.loaded`; kill CH → parts stay pending → restart CH →
  next sweep catches up. Per the real-integration-tests rule.
- Parity gate for cc-crawl retirement (follow-up): one selection range run both ways,
  identical row counts per table.
