# `cc-warc-index-builder` Small-Task Execution Checklist

**Parent design:** [cc-warc-index-builder implementation plan](./2026-07-12-cc-warc-index-builder.md)

**Rule:** Complete, test, and commit one task before starting the next. Each task has one behavioral
purpose. Do not combine tasks merely because they touch the same Python module.

All implementation tests through T26 are offline. They use generated Parquets, temporary DuckDB files,
`httpx.MockTransport`, a local HTTP server where DuckDB HTTP behavior is required, and subprocess-held
`flock`. Public Common Crawl access is reserved for T27.

## Dependency order

```text
Foundation       T01 -> T02 -> T03 -> T04
Discovery        T04 -> T05 -> T06 -> T07
Selection        T07 -> T08 -> T09
Build state      T04 + T06 + T07 + T09 -> T10
Candidates       T08 + T09 + T10 -> T11 -> T12
WARC sizes       T04 + T06 + T10 -> T13 -> T14
Final catalog    T12 + T14 -> T15 -> T16 -> T17 -> T18 -> T19 -> T20 -> T21
Operator path    T21 -> T22 -> T23 -> T24 -> T25 -> T26 -> T27
```

T11-T12 and T13-T14 can be implemented in parallel only in separate worktrees because both paths update
the build-state schema and orchestration.

## Common verification

From `corpscout/commoncrawl/cc-warc-index-builder/`:

```bash
uv sync --frozen
make test
make build
./bin/cc-warc-index-builder --help
```

## Foundation

### T01 — Package skeleton

**Files:** `pyproject.toml`, `uv.lock`, `Makefile`, `warc_index_builder/{__init__,__main__}.py`, test setup.

**Deliver:** locked DuckDB/httpx/humanize dependencies, console entry point, generated `bin/` convention,
and help output. No build behavior.

**Verify:** `uv sync --frozen`, `make build`, `make test`, binary `--help`.

**Commit:** `feat(commoncrawl): scaffold warc index builder`

### T02 — CLI values and safe path derivation

**Depends on:** T01.

**Files:** `warc_index_builder/__main__.py`, `tests/test_cli.py`.

**Deliver:** all planned flags/defaults, strict crawl syntax, pages range, resolved-base containment, exact
`<base>/<crawl>/catalog/pagesN` derivation, and `--check/--rebuild` conflict validation. No filesystem
deletion yet.

**Verify:** `uv run pytest tests/test_cli.py -q -k 'args or path'`.

**Commit:** `feat(commoncrawl): validate builder inputs`

### T03 — JSON events and one error boundary

**Depends on:** T02.

**Files:** `warc_index_builder/events.py`, `warc_index_builder/__main__.py`, `tests/test_cli.py`.

**Deliver:** one structured event emitter, maintained binary-size formatting, and one command boundary that
logs returned errors once. Lower modules do not log failures.

**Verify:** `uv run pytest tests/test_cli.py -q -k 'event or error'`.

**Commit:** `feat(commoncrawl): add builder events`

### T04 — Build lock and rebuild filesystem lifecycle

**Depends on:** T02, T03.

**Files:** `warc_index_builder/catalog.py`, `warc_index_builder/__main__.py`, `tests/test_cli.py`,
`tests/test_catalog.py`.

**Deliver:** real nonblocking `flock`, lock held through the command, exact `.build` cleanup only after path
containment, and old final-catalog preservation during `--rebuild`.

**Verify:** `uv run pytest tests -q -k 'lock or rebuild or containment'`.

**Commit:** `feat(commoncrawl): lock catalog builds`

## Crawl discovery

### T05 — Manifest byte download and snapshot

**Depends on:** T03.

**Files:** `warc_index_builder/manifests.py`, `tests/test_manifests.py`.

**Deliver:** exact compressed-byte download, timeout, transient retry, permanent 404, SHA-256, fsynced
`.partial`, and atomic snapshot rename.

**Verify:** `uv run pytest tests/test_manifests.py -q -k 'download or snapshot'`.

**Commit:** `feat(commoncrawl): snapshot crawl manifests`

### T06 — Manifest parsing and crawl inventories

**Depends on:** T05.

**Files:** `warc_index_builder/manifests.py`, `tests/test_manifests.py`.

**Deliver:** gzip validation; WARC indexes from original `warc.paths.gz` line order; URL-index sources from
original `cc-index-table.paths.gz` order; exact `subset=warc` filtering; duplicate, blank, prefix, and empty
inventory rejection. No fixed counts or sorting.

**Verify:** `uv run pytest tests/test_manifests.py -q -k 'warc_manifest or index_manifest'`.

**Commit:** `feat(commoncrawl): derive crawl inventories`

### T07 — Source schema inspection and normalization

**Depends on:** T06.

**Files:** `warc_index_builder/manifests.py`, `warc_index_builder/selection.py`,
`tests/test_manifests.py`, `tests/test_selection.py`.

**Deliver:** required-field validation, explicit accepted source-type matrix, stable candidate types,
optional MIME/language expressions, ignored future columns, per-source capability record, and path-free
aggregate schema fingerprint. Never branch on crawl ID/year.

**Verify:** legacy/current/mixed/future fixtures and precise missing/incompatible field failures:
`uv run pytest tests -q -k schema`.

**Commit:** `feat(commoncrawl): normalize crawl index schemas`

## Selection identity

### T08 — Canonical page-selection policy

**Depends on:** T07.

**Files:** `warc_index_builder/selection.py`, `tests/test_selection.py`.

**Deliver:** one eligibility/ranking definition for N=1 and N>1, migrated multilingual fixtures, named
ranking columns, deterministic final ties, explicit null order, and no generated SQL containing source or
output paths.

**Verify:** `uv run pytest tests/test_selection.py -q`.

**Commit:** `feat(commoncrawl): centralize page selection`

### T09 — Golden deterministic encodings and hashes

**Depends on:** T07, T08.

**Files:** `warc_index_builder/selection.py`, `warc_index_builder/manifests.py`,
`warc_index_builder/catalog.py`, relevant tests.

**Deliver:** four explicit functions for selection-policy hash, aggregate source-schema hash, ordered WARC
inventory hash, and `catalog_id`. Hash canonical path-independent bytes; never hash SQL containing source
URLs, local paths, or temporary filenames. Freeze golden fixture hashes.

**Verify:** `uv run pytest tests -q -k 'hash or identity'`; repeated processes and different temp paths
produce identical hashes.

**Commit:** `feat(commoncrawl): define catalog identities`

## Resumable build state

### T10 — Typed build-state database

**Depends on:** T04, T06, T07, T09.

**Files:** `warc_index_builder/catalog.py`, `tests/test_catalog.py`.

**Deliver:** explicit `build_identity`, `warc_inventory`, and `source_shards` tables; transactions; exact
identity match/conflict; source/WARC seeding; `running -> pending` recovery. No generic key/value store.

**Verify:** `uv run pytest tests/test_catalog.py -q -k state`.

**Commit:** `feat(commoncrawl): persist catalog build state`

## Candidate shards

### T11 — Local candidate SQL and coordinate canonicalization

**Depends on:** T08, T09, T10.

**Files:** `warc_index_builder/selection.py`, `warc_index_builder/catalog.py`,
`tests/test_selection.py`, `tests/test_catalog.py`.

**Deliver:** eligible projection into the fixed candidate schema; within-source conflict detection;
canonicalize `(warc_filename, offset, length)` before `row_number`; deterministic language winner; local
top N with every global ranking key retained.

**Verify:** `uv run pytest tests -q -k 'local_candidate or coordinate'`, including duplicates at the N
boundary that must not displace a unique capture.

**Commit:** `feat(commoncrawl): select local page candidates`

### T12 — Candidate artifact, reuse, and retry lifecycle

**Depends on:** T11.

**Files:** `warc_index_builder/catalog.py`, `warc_index_builder/events.py`,
`warc_index_builder/__main__.py`, `tests/test_catalog.py`.

**Deliver:** vectorized DuckDB `COPY`; `.partial` validation and rename; ready-state commit; footer/schema/
row-count reuse; corrupt-only rebuild; transient DuckDB HTTP retry; stop with earlier candidates preserved;
one completion event per source. PyArrow remains absent.

**Verify:** `uv run pytest tests/test_catalog.py -q -k candidate`.

**Commit:** `feat(commoncrawl): resume candidate builds`

## Exact WARC sizes

### T13 — Single WARC-size probe

**Depends on:** T03, T06.

**Files:** `warc_index_builder/object_sizes.py`, `tests/test_object_sizes.py`.

**Deliver:** HEAD positive length, one-byte Range fallback, strict `Content-Range`, redirect handling,
Retry-After/backoff, and permanent/transient classification.

**Verify:** `uv run pytest tests/test_object_sizes.py -q -k probe`.

**Commit:** `feat(commoncrawl): probe exact warc sizes`

### T14 — Concurrent size pool and coordinator checkpointing

**Depends on:** T10, T13.

**Files:** `warc_index_builder/object_sizes.py`, `warc_index_builder/catalog.py`,
`warc_index_builder/events.py`, related tests.

**Deliver:** bounded workers, shared 429/503 cooldown, result queue, coordinator-only DuckDB batches,
null-only resume, exact attempts/retries/status metrics, and stable ordered inventory hash.

**Verify:** `uv run pytest tests/test_object_sizes.py tests/test_catalog.py -q -k warc_size`.

**Commit:** `feat(commoncrawl): checkpoint warc sizes`

## Final catalog

### T15 — Exact global selection relation

**Depends on:** T12.

**Files:** `warc_index_builder/catalog.py`, `tests/test_catalog.py`.

**Deliver:** cross-source coordinate canonicalization/conflict checks, same total reranking, global top N,
and WARC filename mapping checks.

**Verify:** randomized multi-shard fixtures prove two-stage output equals a direct single-pass global top-N
query: `uv run pytest tests/test_catalog.py -q -k global`.

**Commit:** `feat(commoncrawl): select global domain pages`

### T16 — Final partial database and `warcs` table

**Depends on:** T14, T15.

**Files:** `warc_index_builder/catalog.py`, `tests/test_final_catalog.py`.

**Deliver:** create `catalog.duckdb.partial`, final DDL, and complete `warcs` bulk load with contiguous indexes,
unique filenames, positive sizes, and inventory hash.

**Verify:** `uv run pytest tests/test_final_catalog.py -q -k final_warcs`.

**Commit:** `feat(commoncrawl): materialize catalog warcs`

### T17 — Final WARC-ordered `pages` table

**Depends on:** T15, T16.

**Files:** `warc_index_builder/catalog.py`, `tests/test_final_catalog.py`.

**Deliver:** bulk filename-to-index join, stable page types, physical `(warc_index, offset)` order, missing/
ambiguous mapping diagnostics, duplicate rejection, and overflow-safe bounds checks using
`offset <= object_bytes` then `length <= object_bytes - offset`.

**Verify:** `uv run pytest tests/test_final_catalog.py -q -k final_pages`.

**Commit:** `feat(commoncrawl): materialize catalog pages`

### T18 — Deterministic metadata and catalog ID

**Depends on:** T09, T16, T17.

**Files:** `warc_index_builder/catalog.py`, `tests/test_catalog.py`, `tests/test_final_catalog.py`.

**Deliver:** exactly one metadata row with versions, counts, canonical hashes, deterministic catalog ID,
and informational timestamp excluded from identity.

**Verify:** `uv run pytest tests/test_catalog.py tests/test_final_catalog.py -q -k metadata`.

**Commit:** `feat(commoncrawl): identify warc catalogs`

### T19 — Shared invariant validation

**Depends on:** T18.

**Files:** `warc_index_builder/catalog.py`, `tests/test_final_catalog.py`.

**Deliver:** one validation path for metadata, WARC identity/hash, mapping, overflow-safe coordinates,
duplicate coordinates, domain ranks, and stored counts; sample invalid rows in returned errors.

**Verify:** one failing fixture per invariant: `uv run pytest tests/test_final_catalog.py -q -k validation`.

**Commit:** `feat(commoncrawl): validate warc catalogs`

### T20 — Build, checkpoint, close, and reopen the partial catalog

**Depends on:** T19.

**Files:** `warc_index_builder/catalog.py`, `tests/test_catalog.py`.

**Deliver:** assemble partial catalog, validate, `FORCE CHECKPOINT`, close, reopen read-only, and validate
again. No final-path mutation.

**Verify:** fault injection around build/checkpoint/close/reopen: `uv run pytest tests/test_catalog.py -q -k final_build`.

**Commit:** `feat(commoncrawl): finalize catalog partials`

### T21 — Manifest recheck and atomic publication

**Depends on:** T05, T20.

**Files:** `warc_index_builder/manifests.py`, `warc_index_builder/catalog.py`,
`warc_index_builder/__main__.py`, related tests.

**Deliver:** re-download/hash both manifests, block changed sources, fsync file, atomic `os.replace`, fsync
directory, clean staging only after success, quick valid-final reuse, and failed-rebuild preservation.

**Verify:** `uv run pytest tests -q -k publish`, with a failure injected at every boundary.

**Commit:** `feat(commoncrawl): publish warc catalogs atomically`

## Operator path and release

### T22 — Read-only `--check` and summaries

**Depends on:** T19, T21.

**Files:** `warc_index_builder/catalog.py`, `warc_index_builder/__main__.py`, related tests.

**Deliver:** read-only catalog audit, shared validators, catalog/WARC/page/domain/coverage/cross-WARC
statistics, isolated spill scratch, and scratch cleanup.

**Verify:** `uv run pytest tests -q -k check`; catalog bytes remain identical and no network is used.

**Commit:** `feat(commoncrawl): check completed warc catalogs`

### T23 — End-to-end offline orchestration

**Depends on:** T22.

**Files:** `warc_index_builder/__main__.py`, `tests/test_cli.py`, test helpers only.

**Deliver:** connect existing direct functions into phase orchestration; fake Common Crawl endpoint with tiny
manifests/index Parquets/WARC metadata; fresh build, interrupt/resume, quick reuse, rebuild, and check.

**Verify:** `uv run pytest tests/test_cli.py -q`, then `make test`.

**Commit:** `test(commoncrawl): cover catalog build end to end`

### T24 — Root wiring and operator documentation

**Depends on:** T23.

**Files:** root/package `Makefile`, root/package `README.md`, environment example, plan status.

**Deliver:** build/test/clean wiring; generic old/new `--crawl` examples; copy/resume/rebuild/check; disk/temp/
locking/event/recovery documentation. Keep both legacy builders and the downloader through runtime canary.

**Verify:** root `make build`, root `make test`, clean rebuild, documented commands, and repository search for
hardcoded new-builder crawl/count/schema claims.

**Commit:** `docs(commoncrawl): document warc catalog builder`

### T25 — Benchmark-only lookup study

**Depends on:** T23.

**Files:** benchmark script and result document only.

**Deliver:** sorted zonemap measurements for row-group sizes 16,384, 65,536, and 122,880 across 1, 10,
1,000, and 50,000 WARC lookups; record build memory, catalog size, and cold/warm latency. Do not add ART in
this task.

**Verify:** repeatable benchmark command and captured results.

**Commit:** `perf(commoncrawl): benchmark warc catalog lookup`

### T26 — Optional ART-index change

**Depends on:** T25 and only exists if the benchmark justifies it.

**Files:** `warc_index_builder/catalog.py`, focused tests, benchmark result update.

**Deliver:** ART creation and validation with its measured memory/storage tradeoff. If the gate is not met,
mark T26 skipped and keep sorted zonemaps.

**Verify:** focused layout tests, full suite, and repeat of T25 comparison.

**Commit:** `perf(commoncrawl): index warc page lookups`

### T27 — Real-crawl acceptance

**Depends on:** T24, T25, and T26 when applicable.

**Files:** no production change. Any discovered defect becomes a new focused task and commit.

**Deliver:** use the same binary and command shape to build/resume/check at least one legacy and one current
crawl on the intended build server; build pages1/pages25 as required; copy catalogs to a processing machine.

**Verify:** discovered counts match manifests, no crawl-specific code change, forced interruption resumes,
second run reuses without network work, `--check` passes, copied catalog validates, and lookup latency meets
the recorded gate.

**Commit:** none for a successful operational gate; record measured results in the plan or operations log.

## Stop conditions

Stop and create a new small task instead of broadening the active one when:

- a required Common Crawl field is absent;
- a source type cannot normalize without changing selection semantics;
- a real duplicate conflicts in a way fixtures did not define;
- a hash would include a local/source URL or temporary path;
- a benchmark requires a final-schema change;
- publication or replay cannot be proven deterministic.

Do not solve these with crawl-ID branches, generic service abstractions, or hidden per-crawl defaults.
