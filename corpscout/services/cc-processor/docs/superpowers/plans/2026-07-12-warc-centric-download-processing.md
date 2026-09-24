# WARC-Centric Common Crawl Processing Plan

**Status:** Proposed implementation plan based on the architecture agreed on 2026-07-12.

**Builder implementation details:** See
[`2026-07-12-cc-warc-index-builder.md`](./2026-07-12-cc-warc-index-builder.md). That focused plan is
authoritative for builder schema, restart state, validation, and publication details.

**Goal:** Replace URL-index-part processing with fixed WARC-file processing while preserving the existing
operator workflow: `cc-crawl -mode ... -parts ...`, local Parquet output, ClickHouse loading, and a local
`.loaded` marker written only after a successful load.

**Major simplification:** Remove `cc-download-worker` and the RustFS raw-staging path. `cc-enrich-worker`
reads Common Crawl directly. Complete WARCs are temporary local files and are never uploaded to RustFS.

## 1. Current and target models

### Current model

```text
URL-index part 0..N
  -> select pages for domains in that index part
  -> fetch every selected WARC record with a Range GET
  -> process and load the part
  -> out_<mode>_<part>.loaded
```

One URL-index part references records spread across almost every source WARC. Processing by URL-index part
therefore destroys WARC locality and creates hundreds of small requests to each WARC collection.

### Target model

```text
cc-warc-index-builder
  -> scan all Common Crawl URL-index files
  -> apply the existing page-selection policy globally
  -> build one immutable DuckDB catalog:
       warcs(warc_index -> filename, object size)
       pages(selected page -> warc_index, offset, length, domain metadata)

cc-crawl -mode tech -parts 0-10
  -> "part 0" now means fixed warc_index 0
  -> skip warc/out_tech_0.loaded when present
  -> invoke cc-enrich-worker for WARC 0
  -> calculate selected compressed bytes / complete WARC bytes
  -> whole WARC GET when coverage >= threshold
     OR exact record Range GETs when coverage < threshold
  -> process locally, write Parquet, load ClickHouse
  -> write warc/out_tech_0.loaded
```

No new runtime assignment API is introduced. The existing `-parts` range is retained; only its meaning
changes from URL-index-part number to WARC index.

## 2. Applications and responsibilities

### `cc-warc-index-builder` — new application

One-time/offline catalog builder. It owns:

- downloading `warc.paths.gz` and `cc-index-table.paths.gz`;
- assigning a fixed WARC index;
- measuring complete WARC object sizes;
- applying the page-selection policy across the entire crawl;
- building and validating the DuckDB catalog;
- publishing the catalog atomically for read-only processor use.

It never downloads WARC bodies, processes HTML, writes enrichment output, or loads ClickHouse.

### `cc-crawl` — same operator-facing application

The CLI remains:

```bash
./cc-crawl/bin/cc-crawl \
  -base /opt/companycollect/corpscout/commoncrawl/data \
  -mode tech \
  -parts 0-10 \
  -crawl CC-MAIN-2026-25
```

It continues to:

1. iterate the inclusive range;
2. skip a unit carrying the mode-specific `.loaded` marker;
3. start `cc-enrich-worker` produce;
4. verify expected output exists;
5. start the existing ClickHouse loader;
6. write `.loaded` only after a successful load.

The loop variable is now `warc_index`, even if the public flag remains named `-parts` for API
compatibility.

### `cc-enrich-worker` — direct Common Crawl reader

For one WARC index, it:

1. opens the immutable catalog read-only;
2. resolves the WARC filename and object size;
3. reads that WARC's selected page mappings;
4. chooses whole-object or exact-range input;
5. supplies byte-identical compressed WARC records to the existing parser;
6. runs the requested enrichment mode;
7. writes the existing local output Parquets.

It does not read or write RustFS raw packs.

### `cc-download-worker` — removed

After the direct path passes canary validation, remove the module, binary, container, docs, worklist
builder, SQLite analyzer, and RustFS staging-specific code.

## 3. Fixed WARC identity

`warc_index` is the zero-based line position of a WARC path in the crawl's official, immutable
`warc.paths.gz`:

```text
line 0     -> warc_index 0     -> fixed WARC filename
line 1     -> warc_index 1     -> fixed WARC filename
...
line M - 1 -> warc_index M - 1 -> final WARC filename for that crawl
```

Rules:

- preserve the official line order exactly;
- reject blank paths and duplicate filenames;
- store SHA-256 of `warc.paths.gz` in catalog metadata;
- processors validate the catalog's schema, requested crawl identity, and requested selection identity;
- index ranges are inclusive;
- the same `-parts 0-10` always resolves to the same eleven WARC files for a crawl/catalog.

`M` is discovered independently for every submitted crawl. Neither the builder nor the runtime assumes a
fixed WARC count, URL-index shard count, or Common Crawl schema generation.

There are no fixed WARC groups. Servers receive arbitrary ranges by running the same `cc-crawl` command
with different `-parts` values. One may run 10 WARC files and another 50,000.

## 4. DuckDB catalog

Default local path:

```text
<base>/<crawl>/catalog/pages25/catalog.duckdb
```

The completed catalog is immutable and copied to each processor machine. Processors open it read-only.
Building in place on a shared network filesystem is forbidden; build locally, validate, then publish/copy.

### `catalog_metadata`

One row containing:

- schema version and deterministic catalog ID;
- crawl ID, selection identity, pages-per-domain, and selection-policy version;
- SHA-256 of `warc.paths.gz` and `cc-index-table.paths.gz`;
- WARC count, selected page count, and distinct domain count;
- selection-policy checksum;
- DuckDB version, builder Git commit, and creation timestamp.

### `warcs`

Exactly one row per line of `warc.paths.gz`:

| Column | Type | Meaning |
|---|---|---|
| `warc_index` | uinteger | Fixed zero-based WARC index. |
| `warc_filename` | varchar | Complete Common Crawl object key. |
| `object_bytes` | ubigint | Complete compressed object size. |

Constraints/indexes:

- primary/unique identity on `warc_index`;
- unique `warc_filename`;
- positive `object_bytes`.

Do not store selected page count, selected bytes, utilization, or strategy. They are calculated from the
`pages` table when that WARC is processed.

### `pages`

One row per globally selected page:

| Column | Type | Meaning |
|---|---|---|
| `warc_index` | uinteger | Foreign key into `warcs`. |
| `root_domain` | varchar | Registered domain used by enrichment grouping. |
| `url` | varchar | Selected capture URL. |
| `domain_page_rank` | usmallint | Global page rank `1..N`; rank 1 is primary. |
| `content_languages` | nullable varchar | Common Crawl language metadata when available. |
| `warc_record_offset` | ubigint | Compressed record start in the WARC. |
| `warc_record_length` | ubigint | Compressed record length. |

Create the table physically ordered by `(warc_index, warc_record_offset)` and add an index on
`warc_index` if benchmarked selective lookups improve. The builder must benchmark both sorted-only and ART
index lookup because a full ART index over hundreds of millions of rows may not pay for its build/storage
cost.

`root_domain`, `url`, and `domain_page_rank` are required. Current enrichment uses them for domain
grouping, source attribution, and primary-page behavior.

### Atomic publication

The completed catalog is one self-contained DuckDB file. Write `catalog.duckdb.partial`, validate it,
force a checkpoint, close it, reopen it read-only for final validation, fsync it, and atomically replace
`catalog.duckdb`. Catalog identity, counts, manifest hashes, and schema version live in the single
`catalog_metadata` row. Processors reject missing or incompatible metadata.

## 5. Catalog construction algorithm

Run:

```bash
./cc-warc-index-builder/bin/cc-warc-index-builder \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25
```

There is no public URL-index `--parts` option. The builder always creates a complete crawl catalog.

### Phase A: WARC inventory and sizes

1. Download and checksum `warc.paths.gz`.
2. Insert one WARC row per line with its fixed index.
3. Measure object sizes with concurrent `HeadObject`, falling back to a one-byte GET that exposes total
   length when HEAD is unavailable.
4. Checkpoint successful size measurements in the build directory.
5. Retry only transient 429/500/502/503/504/network failures.
6. Reject completion if any WARC lacks a positive exact size.

Do not depend on anonymous `ListObjectsV2`; the Common Crawl bucket currently rejects anonymous listing.

### Phase B: globally correct page selection

The existing SQL ranks pages independently inside each URL-index shard. The catalog must enforce the limit
globally.

Use an exact, resumable two-stage selection:

1. Discover every source Parquet from `cc-index-table.paths.gz`.
2. For each source shard, apply the existing status/MIME/domain filters and retain local top `N` candidates
   per domain.
3. Store candidate shards only under the builder's temporary/resume directory.
4. Scan all candidate shards once with DuckDB.
5. Apply the same page-ranking expression globally with
   `row_number() OVER (PARTITION BY root_domain ORDER BY ...)`.
6. Add stable final tie breakers: URL, WARC filename, WARC offset.
7. Keep global rank `<= N`.

Local top-N followed by global top-N is exact: a row outside a shard's local top N cannot enter global top
N because N better rows for the same domain already exist in that source shard.

### Phase C: map and finalize

1. Join selected `warc_filename` values to the fixed WARC inventory once.
2. Store `warc_index` on each page row.
3. Fail any missing or multiple WARC match.
4. Create final `pages` ordered by WARC index and offset.
5. Create/benchmark the optional WARC-index lookup index.
6. Write catalog metadata, checkpoint, close, validate, and publish atomically.

Validation requires:

- WARC indexes are contiguous from zero and filenames unique;
- every selected page maps to exactly one WARC;
- each domain has at most N selected rows and exactly one rank-1 row;
- offsets and lengths are positive;
- overflow-safe coordinate bounds: offset is within the object, then length is at most object size minus
  offset;
- no duplicate `(warc_index, offset, length)` capture;
- repeated builds from the same source manifests produce identical logical rows/catalog ID.

## 6. Runtime WARC query and strategy

For `part=N`, `cc-enrich-worker` runs one read-only catalog query:

```sql
SELECT
    w.warc_index,
    w.warc_filename,
    w.object_bytes,
    p.root_domain,
    p.url,
    p.domain_page_rank,
    p.content_languages,
    p.warc_record_offset,
    p.warc_record_length
FROM warcs w
LEFT JOIN pages p USING (warc_index)
WHERE w.warc_index = ?
ORDER BY p.warc_record_offset;
```

The mode resolves the matching catalog before this query: technology and combined processing use their
configured multi-page catalog (normally `pages25`), while primary-page/industry processing uses `pages1`.
Do not substitute rank 1 from a differently ranked catalog. The query therefore returns every page from
the already-correct catalog for that mode.

From those rows:

```text
selected_pages = count(page rows)
selected_bytes = sum(warc_record_length)
coverage       = 100 * selected_bytes / object_bytes
```

Decision:

```text
coverage >= --whole-warc-threshold -> whole WARC
coverage <  --whole-warc-threshold -> exact ranges
```

Default threshold: 50%. It is runtime configuration, not catalog state.

A WARC with zero selected pages is a successful no-op. `cc-enrich-worker` reports it explicitly, and
`cc-crawl` records completion so it is not retried forever.

## 7. Direct Common Crawl input

Both input strategies feed the existing parser with the same byte-for-byte compressed WARC record.

### Whole-WARC path

1. Download the complete object to `<temp>/warc_<index>.partial`.
2. Verify downloaded size equals `object_bytes`.
3. Atomically rename to the complete temporary file.
4. Use `ReadAt(offset, length)` for every selected page; do not parse unrelated records.
5. Pass each selected compressed member through the existing WARC/HTTP parser and processor.
6. Delete the temporary complete WARC after processor output is durably committed.

The complete source WARC never goes to RustFS, local S3, or the final output directory.

### Exact-range path

Retain the old direct range behavior:

- one source range per selected record;
- configured concurrency;
- logical record attempts and per-attempt timeout;
- AWS adaptive retry throttling;
- shared cooldown for escaped throttling;
- 429, 503, SDK retry, body-read, and stable failure-reason metrics.

### Bounded resources

- Whole WARCs are streamed to disk, never held in a `[]byte`.
- Whole-WARC concurrency is separately bounded from exact-range concurrency.
- A WARC's selected page rows may be held in memory; the complete crawl may not.
- Temporary WARC files are removed on success and stale `.partial` files are removed on restart.

## 8. Processing and `.loaded` tracking

Keep the existing output basenames and marker semantics, but place them under a new WARC-processing
directory so old URL-index-part markers cannot accidentally skip WARC indexes with the same number:

```text
data/<crawl>/warc/
  out_tech_0/
  out_tech_0.loaded
  out_industry_0/
  out_industry_0.loaded
```

The number now identifies `warc_index=0` instead of URL-index part 0. The separate directory is an
internal storage change; the `cc-crawl` command-line API remains unchanged.

For each `(mode, warc_index)`:

1. If `.loaded` exists, `cc-crawl` skips it.
2. Otherwise remove stale/partial output and run produce.
3. Produce commits its Parquet files atomically.
4. `cc-crawl` verifies the expected output contract.
5. Run the existing ClickHouse loader.
6. Write `.loaded` only after loader exit 0.

If ClickHouse accepted an insert but the process failed before writing `.loaded`, the WARC is replayed.
Logical output must therefore remain idempotent. Validate with `FINAL`; physical duplicate parts may exist
until ReplacingMergeTree merges.

Embedding has no ClickHouse load and keeps its current durable-output completion rule.

## 9. Domain aggregation correctness gate

A domain's selected pages may reside in several WARCs. Current tech processing groups all selected pages
for a domain before unioning technologies/contacts/identifiers and choosing domain metadata. Processing
one WARC at a time can create partial domain aggregates, and a later partial one-row-per-domain result may
replace a fuller result.

Measure immediately after building the catalog:

```sql
SELECT
    count(*) AS domains,
    count_if(warc_count > 1) AS cross_warc_domains,
    max(warc_count) AS max_warcs_per_domain
FROM (
    SELECT root_domain, count(DISTINCT warc_index) AS warc_count
    FROM pages
    GROUP BY root_domain
);
```

Before production cutover, choose and test one solution:

1. Recommended: make WARC processing emit page-additive facts and perform deterministic domain reduction
   downstream.
2. Alternatively: after direct WARC reads, add a domain-complete reduction pass that combines outputs from
   every contributing WARC.
3. Do not rely on ClickHouse insert idempotency alone; it prevents duplicate logical keys but does not turn
   partial aggregates into complete ones.

Primary-page-only behavior remains deterministic because the catalog stores one global
`domain_page_rank=1` row.

## 10. CLI changes

### `cc-warc-index-builder`

Initial flags:

```text
--base
--crawl
--pages-per-domain        default 25
--threads                 DuckDB threads
--memory-limit            explicit DuckDB memory cap
--temp-dir                DuckDB spill/build directory
--warc-size-concurrency   concurrent metadata requests
--rebuild
```

### `cc-crawl`

Keep existing public flags and range grammar. Update help text so `-parts` clearly means fixed WARC
indexes for catalogs using the new schema. Do not launch a new downloader.

### `cc-enrich-worker`

Keep mode, crawl, selection, part, output, concurrency, chunk, technology, embedding, and load arguments.
Add internal/defaulted catalog resolution and whole-WARC threshold configuration. `--part` means WARC
index.

Runtime catalog lookup can initially use a small embedded Python/DuckDB helper that opens the local catalog
read-only and exports only one WARC's rows to a temporary Parquet file. Benchmark that against a direct
DuckDB Go reader before accepting the implementation; do not add cgo to the static worker without a proven
benefit.

## 11. Logging

No timer-based progress logs.

### Catalog builder

Emit one event per completed source shard and major phase:

- inventory ready;
- candidate shard ready;
- global selection ready;
- catalog validated;
- catalog ready.

Include rows, rows/s, bytes, human-readable size, elapsed time, DuckDB memory limit, and spill bytes.

### Enrichment worker

One completion event per WARC:

- crawl, selection, WARC index, filename, mode;
- strategy and threshold;
- selected pages/bytes, object bytes, and coverage percentage;
- actual source bytes and junk bytes;
- fetch/process elapsed time, requests/s, MiB/s;
- HTTP attempts, SDK retries, 429, 503, body-read errors/retries;
- failed page counts and stable failure reasons.

Errors are wrapped below the command boundary and logged once by the command.

## 12. Tests and benchmark gates

### Catalog tests

- Stable indexes from a fixed `warc.paths.gz` fixture.
- Duplicate/blank paths rejected.
- Interrupted size measurement resumes.
- A domain crossing several URL-index shards still has global top N only.
- Ranking ties resolve deterministically.
- Every selected page maps to one WARC.
- Invalid offsets/lengths and duplicate coordinates are rejected.
- Missing, invalid, or incompatible catalog metadata is rejected.
- `-parts 0-9` resolves to the same ten WARC filenames on two catalog copies.

### Input strategy tests

Synthetic objects:

- 60% selected bytes: one whole-object request;
- 20% selected bytes: one Range GET per selected page;
- zero selected pages: no source request.

Both strategies must provide byte-identical parser input and normalized processor output. Test below, at,
and above the threshold. Inject 503s, timeouts, truncated bodies, invalid ranges, and interrupted whole
downloads.

### Completion/replay tests

- Stop after output commit but before ClickHouse load.
- Stop after ClickHouse accepts data but before `.loaded` creation.
- Run overlapping `cc-crawl -parts` ranges on two servers.
- Confirm loaded WARC indexes skip locally.
- Confirm old RustFS ready objects cannot affect direct processing.
- Load the same WARC output twice and compare logical tables with `FINAL`.

### Performance gates

- Catalog construction uses vectorized DuckDB operations, never per-row application inserts.
- Initial catalog target: at least 100,000 selected rows/s on the current analysis host.
- One selective WARC lookup target: under 250 ms after warmup, including catalog open/query.
- Whole WARC and exact range are compared on the same real 1,000-WARC sample at 25%, 50%, and 75%.
- Enable whole-WARC production reads only if wall time improves at least 15% and byte inflation remains
  inside the agreed limit.
- Direct WARC partitioning must not increase normalized output loss for cross-WARC domains.

## 13. Implementation sequence

### Task 1: New catalog builder application

- Add `cc-warc-index-builder` as a sibling application.
- Reuse the existing ranking SQL, transient HTTP retry behavior, and WARC-size measurement code.
- Add WARC inventory, global two-stage selection, DuckDB schema, validation, and atomic publication.

### Task 2: Catalog query integration

- Add a concrete read-only catalog query boundary to `cc-enrich-worker`.
- Resolve current `--part` to WARC filename/object size/pages.
- Preserve root domain, URL, content languages, and primary-page rank.

### Task 3: Direct hybrid fetch

- Restore direct Common Crawl credentials/input to `cc-enrich-worker`.
- Add streaming whole-object-to-file support.
- Retain current exact-range fetch/retry/throttle implementation.
- Feed both paths through the same parser and processor functions.

### Task 4: WARC completion tracking

- Keep `cc-crawl` API and output/marker naming.
- Change the internal meaning of part to WARC index.
- Write new outputs beneath `<base>/<crawl>/warc/`, never the legacy `<base>/<crawl>/crawl/` directory.
- Handle zero-page WARC completion explicitly.
- Add overlapping/replay tests.

### Task 5: Domain correctness

- Measure cross-WARC domains on the real catalog.
- Make output page-additive plus deterministic domain reduction, or add a domain-complete reduce pass.
- Prove equivalence against the old domain-complete path.

### Task 6: Canary rollout

- Build and distribute a complete read-only catalog.
- Run 10 WARC files, then 1,000, then 10,000.
- Compare whole/exact input and normalized ClickHouse output.
- Run intentional replay and overlapping-server assignments.

### Task 7: Remove obsolete downloader/staging path

After canary acceptance:

- remove `cc-download-worker` entirely;
- remove `planstore`, `rangeplanner`, SQLite dependencies, and `cc-warc-analyzer`;
- remove RustFS staged-input code from `cc-enrich-worker`;
- remove unused `cc-raw/rawstore` and `cc-raw/rawstate` contracts while retaining/moving reusable fetch code;
- remove Common Crawl RustFS settings from this runtime path;
- update root/commoncrawl Makefiles, Dockerfiles, `.env.example`, READMEs, architecture docs, and raw-staging
  documentation.

Run Go tests, race tests, vet, native builds, Linux ARM64 builds, Python/DuckDB fixtures, and a repository
search for obsolete runtime part/worklist/RustFS references.

## 14. Rollback

Build the new catalog and direct path in parallel with the current binaries. Until correctness and
performance gates pass:

- do not delete existing RustFS packs;
- do not delete old `.loaded` markers;
- do not let old part-based state satisfy new WARC-index processing;
- do not remove `cc-download-worker` in the same commit that introduces the catalog builder.

Rollback means running the old binaries with their URL-index part interpretation. No catalog conversion is
required.
