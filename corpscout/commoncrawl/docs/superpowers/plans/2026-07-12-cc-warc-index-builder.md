# `cc-warc-index-builder` Implementation Plan

**Status:** Ready for implementation.

**Goal:** Build one complete, immutable, WARC-oriented DuckDB catalog from Common Crawl's published URL
Index. The catalog replaces runtime URL-index-part worklists and lets processors resolve a fixed
`warc_index` to the selected pages stored in that WARC.

**Related architecture:** [WARC-centric Common Crawl processing plan](./2026-07-12-warc-centric-download-processing.md).
This document is authoritative for the catalog builder when the two plans differ.

## 1. Decisions fixed by this plan

- Add a standalone sibling application at `corpscout/commoncrawl/cc-warc-index-builder/`.
- Implement it in Python with DuckDB. DuckDB owns all large scans, ranking, joins, and writes.
- Reuse Common Crawl's official URL Index instead of constructing a page-to-WARC index from raw data.
- Build a complete catalog for one `(crawl, pages_per_domain, selection_policy_version)` invocation.
- Do not expose URL-index `--part` or `--parts`; source shards are an internal restart boundary only.
- Assign `warc_index` from the original zero-based line order of `warc.paths.gz`.
- Select pages globally, not independently inside each source Parquet.
- Publish one self-contained `catalog.duckdb`; metadata inside the database is the commit contract.
- Keep temporary candidates and restart state local. Delete them only after the final catalog is published.
- Do not store derived WARC utilization or processing state in the catalog.
- Do not add interfaces, service layers, factories, or a Go wrapper around DuckDB.

The builder does not download WARC bodies, select the whole-WARC threshold, run enrichment, load
ClickHouse, or track completed processing.

## 2. Reused Common Crawl data

Inputs for crawl `<crawl>`:

```text
https://data.commoncrawl.org/crawl-data/<crawl>/warc.paths.gz
https://data.commoncrawl.org/crawl-data/<crawl>/cc-index-table.paths.gz
```

`cc-index-table.paths.gz` identifies the exact Parquet files in `subset=warc`. Those Parquets already
provide the fields required by the existing selection policy and runtime fetches:

```text
url_host_registered_domain
url_host_name
url
url_path
fetch_status
content_mime_type
content_mime_detected
content_languages
warc_filename
warc_record_offset
warc_record_length
```

The existing manifest discovery in `index-builder/index_builder/__main__.py` and selection behavior in
`cc-download-worker/internal/worklistbuilder/builder.py` are migration inputs. The latter is the canonical
starting point because it rejects blank domains, URLs, WARC filenames, negative offsets, and non-positive
lengths. Preserve the pre-2018 schema handling from `index-builder/index_builder/worklist.py`.

Do not use the official URL Index as a per-WARC runtime database. The `CC-MAIN-2026-25` measurement—one
inspected URL-index Parquet contained 6,890,137 rows and referenced all 100,000 WARC objects—is evidence
for materializing a WARC-oriented layout, not a crawl-specific assumption.

### One implementation for every crawl

The operator submits any published crawl ID matching `CC-MAIN-XXXX-YY`, and the same application produces
that crawl's catalog:

```bash
cc-warc-index-builder --crawl CC-MAIN-2016-22
cc-warc-index-builder --crawl CC-MAIN-2026-25
```

Any future concrete `CC-MAIN-YYYY-NN` identifier works as soon as its manifests are published; it requires
no code release or crawl-specific configuration.

For every invocation, discover from the submitted crawl rather than constants:

- the WARC count and exact WARC filenames from `warc.paths.gz`;
- the URL-index source count and exact Parquet paths from `cc-index-table.paths.gz`;
- the schema capabilities and physical types of every source Parquet;
- the completed catalog path under `<base>/<crawl>/catalog/pagesN/`.

Never branch on crawl year or crawl ID. There must be no assumptions that a crawl has 100,000 WARCs, 300
URL-index files, a particular Parquet filename suffix, or the newest schema. Direct inspection already
shows why discovery is required:

| Crawl | Published WARC paths | Inspected URL-index columns | Relevant difference |
|---|---:|---:|---|
| `CC-MAIN-2013-20` | 31,600 | 27 | No `content_languages`. |
| `CC-MAIN-2016-22` | 24,500 | 27 | No `content_languages`. |
| `CC-MAIN-2018-05` | 80,000 | 27 | No `content_languages`. |
| `CC-MAIN-2026-25` | 100,000 | 32 | Current optional metadata columns. |

These are compatibility fixtures and operational evidence, not a hardcoded crawl inventory.

Schema handling is capability-based per source shard:

- required selection and coordinate columns must exist; otherwise fail with the exact missing-column list;
- `content_languages` is optional and normalizes to nullable `VARCHAR` when absent;
- `content_mime_detected` is optional or may be all-null; fall back to `content_mime_type`;
- additional future columns are ignored unless a later selection-policy version explicitly uses them;
- compatible source numeric/string types are explicitly cast into the stable candidate schema;
- source shards with compatible but different optional columns normalize to the same candidate schema.

Required columns are `url`, `url_host_name`, `url_host_registered_domain`, `url_path`, `fetch_status`,
`content_mime_type`, `warc_filename`, `warc_record_offset`, and `warc_record_length`. A future incompatible
rename of a required field fails safely instead of silently changing selection.

Accepted source types are explicit:

- URL/domain/path/MIME/WARC/language columns must be DuckDB `VARCHAR`;
- `fetch_status`, offset, and length accept only signed or unsigned integral DuckDB types;
- validate signed values before casting and reject float, decimal, or string coordinates/status values;
- normalize status through `BIGINT` for filtering and normalize valid offsets/lengths to `UBIGINT`;
- missing optional language becomes `CAST(NULL AS VARCHAR)`;
- missing optional detected MIME uses reported MIME directly.

The stable candidate schema is:

```text
source_index              UINTEGER
root_domain               VARCHAR
url                       VARCHAR
content_languages         VARCHAR nullable
warc_filename             VARCHAR
warc_record_offset        UBIGINT
warc_record_length        UBIGINT
rank_main_site            UTINYINT
rank_homepage             UTINYINT
rank_priority_path        UTINYINT
rank_path_depth           UBIGINT
rank_path_length          UBIGINT
rank_apex                 UTINYINT
```

A crawl is buildable once both manifests are published and valid. Before final publication, download both
manifests again and require their SHA-256 hashes to match the snapshots used by the build. If a newly
published crawl changes while it is being indexed, preserve staging state but do not publish a mixed
catalog.

## 3. Operator contract

Build or resume:

```bash
./cc-warc-index-builder/bin/cc-warc-index-builder \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --memory-limit 48GB
```

Validate an existing catalog without network access or modifying the catalog:

```bash
./cc-warc-index-builder/bin/cc-warc-index-builder \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --check
```

Initial flags:

| Flag | Behavior |
|---|---|
| `--base` | Defaults to `OUT_BASE_DIR`, then `./data`. |
| `--crawl` | Required Common Crawl ID. |
| `--pages-per-domain` | Integer `1..65535`, default `25`. `1` selects only the representative page. |
| `--threads` | Optional DuckDB thread limit; DuckDB's default is used when omitted. |
| `--memory-limit` | Optional explicit DuckDB memory cap such as `48GB`. |
| `--temp-dir` | Build default is `.build/duckdb-temp`; `--check` otherwise uses isolated OS temporary space. |
| `--warc-size-concurrency` | Concurrent WARC metadata probes, default `64`. |
| `--http-attempts` | Attempts for transient HTTP failures, default `5`. |
| `--rebuild` | Discard staging state and build a replacement while leaving the current catalog usable. |
| `--check` | Open the completed catalog read-only and run validation and summary queries; temporary spill is allowed. |

There is no `--parts`, `--mode`, `--threshold`, or `--warcs` flag. Technology processing uses a `pages25`
catalog. Primary-page/industry processing uses a separately built `pages1` catalog unless an explicit
equivalence test later proves that reusing rank 1 from `pages25` preserves the intended selection policy.

## 4. Paths and artifact lifecycle

```text
<base>/<crawl>/catalog/pages25/
  catalog.duckdb
  build.lock
  .build/
    state.duckdb
    manifests/
      warc.paths.gz
      cc-index-table.paths.gz
    candidates/
      source_00000.parquet
      source_00001.parquet
      ...
    catalog.duckdb.partial
    duckdb-temp/
```

Rules:

1. Every partial and final rename occurs on the same filesystem.
2. A normal run with a valid matching `catalog.duckdb` logs `reused=true` and exits without network work.
3. An incomplete matching `.build` directory resumes automatically.
4. Conflicting staging identity fails with an instruction to run `--rebuild`.
5. `--rebuild` removes only `.build`; it never removes the current `catalog.duckdb`.
6. Candidate files are deleted only after successful final publication.
7. An invalid existing final catalog is never silently replaced.

Acquire a nonblocking exclusive `flock` before creating, resuming, rebuilding, or publishing. Hold it for
the complete command and fail clearly when another builder owns the same crawl/selection. Acquire the lock
before `--rebuild` removes any staging content. `--check` is read-only and does not take the build lock.

The final database is the only artifact processors require. Copy it to another machine as
`catalog.duckdb.partial`, then rename it locally after the copy completes.

Building requires a local POSIX filesystem that supports `flock`, fsync, and atomic replacement. Build
locally and copy the completed database; do not build inside RustFS, S3, or a shared object-store mount.

Accept crawl IDs only when they match `^CC-MAIN-[0-9]{4}-[0-9]{2}$`. Resolve `--base` and every derived
path before creating or deleting anything, require the catalog target to remain beneath the resolved base,
and allow recursive cleanup only for the exact derived `.build` directory.

## 5. Final DuckDB schema

Each database represents exactly one crawl and selection, so `crawl_id` is stored once in metadata rather
than repeated on hundreds of millions of page rows.

### `catalog_metadata`

```sql
CREATE TABLE catalog_metadata (
    singleton                   BOOLEAN PRIMARY KEY CHECK (singleton),
    schema_version              USMALLINT NOT NULL,
    catalog_id                  VARCHAR NOT NULL,
    crawl_id                    VARCHAR NOT NULL,
    selection_name              VARCHAR NOT NULL,
    pages_per_domain            USMALLINT NOT NULL,
    selection_policy_version    VARCHAR NOT NULL,
    selection_policy_sha256     VARCHAR NOT NULL,
    source_schema_sha256        VARCHAR NOT NULL,
    warc_manifest_sha256        VARCHAR NOT NULL,
    index_manifest_sha256       VARCHAR NOT NULL,
    warc_inventory_sha256       VARCHAR NOT NULL,
    warc_count                  UINTEGER NOT NULL,
    selected_page_count         UBIGINT NOT NULL,
    distinct_domain_count       UBIGINT NOT NULL,
    source_index_shard_count    UINTEGER NOT NULL,
    duckdb_version              VARCHAR NOT NULL,
    builder_version             VARCHAR NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL
);
```

Insert exactly one row with `singleton = true`. `warc_inventory_sha256` hashes the ordered
`(warc_index, warc_filename, object_bytes)` inventory after all probes complete. `catalog_id` is the
SHA-256 of the schema version, crawl, selection settings, selection-policy hash, normalized source-schema
fingerprint, both manifest hashes, and this WARC-inventory hash.
`created_at` is informational and is not part of the deterministic identity.

All hashes are lowercase SHA-256 over frozen canonical bytes. Implement four explicit identity functions:

- selection policy: canonical policy/version/filter/ranking descriptor, never generated SQL with paths;
- source schema: source-index-ordered normalized capability/type descriptors, without source URLs;
- WARC inventory: ordered index, WARC filename, and object size using unambiguous length-delimited fields;
- catalog ID: canonical identity fields and the three hashes above plus both manifest hashes.

Local base paths, HTTP source URLs, candidate filenames, timestamps, and DuckDB temp paths never influence
logical identity.

### `warcs`

```sql
CREATE TABLE warcs (
    warc_index       UINTEGER PRIMARY KEY,
    warc_filename    VARCHAR NOT NULL UNIQUE,
    object_bytes     UBIGINT NOT NULL CHECK (object_bytes > 0)
);
```

There is exactly one row for every original line in `warc.paths.gz`, including WARCs containing no
selected pages.

### `pages`

```sql
CREATE TABLE pages (
    warc_index             UINTEGER NOT NULL,
    root_domain            VARCHAR NOT NULL,
    url                    VARCHAR NOT NULL,
    domain_page_rank       USMALLINT NOT NULL,
    content_languages      VARCHAR,
    warc_record_offset     UBIGINT NOT NULL,
    warc_record_length     UBIGINT NOT NULL
);
```

Create `pages` physically ordered by `(warc_index, warc_record_offset)`. Do not add a large foreign-key
index. Validate the relationship with an anti-join before publication.

Initially rely on the physical order and DuckDB zonemaps for `pages.warc_index` lookup. Benchmark an ART
index separately; add it only if the lookup improvement pays for its build memory and catalog size.

Do not persist these derived values:

```text
selected_pages
selected_bytes
coverage/utilization
whole-versus-range strategy
threshold
processing status
```

They are calculated at runtime by grouping `pages` and joining `warcs`.

## 6. Build-state schema

`.build/state.duckdb` uses explicit typed tables rather than a generic key/value configuration store:

```text
build_identity
  singleton, crawl_id, pages_per_domain, policy_version,
  selection_policy_sha256, source_schema_sha256, warc_manifest_sha256, index_manifest_sha256,
  warc_inventory_sha256 nullable

warc_inventory
  warc_index, warc_filename, object_bytes nullable, attempts, last_error

source_shards
  source_index, source_url, source_schema_sha256, status, candidate_rows, candidate_bytes,
  attempts, last_error, completed_at
```

Only the coordinator writes `state.duckdb`. HTTP worker threads return WARC-size results through a queue;
they never write DuckDB concurrently.

`source_shards.status` is one of `pending`, `running`, or `ready`. On restart, `running` becomes `pending`.
A `ready` candidate is reused only when its file exists, has the expected size, and DuckDB can read its
footer, schema, and stored row count. Otherwise, rebuild that shard alone.

## 7. Fixed WARC inventory and exact sizes

Manifest rules:

- hash the downloaded compressed `warc.paths.gz` bytes with SHA-256;
- preserve original line order exactly and never sort it;
- assign the zero-based line number as `warc_index`;
- reject blank lines, duplicate filenames, and paths outside the requested crawl;
- reject an empty inventory.

For every WARC object:

1. Send `HEAD https://data.commoncrawl.org/<warc_filename>`.
2. Accept a successful response with a positive `Content-Length`.
3. Otherwise send `GET` with `Range: bytes=0-0`.
4. Require HTTP 206 and parse the total size from `Content-Range`.
5. Retry 429, 500, 502, 503, 504, timeouts, connection resets, and unexpected EOF.
6. Honor `Retry-After` and apply exponential backoff with jitter plus a shared cooldown after 429/503.
7. Treat 404 and malformed length metadata as permanent failures.

Commit successful sizes to `state.duckdb` in fixed completion batches. A restart probes only rows whose
`object_bytes` is still null. Catalog publication is forbidden until all inventory rows have positive exact
sizes.

## 8. Exact global page-selection algorithm

### Eligibility filters

Apply the existing behavior:

```text
fetch_status = 200
COALESCE(content_mime_detected, content_mime_type) IN
  ('text/html', 'application/xhtml+xml')
registered domain is nonblank
URL is nonblank
WARC filename is nonblank
WARC offset >= 0
WARC length > 0
```

When `content_languages` or `content_mime_detected` is absent in an older crawl, generate the normalized
expression described above. The output schema must be identical across crawl generations. Make these
decisions from the inspected source columns, never from the crawl date.

### Deterministic ranking

For `pages_per_domain = 1`:

1. apex/`www` before functional subdomains;
2. shallowest path;
3. shortest path;
4. apex before `www`;
5. URL, WARC filename, offset, and length as deterministic ties.

For `pages_per_domain > 1`:

1. apex/`www` before functional subdomains;
2. homepage before non-homepage;
3. multilingual legal/contact/about/privacy/terms paths before ordinary paths;
4. shallowest path;
5. shortest path;
6. apex before `www`;
7. URL, WARC filename, offset, and length as deterministic ties.

Define this ordering once in `selection.py`; local and global ranking must consume the same ordered list of
SQL expressions. Specify `NULLS LAST` explicitly for every nullable ordering term.

### Duplicate capture coordinates

Canonicalize capture coordinates before applying local top N and again after unioning all local candidates.
The coordinate identity is:

```text
(warc_filename, warc_record_offset, warc_record_length)
```

This must happen before ranking: otherwise repeated index rows can occupy several local top-N positions and
incorrectly exclude a unique page.

For rows sharing one coordinate:

- fail if `root_domain`, `url`, or any computed selection-ranking value conflicts;
- choose the lexicographically smallest non-null `content_languages`; use null only when every duplicate is
  null;
- use `source_index` as the last canonicalization tie breaker;
- retain exactly one canonical row.

The global pass repeats coordinate canonicalization because the same capture can appear in more than one
source shard. After this step, URL, WARC filename, offset, and length form a total ranking tie breaker.

### Stage A: resumable local candidates

Discover exact `subset=warc` Parquet paths from the downloaded `cc-index-table.paths.gz`; never use an S3
glob or anonymous LIST. Preserve manifest order and assign an internal `source_index`.

For each source Parquet, run one vectorized DuckDB `COPY`:

1. filter eligible rows;
2. compute named ranking columns;
3. validate and canonicalize duplicate capture coordinates;
4. calculate `row_number()` per `root_domain`;
5. retain local rank `<= N`;
6. write `source_XXXXX.parquet.partial` with ZSTD;
7. close and validate the Parquet;
8. rename it atomically to `source_XXXXX.parquet`;
9. commit `ready` state.

Candidate rows retain the raw output fields plus named ranking columns needed for the global pass:

```text
rank_main_site
rank_homepage
rank_priority_path
rank_path_depth
rank_path_length
rank_apex
source_index
```

The candidate writer explicitly casts every output to the stable candidate schema defined above.

Do not materialize an entire candidate shard through PyArrow or insert its rows from Python.

Retry a source query only when DuckDB reports an HTTP 429/500/502/503/504 or a transient network failure.
After attempts are exhausted, stop the build while preserving every previously completed candidate.

### Stage B: exact global top N

After every candidate is ready, DuckDB reads all candidate Parquets locally, validates and canonicalizes
coordinates across shards, and repeats
`row_number() OVER (PARTITION BY root_domain ORDER BY <the same total ordering>)`. Keep global rank `<= N`
and expose that value as `domain_page_rank`.

This two-stage reduction is exact. A row below local rank N cannot enter global rank N because its own
source shard already contains N better rows for that domain.

Tests must also compare the complete two-stage result against a direct single-pass global top-N query over
randomized multi-shard fixtures; the proof is not accepted only as prose.

Before loading final pages:

- fail if a selected WARC filename is absent from the inventory;
- fail if a filename maps to more than one WARC index;
- fail on duplicate selected `(warc_index, offset, length)` coordinates rather than silently deduplicating;
- cast offsets and lengths to unsigned 64-bit values only after validating their source values.

## 9. Final construction and atomic publication

Build `.build/catalog.duckdb.partial` with bulk SQL:

1. create the final schema;
2. copy the complete, sized WARC inventory into `warcs`;
3. create the globally selected page relation from local candidates;
4. join WARC filenames to indexes;
5. insert `pages` ordered by WARC index and offset;
6. insert the one metadata row;
7. execute all validation queries;
8. re-fetch both crawl manifests and reject publication if either checksum changed;
9. `FORCE CHECKPOINT` and close the database;
10. reopen it read-only and validate metadata and row counts again;
11. fsync the file;
12. atomically `os.replace()` it over `catalog.duckdb`;
13. fsync the parent directory;
14. remove `.build` candidates and spill files.

The current completed catalog remains available throughout `--rebuild`. On Linux, a worker that already
has the old catalog open continues reading its old inode after replacement.

## 10. Validation and `--check`

Publication and `--check` run the same full validation functions:

- exactly one metadata row;
- catalog identity matches its schema and settings;
- WARC indexes are contiguous from `0` to `warc_count - 1`;
- WARC filenames are unique and all object sizes are positive;
- the ordered WARC inventory hash matches `catalog_metadata.warc_inventory_sha256`;
- every page maps to one WARC;
- every offset and length is valid;
- overflow-safe bounds: `offset <= object_bytes`, followed by `length <= object_bytes - offset`;
- no duplicate selected WARC coordinates;
- every domain has ranks starting at 1 with no gaps or duplicates;
- no domain rank exceeds `pages_per_domain`;
- stored counts equal actual counts.

`--check` also reports, without storing the results in the catalog:

```text
catalog size
WARC count and total compressed bytes
selected page and domain counts
selected compressed bytes
WARCs with zero selected pages
coverage percentiles and fixed coverage buckets
cross-WARC domain count and maximum WARCs per domain
```

It does not choose or persist a whole-WARC threshold. Full validation and statistics may spill while
grouping the large `pages` table. Open the catalog itself read-only, create an isolated temporary scratch
directory beneath `--temp-dir` or the operating-system temp directory, and remove that scratch directory
on exit.

A normal reuse check reads only the schema and single metadata row and compares crawl, selection, schema
version, policy version, and selection-policy hash. It does not repeat the full-table validation on every
invocation; operators use `--check` for that explicit audit.

## 11. Logging contract

Emit structured JSON and log failures once at the command boundary. There are no timer-based progress
events.

Completion events:

```text
catalog build started
source manifests ready
WARC inventory ready
WARC size batch ready
candidate shard ready
global selection ready
catalog pages ready
catalog validated
catalog ready
catalog check ready
```

Each applicable event includes identifiers, completed/total units, rows, raw bytes plus a human-readable
binary size, elapsed seconds, rows or objects per second, attempts, retries, HTTP 429/503 counts, reused
work, and DuckDB spill size. Use a maintained human-size library rather than a new formatter.

Candidate logging is one event per source shard. WARC-size logging is one event per committed batch. Do
not add once-per-second status output.

## 12. Package and file layout

```text
cc-warc-index-builder/
  Makefile
  README.md
  pyproject.toml
  uv.lock
  bin/                              generated by `make build`
  warc_index_builder/
    __init__.py
    __main__.py                     CLI, phase orchestration, error boundary
    manifests.py                    exact manifest download, hashing, validation
    selection.py                    one canonical eligibility/ranking policy
    object_sizes.py                 bounded HEAD/range probes and retry behavior
    catalog.py                      state schema, candidate build, final build, validation
    events.py                       structured JSON event emission only
  tests/
    test_manifests.py
    test_selection.py
    test_object_sizes.py
    test_catalog.py
    test_cli.py
```

Production dependencies:

```text
duckdb
httpx
humanize
```

Do not carry PyArrow into the new production package. Test fixtures can be created directly with DuckDB.
The Makefile creates `bin/cc-warc-index-builder` from the locked virtual environment and exposes `build`,
`test`, and `clean` targets.

## 13. Implementation tasks

The broad phases above are divided into 27 dependency-ordered, independently testable tasks in the
[small-task execution checklist](./2026-07-12-cc-warc-index-builder-tasks.md). Each task defines its file
scope, verification, dependency, and intended commit boundary.

Do not combine tasks merely because they touch the same module. The separate boundaries are intentional:
they isolate manifest correctness, schema compatibility, page-selection correctness, recovery behavior,
catalog integrity, and publication safety.

## 14. Integration constraints for later tasks

- `cc-enrich-worker` must open the catalog read-only.
- Technology and primary-page processing must resolve the catalog matching their configured
  `pages_per_domain`; they must not silently substitute `pages25` for `pages1`.
- Runtime selected bytes are `SUM(warc_record_length)` and coverage is selected bytes divided by
  `warcs.object_bytes`.
- A WARC can have zero selected pages.
- A domain can span several WARCs. The catalog deliberately preserves that mapping; it does not make
  current per-domain aggregation safe. The WARC-centric runtime plan's page-additive/domain-reduction gate
  remains mandatory before production cutover.
- Old URL-part worklists and the new WARC catalog must use separate output/`.loaded` namespaces.
- Do not remove `cc-download-worker` or the legacy index-builder until the new catalog and direct-read path
  pass canary validation.

## 15. Rollback and cleanup

The builder is additive during implementation. A failed build affects only `.build`; the existing runtime
continues using its previous worklists and downloader. A failed `--rebuild` leaves the previous completed
catalog untouched.

After catalog and runtime canaries pass:

1. remove `index-builder/` and the embedded downloader worklist builder;
2. remove `cc-download-worker` and WARC analyzer in the later runtime cleanup task;
3. remove obsolete URL-index-part documentation and dependencies;
4. retain migrated selection and recovery tests in the new package.
