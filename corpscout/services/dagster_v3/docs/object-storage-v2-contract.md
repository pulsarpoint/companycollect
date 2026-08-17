# Object storage v2 contract

Status: accepted for pilot implementation
Contract version: 2
Decision date: 2026-08-16
Related inventory: [Object storage layout inventory](object-storage-layout-inventory.md)

## Purpose

The v2 layout makes an explicit catalog the downstream contract. A reader must be able
to locate and verify every object for one logical partition without calling S3 `LIST`.
The layout also replaces entity-per-object datasets with coarser immutable batches while
allowing source files with audit or replay value to remain as original blobs.

This contract defines storage identity, catalog schema, publication order, reader
behavior, idempotency, Dagster metadata, and migration rules. It does not authorize
deleting v1 data.

## Required invariants

1. Production readers never discover partition data by enumerating an S3 prefix.
2. Every logical partition has one deterministic `commit.json` key.
3. `commit.json` is written last and identifies one immutable Parquet catalog by exact
   key, size, and SHA-256 digest.
4. The Parquet catalog contains one row per data object and gives its exact key, size,
   digest, format, and optional logical row count.
5. Compacted data objects are immutable and content-addressed by SHA-256.
6. A failed writer must not replace the existing commit. Unreferenced immutable objects
   are safe to retain until a separate reconciliation process exists.
7. Only one Dagster run may publish a given logical partition at a time.
8. Retention and deletion are separate, explicitly reviewed operations.

## Logical location

A catalog location consists of:

- `source`: lowercase source slug, for example `denmark_cvr`.
- `dataset`: lowercase durable dataset slug, for example `company_details`.
- `partition`: one or more named partition dimensions.

Source, dataset, and dimension names use lowercase letters, numbers, underscores, and
hyphens. Partition values are UTF-8 percent-encoded in object keys. Dimension names are
sorted so callers cannot construct different keys from differently ordered mappings.

All datasets must provide at least one partition dimension. A naturally unpartitioned
snapshot uses a real bounded identity such as `snapshot_date`; it must not use an
unbounded root prefix.

The partition prefix is:

```text
v2/source=<source>/dataset=<dataset>/partition/<dimension>=<value>/...
```

Example:

```text
v2/source=denmark_cvr/dataset=company_details/partition/hash_bucket=0a/year=2026/
```

## Object keys

### Compacted data

```text
<partition-prefix>/objects/sha256=<sha256>.<format>
```

Examples:

```text
.../objects/sha256=8d7c....parquet
.../objects/sha256=91ab....ndjson.gz
```

Parquet is the default for typed entity rows. Compressed NDJSON is used only when exact
JSON replay has durable value. The SHA-256 digest is calculated from the exact uploaded
bytes, not from an uncompressed or logical representation.

Durable source archives, filings, XML/XBRL, PDFs, or XHTML may keep their existing
immutable object keys. A v2 catalog can reference those keys directly after size and
digest verification; migration must not copy them merely to conform to the v2 prefix.

### Immutable catalog

```text
<partition-prefix>/catalogs/run_id=<encoded-run-id>/catalog.parquet
```

The catalog is immutable after publication. A Dagster retry within the same run must
produce the same logical rows; a later materialization writes a new run catalog.

### Partition commit

```text
<partition-prefix>/commit.json
```

This is the one intentionally mutable object. It is a small commit record, not an
object inventory. It points to the exact immutable catalog and is replaced only after
the new data and catalog have been verified.

## Parquet catalog schema

Every catalog contains these columns in this order:

| Column | Type | Nullable | Meaning |
| --- | --- | --- | --- |
| `schema_version` | int32 | no | Always `2` |
| `source` | string | no | Source slug |
| `dataset` | string | no | Durable dataset slug |
| `partition_json` | string | no | Canonical JSON object with sorted partition dimensions |
| `source_run_id` | string | no | Dagster run that published the catalog |
| `created_at` | timestamp UTC | no | Catalog row creation time |
| `object_key` | string | no | Exact S3 key; never a prefix |
| `object_format` | string | no | `parquet`, `ndjson.gz`, or a source-blob format |
| `size_bytes` | int64 | no | Size of the exact stored bytes |
| `sha256` | string | no | Lowercase SHA-256 of the exact stored bytes |
| `row_count` | int64 | yes | Logical records in a batch; null for opaque source blobs |

Rows are sorted by `object_key` before the catalog is written. Source-specific columns
may be appended for real discovery or audit fields such as company ID bounds, filing
dates, source URLs, or upstream payload hashes. Readers must require the core columns
and tolerate additional columns.

The catalog must remain a control-plane object rather than another large dataset. The
target is fewer than 256 data-object rows per logical partition. A design expected to
exceed 1,000 rows must introduce another meaningful partition dimension.

## Commit schema

The canonical JSON commit is represented by `ObjectCatalogCommit` in
`dagster_v3.defs.common.object_catalog`. It contains:

- `schema_version`, always `2`.
- `location`: source, dataset, and partition dimensions.
- `source_run_id` and timezone-aware `created_at`.
- `catalog`: exact key, byte size, SHA-256, and catalog row count.
- `data_object_count`, which must equal the catalog row count.
- `data_size_bytes` and optional `data_row_count` aggregates.

The JSON is UTF-8, has sorted keys and compact separators, and ends with a newline.
Unknown fields are rejected so a producer cannot silently publish an incompatible
shape.

## Batch sizing

For entity rows, writers should target 64-256 MiB per compressed data object. Objects
below 8 MiB are acceptable only for the final batch, a genuinely small partition, or a
durable original source blob. A writer must not flush batches solely by entity count
without also measuring encoded bytes.

Source-specific boundaries still matter. Batches must not mix logical partitions that
need independent retries, retention, or downstream replacement.

## Publication protocol

A writer publishes one partition in this order:

1. Build batches locally or in a bounded streaming process.
2. Calculate SHA-256, size, format, row count, and source-specific audit fields.
3. Upload every immutable data object to its content-addressed key. Reuse an existing
   object only after its stored size and digest metadata match.
4. Build the sorted Parquet catalog and validate its required schema and aggregate
   counts.
5. Upload the catalog to the run-specific immutable catalog key.
6. Read or inspect the stored catalog and verify its size and SHA-256.
7. Write `commit.json` to the deterministic partition key in one S3 `PUT`.
8. Return a Dagster materialization containing the commit identity and aggregates.

The commit is the visibility boundary. If any step before step 7 fails, downstream
readers continue to see the previous valid commit or no committed v2 partition. Writers
must not create an empty commit to hide a failed extraction. A legitimately empty
partition publishes a valid zero-row Parquet catalog and a zero-count commit.

## Reader protocol

A reader:

1. Constructs the exact `commit.json` key from source, dataset, and partition.
2. Reads and validates the commit without listing its parent prefix.
3. Confirms the commit location matches the requested logical partition.
4. Downloads the exact catalog key and verifies its stored size and SHA-256.
5. Validates required columns, schema version, catalog row count, and aggregate counts.
6. Reads only the exact object keys in the catalog, verifying digest or stored checksum
   when the risk of corruption justifies downloading the bytes.

A missing commit means that v2 is not published for the partition. During an explicitly
temporary migration window, a reader may then use its existing v1 path. Other catalog
or validation errors fail the materialization; they must not silently fall back to v1
because that would hide a corrupt v2 publication.

## Runtime listing guardrails

`ObjectStoreResource.list_keys()` protects legacy v1 readers while they are being
migrated. Its initial rollout limits are:

| Resource setting | Default | Behavior |
| --- | ---: | --- |
| `list_max_keys` | 50,000 | Fail instead of returning a larger in-memory key list |
| `list_max_pages` | 50 | Stop before requesting another page when S3 reports more data |
| `list_max_elapsed_seconds` | 60 | Fail after the current page if the listing budget is exhausted |
| `list_warn_seconds` | 2 | Emit slow-list telemetry |
| `s3_connect_timeout_seconds` | 5 | Bound each S3 connection attempt |
| `s3_read_timeout_seconds` | 30 | Bound each S3 response wait |
| `s3_max_attempts` | 2 | Limit total attempts, including the initial request |

An empty or whitespace-only prefix is rejected before an S3 request. A guardrail
failure raises `ObjectStoreListingLimitError` with bucket, prefix, page count, observed
key count, elapsed seconds, and the exceeded limit.

Slow and rejected listings log the same dimensions as structured log-record fields:
`object_store_bucket`, `object_store_prefix`, `object_store_page_count`,
`object_store_key_count`, `object_store_elapsed_seconds`, and
`object_store_limit_exceeded`. Source-specific limits can be tightened on each
configured resource after one normal schedule cycle establishes its bounded baseline.

## Dagster contract

The publishing asset returns at least this materialization metadata:

- `object_catalog_schema_version`
- `object_catalog_bucket`
- `object_catalog_commit_key`
- `object_catalog_key`
- `object_catalog_sha256`
- `data_object_count`
- `data_size_bytes`
- `data_row_count` when known
- `source_run_id`

Downstream assets use normal Dagster asset dependencies. They locate the object catalog
from their deterministic source/dataset/partition contract, not by polling S3. When a
workflow needs imperative cross-job coordination, an asset sensor reacts to the upstream
materialization event; a sensor must not scan a bucket every evaluation.

Publication for the same logical partition must be serialized with the existing source
pool or a source-specific Dagster concurrency key. The contract deliberately does not
implement last-writer conflict resolution in object storage.

## Idempotency and recovery

- Data objects are content-addressed, so retries reuse identical bytes.
- Catalogs are immutable and scoped by run ID.
- The deterministic commit is written last, making an incomplete run invisible.
- Retrying the same Dagster run is safe when it produces the same catalog content.
- Re-materializing in a new run creates a new catalog and atomically advances the commit.
- Orphan data/catalog objects from failed runs are not deleted in a task failure path.
- A future retention asset may remove unreferenced objects only after a bounded catalog-
  based grace-period analysis; it may not discover candidates with a root listing.

## Migration sequence

For each source:

1. Add a v2 publisher without changing the existing v1 recovery path.
2. Backfill or catalog one bounded partition.
3. Shadow-read v1 and v2 and compare entity counts, identifiers, hashes, and source-
   specific invariants.
4. Publish parity results as Dagster materialization metadata and tests.
5. Switch the normal reader to v2 with temporary missing-commit fallback to v1.
6. Remove v1 listing from the hot path after the agreed observation window.
7. Make any v1 retention or deletion decision separately.

Sweden company is the direct-key/catalog pilot. Denmark is the first compacted-batch
pilot. Norway BRREG follows after the Denmark pattern proves parity and recovery.

## Phase 2 acceptance checks

- Deterministic keys are independent of partition-map insertion order.
- Unsafe storage names, blank partition values, and invalid SHA-256 digests are rejected.
- Commit JSON round-trips canonically and rejects unknown fields.
- A commit cannot reference a catalog for another run or partition.
- Commit object count equals catalog row count.
- The contract code performs no S3 listing and no deletion.
- Existing writers and readers remain unchanged until their source-specific pilot.
