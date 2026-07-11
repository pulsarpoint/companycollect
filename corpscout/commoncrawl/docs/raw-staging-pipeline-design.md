# Common Crawl raw staging pipeline

Status: agreed for implementation. Phases 1-3 are implemented: shared raw contracts and WARC handling live
in the sibling `cc-raw` module, `cc-download-worker` builds worklists and stages requested part ranges as
bounded, resumable RustFS packs, and `cc-enrich-worker` remains an independent processor service. Storage
watermarks remain future work.

This document defines how Common Crawl network reads are separated from enrichment compute. A downloader
retrieves the selected WARC records once, stores bounded raw packs in the local RustFS object store, and
independent processors consume those packs. Processor and load completion are recorded separately so the
same raw data can be reused by technology, industry, embedding, and future passes before an operator
explicitly reclaims it.

The design keeps the established page-selection policy and final ClickHouse tables. It inserts a durable,
bounded staging boundary between them.

## 1. Why split download from processing

The current `cc-enrich-worker` fetches one Common Crawl S3 range for each selected page and immediately
processes the response. That couples two resources with different failure and scaling behavior:

- Common Crawl reads have roughly 270-280 ms request latency and can experience timeouts or transient
  failures.
- Technology detection is CPU-bound on newer crawls and already reaches roughly 95-100% CPU at concurrency
  32. Increasing fetch concurrency cannot improve throughput once processing is the bottleneck.
- Network stalls currently interrupt the CPU work queue, while slow CPU processing leaves no independently
  managed download queue.
- Technology, industry, and embedding passes may need the same selected records, causing repeated reads from
  Common Crawl.

The split is primarily an isolation, reuse, and operability improvement. It does not make CPU-bound analysis
itself faster. Downloader and processor instances can scale independently and can run on different machines;
RustFS and processors are connected over the local network.

## 2. Goals and non-goals

### Goals

- Download each selected WARC record once per worklist selection and make it reusable by multiple processors.
- Keep Common Crawl credentials, retry behavior, and remote-read metrics in one downloader service.
- Feed processors with large sequential reads over the LAN rather than one object-store request per page.
- Bound RustFS usage with backpressure; do not retain a permanent copy of the selected crawl.
- Make download, processing, final loading, active ownership, and reclamation visible from object-store state.
- Resume safely after downloader, processor, loader, or host failures.
- Reclaim raw data only through an explicit operator command after inspecting all required processor states.

### Non-goals

- Mirroring complete WARC files or an entire Common Crawl snapshot.
- Automatically deciding which processor types are required for a crawl.
- Automatically deleting raw packs after one processor finishes or loads.
- Changing the established page-ranking policy, output Parquet contract, or ClickHouse schema.
- Providing high availability for the existing single-node RustFS deployment. Staged data is reproducible
  from Common Crawl, so checksums and regeneration are the recovery mechanism.

## 3. Terms and units of work

Use these terms consistently; Common Crawl itself also uses the word "segment", so the pipeline should not
use that word for several different units.

| Term | Meaning |
|---|---|
| crawl | One Common Crawl snapshot, such as `CC-MAIN-2026-25`. |
| selection | The processor-neutral worklist policy and limit, such as `pages25`. It prevents incompatible worklists from sharing raw state. |
| part | One URL-index/worklist partition, currently numbered approximately 0-299. It is the initial processing and reclamation unit. |
| chunk | A bounded consecutive subset of a part. One chunk produces one raw pack, index, and manifest. |
| source WARC | A Common Crawl WARC file named by a worklist row. |
| pack | Concatenated selected compressed WARC records for one chunk. It is not a complete source WARC. |
| processor | A named consumer of raw packs, for example `tech`, `industry`, or `embedding`. |

The worklist ordinal is the stable ordering within a part. Chunk boundaries may change only when the
selection or worklist identity changes.

## 4. Components and ownership

```text
Common Crawl S3
       |
       | selected WARC range reads
       v
cc-download-worker ------> RustFS raw packs + download state
                                  |
                                  | sequential pack reads over LAN
                 +----------------+----------------+
                 |                |                |
                 v                v                v
          tech processor   industry processor   embedding processor
                 |                |                |
                 +----------------+----------------+
                                  |
                                  v
                         output Parquet / vectors
                                  |
                                  v
                              ClickHouse

cc-rawctl <------ reads all RustFS state; reports status and performs explicit reclamation
```

### `cc-download-worker`

The downloader owns:

- Common Crawl S3 access and credentials;
- materializing and caching deterministic per-part worklists from the Common Crawl URL index;
- range-request concurrency, timeouts, retry policy, and S3 metrics;
- reading a deterministic part worklist;
- writing `records.pack`, `index.parquet`, and `manifest.json` to RustFS;
- download progress and ready state;
- storage high/low-watermark backpressure.

It does not parse page content, detect technologies, embed text, write processor output, load ClickHouse, or
delete staged raw data.

### `cc-enrich-worker`

The worker uses staged input exclusively. It:

- enumerates committed chunk manifests for a part;
- verifies that each manifest matches the requested crawl, selection, part, and worklist identity;
- downloads each pack once to a temporary local cache and uses `index.parquet` to locate individual records;
- parses the original WARC records and runs the selected processor;
- produces the existing output artifacts and loads them through the existing explicit load stage; and
- leaves raw RustFS objects untouched.

Direct Common Crawl input has been removed from `cc-enrich-worker`; it needs no Common Crawl credentials.
The current orchestrator deliberately retains local `out_<mode>_<part>.loaded` files as its completion
authority. Remote processor heartbeats and completion markers remain data contracts for possible future
operations tooling, but are not consulted or written by `cc-crawl`/`cc-enrich-worker`.

### `cc-rawctl`

This is a small operational command. It reads manifests and state markers directly from RustFS and:

- reports which parts are downloaded, active, processed, loaded, failed, or reclaimable;
- identifies the run and host currently processing a part;
- explains why a part is not reclaimable;
- previews or executes explicit raw-data reclamation.

It does not infer which processors the business requires. Required processors are supplied by the operator
when determining reclaimability.

### Shared code

A small concrete package may be shared by these commands for the stable RustFS object contract, manifest
validation, marker validation, and deterministic key construction. Keep Common Crawl downloading and
processor-specific behavior in their owning commands. Do not introduce a generic service layer or an
interface unless a second real implementation requires it.

Repository layout:

```text
cc-raw/
  fetch/
  rawstore/
  rawstate/
cc-download-worker/
  cmd/cc-download-worker/
  internal/rawdownload/
  internal/worklistbuilder/
cc-enrich-worker/
  cmd/cc-enrich-worker/
cc-rawctl/                       # future sibling command
```

`cc-download-worker` and `cc-enrich-worker` have independent Go modules, binaries, containers, and runtime
configuration. Neither application imports the other. They share only the concrete `cc-raw` protocol module.

## 5. RustFS object layout

Raw data and state use separate prefixes. State must survive deletion of the raw objects.

```text
commoncrawl/raw/
  crawl=CC-MAIN-2026-25/
    selection=pages25/
      part=000/
        chunk=000042/
          records.pack
          index.parquet
          manifest.json

commoncrawl/state/
  crawl=CC-MAIN-2026-25/
    selection=pages25/
      part=000/
        download/ready.json
        processor=tech/processing.json
        processor=tech/processed.json
        processor=tech/loaded.json
        processor=industry/processing.json
        processor=industry/processed.json
        processor=industry/loaded.json
        processor=embedding/processing.json
        processor=embedding/processed.json
        reclaimed.json
```

There is no path-level `version=v1` directory initially. Each persisted document contains an explicit
`schema_version`. If an incompatible schema is introduced later, readers can support both schemas or a
new prefix can be introduced as part of a deliberate migration.

`selection` identifies the exact processor-neutral worklist policy. For example, `pages25` means the
homepage/legal/contact ranking policy with a maximum of 25 pages per domain. Its exact worklist file and
checksum are recorded in every chunk manifest.

## 6. Raw chunk commit contract

One committed chunk consists of exactly three objects.

### `records.pack`

`records.pack` is the byte-for-byte concatenation of the selected compressed WARC gzip members returned by
Common Crawl range requests. The downloader does not decompress and recompress records. This preserves the
source representation and moves WARC parsing to the processor.

It is intentionally neither one RustFS object per page nor a copy of a complete source WARC. Packs should
normally target roughly 256 MiB to 1 GiB, bounded by both record count and bytes. Oversized individual
records are allowed to create an oversized pack rather than being split.

### `index.parquet`

The index has one row for every requested worklist record, including download failures:

| Column | Meaning |
|---|---|
| `worklist_ordinal` | Stable ordinal within the part. |
| `domain_rank` | Original domain ordering/rank used by the worklist. |
| `root_domain` | Registered domain. |
| `url` | Selected page URL. |
| `is_primary` | Whether the worklist considers this the primary domain page. |
| `content_languages` | Worklist languages when available. |
| `warc_filename` | Common Crawl source WARC key. |
| `warc_offset` | Source range offset. |
| `warc_length` | Source range length. |
| `download_status` | `downloaded`, `not_found`, or `failed`. |
| `download_attempts` | Number of downloader attempts; the selected transport may perform internal HTTP retries. |
| `pack_offset` | Start byte in `records.pack`; null on failure. |
| `pack_length` | Record length in `records.pack`; null on failure. |
| `record_checksum` | Optional checksum of the stored record bytes. |
| `error_code` | Stable low-cardinality failure code; null when downloaded. |

Failure codes distinguish conditions such as `timeout`, `not_found`, `throttled`, `access_denied`,
`short_read`, `unexpected_eof`, `connection_reset`, `connection_refused`, and `network_unreachable`.

Invariants:

- `downloaded` requires non-null `pack_offset` and `pack_length` and a null `error_code`.
- `not_found` and `failed` require null pack coordinates and a non-null `error_code`.
- Pack ranges are contiguous in worklist order and cover the pack size declared by the manifest exactly.
- Rows are ordered by `worklist_ordinal`.
- A processor skips failed rows but includes their counts in its completion state.

### `manifest.json`

The manifest summarizes the chunk, pins the input identity, and acts as the commit marker. It contains no
processor state, ClickHouse state, credentials, or secrets.

```json
{
  "schema_version": 1,
  "crawl_id": "CC-MAIN-2026-25",
  "selection": "pages25",
  "part": 0,
  "chunk": 42,
  "worklist": {
    "key": "crawl/shard_tech_0.parquet",
    "sha256": "...",
    "first_ordinal": 1048576,
    "record_count": 16384
  },
  "pack": {
    "key": "commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000042/records.pack",
    "size_bytes": 402653184,
    "sha256": "..."
  },
  "index": {
    "key": "commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000042/index.parquet",
    "size_bytes": 1572864,
    "sha256": "..."
  },
  "results": {
    "requested_records": 16384,
    "downloaded_records": 16380,
    "failed_records": 4,
    "source_bytes": 402653184,
    "packed_bytes": 402653184,
    "errors": {
      "not_found": 0,
      "timeout": 3,
      "other": 1
    },
    "failure_reasons": {
      "timeout": 3,
      "connection_reset": 1
    }
  },
  "download": {
    "run_id": "01J...",
    "worker_host": "commoncrawl-download-1",
    "git_commit": "...",
    "started_at": "2026-07-11T17:00:00Z",
    "completed_at": "2026-07-11T17:02:31Z"
  }
}
```

The downloader writes the pack and index first, verifies their sizes and checksums, and writes the manifest
last. Readers treat a chunk without a valid manifest as incomplete. Orphaned pack/index objects may be
garbage-collected after an age threshold, but are never eligible for processing.

The downloader may commit a manifest with a small number of terminal record failures. Retryable failures are
retried up to `--record-attempts` times, with an independent `--record-timeout` for each logical attempt;
permanent `not_found` failures are not retried. A part-level `download/ready.json` is written only after every expected
chunk has a valid manifest and the union of manifest ordinal ranges exactly covers the worklist.

`ready.json` is the compact inventory used by processors and `cc-rawctl`; it avoids relying on eventually
consistent prefix listing as the source of truth:

```json
{
  "schema_version": 1,
  "crawl_id": "CC-MAIN-2026-25",
  "selection": "pages25",
  "part": 0,
  "worklist": {
    "key": "crawl/shard_tech_0.parquet",
    "size_bytes": 512,
    "sha256": "...",
    "record_count": 4
  },
  "chunks": [
    {
      "chunk": 0,
      "manifest_key": "commoncrawl/raw/crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/manifest.json",
      "manifest_sha256": "...",
      "first_ordinal": 0,
      "record_count": 4,
      "raw_bytes": 2730
    }
  ],
  "totals": {
    "chunk_count": 1,
    "requested_records": 4,
    "downloaded_records": 3,
    "failed_records": 1,
    "raw_bytes": 2730
  },
  "download_run_id": "01J...",
  "completed_at": "2026-07-11T17:55:00Z"
}
```

Chunk entries are sorted by `chunk`, and their ordinal ranges must be contiguous and non-overlapping.
`raw_bytes` is the combined size of the pack, index, and manifest objects. Before publishing `ready.json`,
the downloader validates its worklist identity, ranges, record totals, object sizes, and manifest checksums
against the actual committed chunk manifests. The ready marker itself is uploaded atomically after
validation. Its SHA-256 becomes the part input identity for all processor markers.

## 7. Processor and loader state

Raw manifests are immutable input facts. Mutable execution state and atomically published completion
snapshots live under the separate state prefix, once per processor.

### Active processing lease

`processing.json` is a mutable heartbeat, not evidence of completion. It contains at least:

```json
{
  "schema_version": 1,
  "crawl_id": "CC-MAIN-2026-25",
  "selection": "pages25",
  "part": 0,
  "processor": "tech",
  "processing_version": "fast-v2",
  "git_commit": "...",
  "run_id": "01J...",
  "worker_host": "enrich-tech-2",
  "pid": 18422,
  "input_ready_sha256": "...",
  "started_at": "2026-07-11T18:00:00Z",
  "heartbeat_at": "2026-07-11T18:07:00Z",
  "lease_expires_at": "2026-07-11T18:12:00Z"
}
```

Only the current run refreshes or removes its heartbeat. A heartbeat past `lease_expires_at` is stale and
may be replaced by a later run. The initial scheduler assigns disjoint part ranges to processor machines;
the heartbeat is operational visibility and crash detection, not a transactional distributed lock. Dynamic
multi-worker claiming requires a storage operation with compare-and-set semantics or a separate coordinator
before it is enabled.

### Processed completion

`processed.json` is written only after all processor output artifacts for the part are durably committed. It
contains at least:

- `schema_version`, `crawl_id`, `selection`, `part`, `processor`, `processing_version`, and `git_commit`;
- `run_id`, `worker_host`, `started_at`, and `completed_at`;
- the checksum of `download/ready.json` and the checksums of all input chunk manifests;
- input, downloaded, failed, processed, and skipped record counts;
- output names, locations, sizes, row counts, and checksums.

Output artifacts may remain in the existing processor-local output directory when the loader runs on the
same machine. In that case the marker records the absolute path and owning host. An artifact that is itself
the final durable product, such as retained embeddings, must be stored in durable shared/object storage
rather than only on an ephemeral processor disk.

This marker ties processor output to the exact raw input. Changing the worklist, a chunk manifest, or the
processing version requires a new run and must not silently reuse an incompatible marker. A later explicit
reprocessing run publishes a new `processed.json` atomically only after its artifacts are complete. Any
existing `loaded.json` then becomes stale because its processed-marker checksum no longer matches; it does
not count as loaded until the new output is loaded.

### Loaded completion

`loaded.json` is written only after the corresponding output is durably committed to its final destination.
For technology and industry this means the ClickHouse load command completed successfully. It contains:

- `schema_version`, `crawl_id`, `selection`, `part`, `processor`, `processing_version`, `git_commit`, and
  `source_run_id`;
- the exact `processed.json` checksum;
- loader run/host identity and start/completion timestamps;
- destination identity and committed row/object counts.

Embedding may initially stop at `processed.json` when its durable vector artifacts are the final output. If
embedding later has a separate index or database destination, it should write its own final marker using the
same completion model.

Completion markers are never written merely because records were processed in memory. The state sequence is:

```text
download manifests -> download ready -> processing heartbeat -> processed -> loaded
```

A processor crash leaves the raw input intact. A loader crash leaves `processed.json` intact and no
`loaded.json`, allowing the load to be retried without downloading or processing again.

## 8. Status and manual reclamation

Initial commands:

```text
cc-rawctl status --crawl CC-MAIN-2026-25 --selection pages25
cc-rawctl status --crawl CC-MAIN-2026-25 --selection pages25 --part 0
cc-rawctl list --crawl CC-MAIN-2026-25 --selection pages25 --state processing
cc-rawctl list --crawl CC-MAIN-2026-25 --selection pages25 --state reclaimable \
  --require-loaded tech,industry --require-processed embedding
cc-rawctl remove --crawl CC-MAIN-2026-25 --selection pages25 --part 0 \
  --require-loaded tech,industry --require-processed embedding
cc-rawctl remove --crawl CC-MAIN-2026-25 --selection pages25 --part 0 \
  --require-loaded tech,industry --require-processed embedding --execute
```

The status view should show at least:

| Part | Download | Raw bytes | Active owner | Tech | Industry | Embedding | Reclaimable |
|---:|---|---:|---|---|---|---|---|
| 0 | ready | 16.1 GiB | `tech@enrich-tech-2` | processing | loaded | processed | no: active lease |

`remove` is a dry run by default. `--execute` is required to mutate RustFS. Before deletion it must:

1. validate `download/ready.json` and every referenced chunk manifest;
2. reject any non-stale `processing.json` for any processor;
3. validate the checksums and input identity of every required `processed.json` or `loaded.json`;
4. print the exact objects and total bytes to be removed;
5. delete only the raw pack, index, and manifest objects for the requested part;
6. write `reclaimed.json` under the state prefix after successful deletion.

`reclaimed.json` records the input identity, deleted object count and bytes, operator/run identity, and
completion timestamp. It remains after the raw prefix is gone. The downloader must not recreate reclaimed
data during a normal resume; explicit `--force-redownload` is required.

Deletion is idempotent. If the command stops after deleting only some objects, no `reclaimed.json` is
written. A rerun uses the still-present `ready.json` inventory, accepts already-missing planned objects,
validates every surviving object, finishes the same deletion set, and then writes `reclaimed.json`. Status
reports this condition as an incomplete reclamation rather than as a corrupt ready part.

Reclamation is part-wide initially because current processor output and ClickHouse completion are committed
per worklist part. Chunk-level reclamation would require chunk-level processor and loader commits first.

No enhancer automatically deletes raw packs. The operator decides which processor set is complete for the
current use case and supplies those requirements to `cc-rawctl`.

## 9. Backpressure and capacity

A sampled complete `CC-MAIN-2026-25` URL-index part contained 708,218 selected pages and approximately
16.1 GiB of selected compressed WARC records, averaging about 24 KiB per record. Retaining the same selection
for a full crawl is therefore roughly 4-5 TiB. RustFS is a bounded staging queue, not a permanent 4-5 TiB
mirror.

The downloader accepts configured byte watermarks over committed, unreclaimed raw data:

- below the low watermark, it fills the queue normally;
- between low and high watermarks, it finishes in-flight chunks but starts no unnecessary new work;
- at or above the high watermark, it stops scheduling downloads and reports that it is waiting for operator
  reclamation;
- after reclamation takes usage below the low watermark, it resumes.

Watermarks are based on manifest-declared committed bytes plus known orphan bytes, not filesystem free space
alone. RustFS capacity should still reserve room for multipart uploads, orphan cleanup, and service operation.

Processors stream complete packs sequentially and use pack offsets locally. They should not translate each
index row into a separate RustFS range GET. A 1 Gbit/s LAN can become a shared bottleneck with several
processor machines; deployment metrics must expose RustFS read MiB/s, processor input wait time, and CPU
utilization. A 10 Gbit/s LAN provides substantially more headroom but does not remove the need for metrics.

## 10. Failure and restart invariants

- Object existence alone is not readiness: a valid manifest commits a chunk and `ready.json` commits a part.
- Deterministic keys plus worklist checksums let the downloader skip an exact already-ready chunk.
- A pack or index with no manifest is ignored and is safe to remove after the orphan retention period.
- A checksum mismatch makes a chunk corrupt, never ready. Because raw data is derived, it can be regenerated.
- Processor output is not complete until `processed.json` references its durable artifacts.
- Final loading is not complete until `loaded.json` references the exact processed marker.
- Stale processing heartbeats are visible but do not permanently block a retry.
- Reclaimed state survives raw deletion and prevents accidental re-download.
- Lower-level storage and parsing code wraps errors with crawl/selection/part/chunk context; command
  boundaries log each failure once with `log/slog`. Logs and markers never contain credentials or secrets.

## 11. Observability

Downloader metrics should include:

- Common Crawl GET calls, HTTP attempts, retries, status codes, timeouts, and header/body latency;
- downloaded source bytes, committed pack bytes, records by status, and effective MiB/s;
- current committed raw bytes, ready parts/chunks, in-flight chunks, orphan bytes, and watermark state.

Processor metrics should include:

- RustFS requests and bytes, pack-read MiB/s, input wait time, checksum failures, and parse failures;
- records/pages processed, processor latency, CPU utilization, output rows/bytes, and active part;
- heartbeat age and time spent waiting for input.

All run and completion logs carry crawl, selection, part, processor, run ID, and worker host. Errors are
wrapped through lower layers and logged once at the command/worker boundary.

## 12. Migration plan

1. **Implemented:** concrete Go structs and validation tests for manifest, index, ready, processing,
   processed, loaded, and reclaimed documents, with golden contract fixtures.
2. **Implemented:** split the existing fetch path into byte-range retrieval and raw WARC parsing without
   changing direct-mode behavior.
3. **Implemented:** add `cc-download-worker`, embed deterministic worklist generation, accept part ranges,
   and produce staged objects in RustFS.
4. **Implemented:** switch `cc-enrich-worker` to verified staged input while retaining the existing output
   Parquet and explicit load formats.
5. Run one representative previously processed part against staged input and compare domain/page counts and
   normalized output rows. Explain downloader failures rather than masking them in the comparison.
6. **Operational decision:** retain the existing machine-local `.loaded` marker as the process/load gate;
   do not add remote processing ownership to the worker.
7. Add `cc-rawctl status`, dry-run reclamation, validation, and executed reclamation.
8. **Implemented:** keep `cc-crawl` processing and loading behavior unchanged while replacing only its
   worker input with RustFS part coordinates.
9. Deploy downloader and processor machines independently, set conservative storage watermarks, and measure
   RustFS/LAN throughput before increasing processor count.

## 13. Acceptance checks

- A downloader restart does not duplicate a committed chunk and does recover an incomplete chunk.
- A committed pack checksum and every downloaded index range validate.
- Staged input produces equivalent normalized output to an earlier direct run for the same selection and
  processor revision.
- A processor restart can resume without reading Common Crawl.
- A load failure never creates the local `.loaded` marker; rerunning the part retries produce and load.
- Technology and industry local `.loaded` markers coexist for the same raw part without overwriting each other.
- `cc-rawctl` reports active host/run ownership and explains every non-reclaimable part.
- Reclamation defaults to dry run, rejects active processing, rejects missing required completion, and leaves
  an auditable `reclaimed.json` only after successful deletion.
- A normal downloader resume does not recreate a reclaimed part.
- Storage at the high watermark stops new downloads without disturbing active processors.

## 14. Decisions deliberately left configurable

These values affect deployment tuning, not the storage contract:

- target pack size and maximum records per chunk;
- downloader range concurrency, retry count, and request timeouts;
- RustFS low/high byte watermarks and orphan retention period;
- heartbeat frequency and lease duration;
- the required processor set passed to each reclamation operation;
- whether embedding's durable artifact is considered `processed` final or gains a later `loaded`/`indexed`
  marker.

They should be explicit command flags with conservative defaults and recorded in run logs. They do not belong
in a generic configuration abstraction.
