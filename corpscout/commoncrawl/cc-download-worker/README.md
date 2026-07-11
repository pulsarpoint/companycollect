# cc-download-worker

Independent Common Crawl download service. One command builds or reuses the selected URL-index
worklists, downloads only those compressed WARC records, and commits bounded reusable packs to RustFS.
Downloading and enrichment can therefore run on different machines and scale independently.

Range-strategy experiments are isolated in the separate
[`cc-warc-analyzer`](cmd/cc-warc-analyzer/README.md) binary. The production downloader continues to use
one exact Common Crawl range request per selected record until analyzer results justify a planner policy.

The service never parses HTML, performs enrichment, writes ClickHouse, or deletes raw packs. Shared WARC
and object contracts live in [`../cc-raw`](../cc-raw/).

## Data flow

```text
Common Crawl URL index -> cached worklist.parquet -> Common Crawl range GETs
                                                     |
                                                     +-> records.pack + index.parquet
                                                           |
                                                           +-> manifest.json (chunk commit)
all committed chunk manifests --------------------------------> download/ready.json (part commit)
```

`records.pack` contains the original compressed WARC gzip members in worklist order. It is not a complete
source WARC and does not contain unrelated Common Crawl pages.

## Build and test

From `commoncrawl/`:

```bash
make download                       # cc-download-worker/bin/cc-download-worker
make -C cc-download-worker test
make -C cc-download-worker vet
make -C cc-download-worker arm      # linux/arm64
```

The binary runs the embedded worklist builder with Python and DuckDB. For a bare-metal installation,
provide `python3` with `duckdb==1.5.4`; set `CC_DOWNLOAD_PYTHON` when the interpreter is elsewhere. The
container includes both dependencies:

```bash
docker build -f cc-download-worker/Dockerfile -t cc-download-worker .
```

Use `commoncrawl/` as the Docker build context because `cc-raw` is a sibling module.

## Run

```bash
./bin/cc-download-worker \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --parts 0-10
```

`--pages-per-domain` controls both selection and its durable identity:

- `1` selects one best representative homepage per domain and stores data under `selection=pages1`.
- Values greater than `1` use the existing homepage → legal/contact/about → shallow-page ranking, capped
  at that number. The default is `25`, stored under `selection=pages25`.

`--parts` accepts one zero-based URL-index part (`7`) or an inclusive range (`0-10`). Parts in a range run
sequentially, while WARC records within each part download concurrently.

The worklist is no longer a public input. A missing worklist is generated automatically and a valid cached
one is reused:

```text
<base>/<crawl>/download/worklists/pagesN/part_NNN.parquet
```

Use `--worklist-dir` only to move that cache; keep an override dedicated to one crawl/selection. Use
`--rebuild-worklists` to replace valid cached shards.
There are no `--worklist`, `--worklist-key`, `--selection`, or singular `--part` flags.

The standalone `index-builder` remains only as a diagnostic/legacy CLI. `cc-crawl` consumes the ready
RustFS parts produced by this service and does not invoke it.

### Flags

| Flag | Default | Meaning |
|---|---:|---|
| `--base` | `OUT_BASE_DIR`, else `data` | Data root containing crawl worklist caches. |
| `--crawl` | required | Crawl identity such as `CC-MAIN-2026-25`. |
| `--pages-per-domain` | `25` | Page limit and selection policy; `1` is the representative homepage. |
| `--parts` | required | One part or inclusive range, such as `0` or `0-10`. |
| `--worklist-dir` | derived | Override the generated-worklist cache directory. |
| `--rebuild-worklists` | false | Replace otherwise valid cached worklists. |
| `--concurrency` | `64` | Concurrent selected-record downloads. |
| `--max-pack-bytes` | `256 MiB` | Target maximum advertised WARC bytes per pack. An oversized record stays whole. |
| `--max-records` | `16384` | Maximum worklist rows per pack. |
| `--max-failure-rate` | `0.01` | Terminal failures allowed per committed chunk, rounded up to a whole record. |
| `--record-attempts` | `3` | Logical attempts for transient record failures; `not_found` is never retried. |
| `--record-timeout` | `30s` | Deadline for each logical attempt to download one selected WARC record. |
| `--source-anonymous` | false | Use anonymous Common Crawl HTTPS instead of signed S3. |
| `--temp-dir` | system temp | Parent directory for local pack/index construction. |
| `--rustfs-endpoint` | environment | Override `CORPSCOUT_S3_ENDPOINT`. |
| `--rustfs-bucket` | environment | Override `CORPSCOUT_S3_BUCKET`. |
| `--force-redownload` | false | Recreate parts carrying `reclaimed.json`. |

## Configuration

Common Crawl and RustFS intentionally use separate credential sources:

| Variable | Purpose |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Signed Common Crawl S3 access. EC2 instance-role credentials also work. |
| `CORPSCOUT_S3_ENDPOINT` | RustFS S3-compatible endpoint on the local network. |
| `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY` | RustFS credentials. They are environment-only and cannot be passed as flags. |
| `CORPSCOUT_S3_BUCKET` | RustFS bucket containing `commoncrawl/raw` and `commoncrawl/state`. |
| `CC_BASE_URL` | Optional anonymous Common Crawl HTTPS endpoint override, used with `--source-anonymous`. |
| `CC_DOWNLOAD_PYTHON` | Python executable used by the embedded worklist builder; default `python3`. |

## Chunk statistics and throttling

The worker emits one `chunk ready` statistic after a chunk has been downloaded, uploaded to RustFS, and
verified. It does not emit timer-based progress statistics. The existing chunk fields include record
counts, failure reasons, raw bytes, elapsed time, records/s, and MiB/s. The same event also reports:

| Field | Meaning |
|---|---|
| `chunks_ready`, `chunks_total` | Committed chunks and total planned chunks for the part. |
| `raw_size` | Combined committed pack, Parquet index, and manifest size for this chunk. |
| `http_attempts` | Actual S3 HTTP attempts for this chunk. |
| `requests_per_second` | Actual S3 HTTP attempts divided by the chunk elapsed time, including SDK retries. |
| `sdk_retry_attempts` | HTTP attempts performed internally by the AWS SDK beyond `GetObject` calls. |
| `http_429`, `http_503` | Throttling responses observed for this chunk, including recovered retries. |
| `body_read_errors`, `body_read_retries` | Interrupted response-body reads and their retries. |

`raw_size` can be slightly higher than the 256 MiB pack target because it also contains
`index.parquet` and `manifest.json`.

Signed S3 uses the AWS SDK adaptive retry limiter. It observes 429/503 responses immediately, reduces the
request rate through its shared token bucket, and ramps up again when the source recovers. If throttling
still escapes the SDK after its retry budget, the downloader applies a shared exponential cooldown
(1, 2, 4, 8, then 16 seconds) before new logical attempts. This prevents 64 workers from retrying as one
new request wave. The startup log exposes `source_rate_control=aws_adaptive` so the active policy is clear.

## RustFS layout

```text
commoncrawl/raw/
  crawl=CC-MAIN-2026-25/selection=pages25/part=000/chunk=000000/
    records.pack
    index.parquet
    manifest.json

commoncrawl/state/
  crawl=CC-MAIN-2026-25/selection=pages25/part=000/
    download/ready.json
```

For every chunk, publication order is `records.pack` → `index.parquet` → `manifest.json`. The part-level
`ready.json` is uploaded only after all chunk manifests and object checksums validate.

## Resume and failure behavior

- A valid cached worklist is reused; a corrupt or incomplete one is rebuilt atomically.
- A fully valid ready part performs no Common Crawl downloads or RustFS uploads on rerun.
- A valid committed chunk is reused when rebuilding a missing or invalid ready marker.
- A chunk with missing objects, wrong sizes, or mismatched checksum metadata is downloaded and replaced.
- Pack/index objects without a manifest are incomplete and are overwritten on retry.
- A chunk exceeding `--max-failure-rate` is not committed.
- A part carrying `reclaimed.json` is not recreated without `--force-redownload`.
- The worker never removes successfully staged raw objects.

The complete storage and marker contract is documented in
[`../docs/raw-staging-pipeline-design.md`](../docs/raw-staging-pipeline-design.md).
