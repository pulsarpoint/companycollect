# cc-enrich-worker

The Common Crawl processor. It reads a completed raw part from local-network RustFS, parses the selected
WARC records, runs industry or technology enrichment, writes local Parquet files, and loads those files
into ClickHouse as a separate command.

Raw downloading is owned by [`cc-download-worker`](../cc-download-worker/). Shared pack, index, manifest,
and ready-document contracts live in [`cc-raw`](../cc-raw/).

## Processing contract

The existing two-stage lifecycle is unchanged:

| Stage | Command | Result |
|---|---|---|
| produce | `industry`, `embed`, `tech`, or `both` | Reads one ready RustFS part and writes local Parquet. |
| load | `load --dir ...` | Reads the local Parquet and inserts it into ClickHouse. |

Produce never inserts result rows into ClickHouse. `cc-crawl` runs produce, verifies
`domains.parquet`, runs `load --dir`, and creates the local `out_<mode>_<part>.loaded` marker only after
the load succeeds. A rerun skips that part before the worker reads RustFS.

There is no input-mode flag. RustFS is the only produce input. There is also no public worklist flag:
`cc-download-worker` stores the exact worklist identity, indexes, and packs that comprise a ready part.

## Build

From `commoncrawl/`:

```bash
make -C cc-enrich-worker
make -C cc-enrich-worker test
make -C cc-enrich-worker vet
make -C cc-enrich-worker arm
```

The binary is written to `cc-enrich-worker/bin/cc-enrich-worker`.

## Run

The normal entry point remains `cc-crawl`; its CLI is unchanged:

```bash
set -a; source .env; set +a

./cc-crawl/bin/cc-crawl \
  -base /opt/companycollect/corpscout/commoncrawl/data \
  -mode tech \
  -tech-conc 32 \
  -parts 0-10 \
  -crawl CC-MAIN-2026-25
```

For a direct worker run, identify the already-downloaded RustFS part:

```bash
./cc-enrich-worker/bin/cc-enrich-worker tech \
  --crawl-id CC-MAIN-2026-25 \
  --selection pages25 \
  --part 0 \
  --concurrency 32 \
  --chunk 16384 \
  --out data/CC-MAIN-2026-25/crawl/out_tech_0

./cc-enrich-worker/bin/cc-enrich-worker load \
  --dir data/CC-MAIN-2026-25/crawl/out_tech_0
```

The downloader and processor must use the same selection. `cc-crawl -max-pages N` derives selection
`pagesN`; its default remains `25`.

## Produce flags

Common to `industry`, `embed`, `tech`, and `both`:

| Flag | Default | Meaning |
|---|---|---|
| `--crawl-id` | required | Crawl identity, for example `CC-MAIN-2026-25`. |
| `--selection` | `pages25` | RustFS raw selection identity. |
| `--part` | required | Non-negative URL-index part number. |
| `--out` | derived | Output directory; defaults to `<base>/<crawl>/crawl/out_<mode>_<part>`. |
| `--base` | `OUT_BASE_DIR` | Root used only when `--out` is omitted. |
| `--concurrency` | `32` | Industry/embed pages in flight; tech/both domains in flight. |
| `--chunk` | `1024` | RustFS index rows processed per tech/both chunk. |

Industry/embed/both additionally accept `--embed-batch` and `--embed-concurrency`. Tech/both additionally
accept `--tech-engine` and `--tech-max-bytes`. Run a subcommand with `-h` for exact defaults.

`load` accepts exactly one of `--dir` and `--file`; `--kind` can override kind inference for a single file.

## Environment

RustFS produce input:

| Variable | Meaning |
|---|---|
| `CORPSCOUT_S3_ENDPOINT` | Local-network RustFS endpoint, for example `http://rustfs:9000`. |
| `CORPSCOUT_S3_ACCESS_KEY` | RustFS access key. |
| `CORPSCOUT_S3_SECRET_KEY` | RustFS secret key. |
| `CORPSCOUT_S3_BUCKET` | Bucket containing `commoncrawl/raw` and `commoncrawl/state`. |
| `CORPSCOUT_S3_REGION` | Optional S3-compatible region; defaults to `us-east-1`. |

Industry/embed use `COMMONCRAWL_EMBED_*`. Industry produce reads the NACE reference from ClickHouse, and
`load` writes results using `CLICKHOUSE_*`. Tech produce itself needs neither AWS credentials nor
ClickHouse.

## RustFS input validation and local buffering

Before processing a part, the worker:

1. requires and validates `download/ready.json`;
2. validates every chunk manifest and index checksum;
3. downloads each complete `records.pack` to a temporary directory under `--out`;
4. validates pack size and SHA-256 while streaming;
5. serves the existing parser the same complete compressed WARC record bytes and original source
   coordinates; and
6. removes the temporary packs after a successful run.

Failed downloader records remain auditable in `index.parquet` and are reported as skipped. A ready part
with no downloaded records is rejected. RustFS objects are never removed by this worker.

## Output and resumability

Industry/tech outputs retain the existing fixed names such as `domains.parquet`, `tech.parquet`,
`identifiers.parquet`, and `metadata.parquet`. Embed output remains under the sibling embedding tree and
is skipped when a valid `embeddings.parquet` or `embeddings_fp16.parquet` already exists.

The authoritative processed-and-loaded check remains the local file:

```text
data/<crawl>/crawl/out_<mode>_<part>.loaded
```

RustFS processing markers or distributed leases are not consulted by `cc-crawl`.
