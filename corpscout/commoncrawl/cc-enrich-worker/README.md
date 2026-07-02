# cc-enrich-worker

The **processor** of the CommonCrawl enrichment pipeline: a single Go binary that byte-range-fetches
WARC pages from S3, runs one of two workflows over them (industry classification or technology/company-info
extraction), writes **Parquet**, and — as a separate step — **loads** that Parquet into ClickHouse.

It is a subcommand CLI: `cc-enrich-worker <industry|embed|tech|both|load> [flags]`. The command **is** the
workflow. It does not orchestrate parts or build worklists — that's `cc-crawl` and `index-builder`
(see [`../README.md`](../README.md)).

**Two stages, and every ClickHouse `INSERT` is in stage 2 (`load`):**

| Stage | Command | Does | ClickHouse |
|---|---|---|---|
| **produce** | `industry` / `embed` / `tech` / `both` | fetch → (embed/classify \| detect tech) → write Parquet | only **reads** the NACE reference (industry/both) |
| **load** | `load` | read the produced Parquet → `INSERT` into `commoncrawl_*` | the **only** writer |

Splitting them means a produce can be inspected before it's loaded, a load can re-run without re-fetching,
and a crashed produce never half-writes ClickHouse. Both stages use the native ClickHouse protocol, so
`clickhouse-client` is never required.

> Full pipeline context (cc-crawl driver, workflows A/B, per-part loop, the ClickHouse schema):
> [`../README.md`](../README.md) and [`../docs/schema.md`](../docs/schema.md). This doc is the worker's
> CLI + internals reference.

## Build & run

Go **1.26+**, no cgo. From `commoncrawl/`:

```bash
make -C cc-enrich-worker            # -> cc-enrich-worker/bin/cc-enrich-worker  (static, CGO_ENABLED=0)
# or, at commoncrawl/:  make worker
make -C cc-enrich-worker arm        # cross-compile linux/arm64 (Graviton)
make -C cc-enrich-worker test       # go test ./...
make -C cc-enrich-worker vet        # go vet ./...
make -C cc-enrich-worker clean      # rm -rf bin
# amd64 cross-compile (no target):  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/cc-enrich-worker ./cmd/cc-enrich-worker
```

A minimal run (produce, then load) — see §Flags for every option:
```bash
set -a; source ../.env; set +a
./bin/cc-enrich-worker tech --worklist ../data/index/CC-MAIN-2026-25/shard_tech_0.parquet \
                            --crawl-id CC-MAIN-2026-25 --out ../data/crawl/run0
./bin/cc-enrich-worker load --dir ../data/crawl/run0
```

## Module layout

```
cc-enrich-worker/
├── cmd/cc-enrich-worker/main.go   # subcommand dispatch, flag parsing, run() + runLoad()
├── internal/
│   ├── classify/   # NACE cosine classify + page-type signals + startup model-match check
│   ├── embed/      # OpenAI-compatible embedding client (batching, concurrency, retry, L2-normalize)
│   ├── extract/    # schema.org JSON-LD profile, LEI, VAT (checksum-validated)
│   ├── fetch/      # RangeGetter: signed S3 + anonymous CDN; FetchRecord unpacks a WARC record
│   ├── load/       # Parquet -> ClickHouse (native protocol); legacy fat-domains fan-out
│   ├── model/      # shared types (Reference, Prototypes, WorklistItem, Technology, Identifier, …)
│   ├── output/     # *Row structs (parquet+ch tags), Write* funcs, S3 upload
│   ├── parse/      # HTML -> visible text, emails, social links
│   ├── tech/       # DetectTech: fast Aho-Corasick gating | upstream wappalyzergo
│   ├── vec/        # Dot, L2-normalize
│   └── worker/     # ShardConfig, ProcessIndustryStream, FetchChunk + Finalize
├── bin/            # build output (gitignored)
├── Makefile  ·  Dockerfile  ·  go.mod
```

## Subcommands

`cc-enrich-worker <command> [flags]`. The command is the workflow; mode-specific flags appear only under
their command.

| Command | Fetches | Embeds? | Classifies NACE? | Needs | Writes | Bottleneck |
|---|---|---|---|---|---|---|
| **`industry`** | 1 primary page/domain | yes (instructed) | yes | embed endpoint + ClickHouse reference + S3 | `domains`, `industries`, `page_signals` + `embeddings.parquet` | GPU |
| **`embed`** | 1 primary page/domain | yes (vector only) | no | embed endpoint + S3 | **only** `embeddings.parquet` — no ClickHouse, no load | GPU |
| **`tech`** | many pages/domain | no | no | S3 only | `domains`, `technologies`, `identifiers`, `metadata`, `contacts` | CPU |
| **`both`** | many pages/domain | yes | yes | everything industry+tech need | all of the above | GPU (discouraged) |
| **`load`** | — (reads Parquet) | — | — | ClickHouse | `INSERT`s into `commoncrawl_*` | — |

- **`both` is discouraged** — co-locating the CPU tech pass with the GPU industry pass throttles the GPU;
  prefer two separate processes.
- **`embed` is `industry` minus the classify + ClickHouse** — its whole job is to persist the raw page
  vector for parts that were processed before vectors were stored (backfill) or that industry hasn't
  reached. It is **idempotent**: it skips a shard whose output dir already holds a complete
  `embeddings.parquet` **or** `embeddings_fp16.parquet` (§Limits).
- **`load` is decoupled** from produce so you can inspect Parquet before inserting and re-load without
  re-fetching (§Processing).

## Flags

Parsed per-subcommand via `flag.NewFlagSet(..., flag.ExitOnError)` in `parse()` / `runLoad()`
(`cmd/cc-enrich-worker/main.go`). `-h` under any command prints its own set.

### Common to `industry`, `embed`, `tech`, `both`

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--worklist` | string | **required** | worklist Parquet shard (one row per page to fetch) |
| `--crawl-id` | string | **required** | e.g. `CC-MAIN-2026-25`; stamped on every output row |
| `--out` | string | `../data/crawl/<worklist-stem>/` | output **directory** (fixed per-mode filenames); an explicit dir must be **empty** (except `embed`, which reuses it) |
| `--concurrency` | int | `32` | fetch/parse goroutines; push toward ~128 to hide off-AWS fetch RTT |
| `--chunk` | int | `1024` | domains per fetch+process chunk (lower = earlier GPU traffic) |
| `--s3-anonymous` | bool | `false` | fetch via the anonymous HTTPS CDN instead of signed S3 — **rate-limited**, no upload |
| `--s3-bucket` | string | (unset) | if set, upload outputs to this bucket (in-AWS path; signed S3 only) |
| `--s3-prefix` | string | (unset) | key prefix for uploaded outputs |

### `industry`, `embed`, `both` only

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--embed-batch` | int | `16` | texts per embed request — keep small; big batches overflow the engine's token budget |
| `--embed-concurrency` | int | `96` | embed requests in flight (saturate the GPU) |

### `tech`, `both` only

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--tech-engine` | string | `fast` | `fast` = Aho-Corasick-gated Wappalyzer (a strict superset of the library's output, ~4.6× faster) \| `wappalyzer` = unmodified `wappalyzergo` (every fingerprint; parity baseline, slower) |
| `--tech-max-bytes` | int | `131072` | cap body bytes fed to the tech matcher (`0` = full body; full-body regex ≈ 1.2 s/page) |

### `load` only

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--dir` | string | — | a shard output directory; loads every `{domains,industries,page_signals,metadata,contacts,tech,identifiers}.parquet` present |
| `--file` | string | — | a single Parquet file to load |
| `--kind` | string | (inferred) | override the kind for `--file`: `domains\|industries\|page_signals\|metadata\|contacts\|tech\|identifiers` |

Pass **exactly one** of `--dir` / `--file`.

## Environment variables

Read via `envOr()` / `envInt()` in `main.go`, plus the fetch/embed clients. Load them with
`set -a; source ../.env; set +a` (see [`../README.md`](../README.md) §2).

| Var | Default | Controls |
|---|---|---|
| `COMMONCRAWL_EMBED_BASE_URL` | `http://localhost:8000/v1` | OpenAI-compatible embedding endpoint |
| `COMMONCRAWL_EMBED_MODEL` | (empty → auto-detect) | model name; empty ⇒ probe `/v1/models` for the served model |
| `COMMONCRAWL_EMBED_MAX_CHARS` | `0` → **2000** | chars per text before embedding (`0` means use the built-in cap 2000) |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_NATIVE_PORT` | `9000` | ClickHouse native protocol port |
| `CLICKHOUSE_DATABASE` | `corpscout` | database |
| `CLICKHOUSE_USER` | `default` | user |
| `CLICKHOUSE_PASSWORD` | (empty) | password |
| `AWS_REGION` | `us-east-1` | S3 region (the CommonCrawl bucket is permanently `us-east-1`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | (none) | signed-S3 credentials; validated upfront (5 s) in `NewS3Getter`. Off-AWS you must export these |
| `CC_BASE_URL` | (empty → `data.commoncrawl.org/`) | anonymous CDN base for `--s3-anonymous` |

**Connection gating:** `embed` uses **no** ClickHouse (logs `embed-only mode: no NACE reference, no
ClickHouse, no load`). `industry`/`both` connect to **read** the reference + run the startup model-match
check. `load` always connects.

## Processing logic

### industry / embed — `ProcessIndustryStream` (continuous fetch→embed)

One primary page per domain, streamed so the GPU is fed non-stop (no per-chunk "embed all then wait"
barrier):
1. **Fetch workers** (semaphore = `--concurrency`, default 32) each fetch the domain's `Primary` page
   (the worklist is ordered shallowest-first, so the first row per `root_domain` is the homepage),
   30 s timeout/record, parse HTML → visible text, and stream a `streamItem` (domain index, text, WARC
   coords) onto a channel.
2. **Embed workers** (pool = `--embed-concurrency`, default 96) batch texts into `--embed-batch`
   (default 16) and POST to the endpoint.
   - **embed mode:** store the raw vector only — skip classify and the domain/industry/signal rows.
   - **industry/both:** page-type **signal matching** runs first (keyword regexes; a matched
     junk/parked type wins and skips NACE), else `classify.Classify` does cosine over the NACE matrix and
     the row is fanned into `domains` + `industries` (top-3) + `page_signals`.

Per-domain result slices are indexed by domain order (no shared writes).

### tech / both — `FetchChunk` + `Finalize` (chunked pipeline)

Tech wants **coverage**, so it fetches *all* of a domain's pages and aggregates:
- **FetchChunk** (semaphore = `--concurrency`): per page — detect tech (body capped at `--tech-max-bytes`,
  deduped by tech name per domain), email regex, schema.org JSON-LD profile, LEI (text+jsonld), VAT
  (format+checksum). First surviving page becomes the domain's `primaryURL`; the `Primary` page's text is
  kept for embedding (`both`).
- **Finalize** (`runtime.NumCPU()` classify workers, non-overlapping index ranges): for `both`, embed +
  classify the primary texts; fan each domain into tech / identifier / metadata / contact rows.
- **Chunking** (`--chunk`, default 1024): the next chunk is **prefetched over the network while the
  current chunk embeds on the GPU**, so the GPU isn't idle during fetch.

### embed verify-and-skip (idempotency)

Before doing any work, `embed` checks the output dir for a complete vector file under **either** name —
`embeddings.parquet` (fp32) **or** `embeddings_fp16.parquet` (converted offline). It reads only the
Parquet **footer row count**; `> 0` ⇒ the shard is done and is skipped (logs `skip: embeddings already
present …`). If the footer can't be read (fp16) but the file is non-empty, it's accepted as done. There is
**no `.loaded` marker** for embed — the vector file *is* the marker. Non-embed modes instead require
`--out` to be **empty**.

### load — Parquet → ClickHouse

`load --dir` reads each fixed-name Parquet and `INSERT`s it into its table over the native protocol
(§Output). `load --file [--kind]` loads one. The column list is derived from each struct's `ch` tags, so a
table may carry extra defaulted columns the worker doesn't write. All target tables are
**ReplacingMergeTree**, so re-loading is safe (dedupe on the sort key, newest `resolved_at` wins) — **read
with `FINAL`**. An **old fat `domains.parquet`** (pre-split, detected by a `nace_top3_codes` column) is
fanned into the split tables automatically.
