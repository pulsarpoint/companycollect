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
