# cc-enrich-worker

A single Go binary that enriches **CommonCrawl** domains at scale into the **corpscout**
database. Given an index-driven *worklist* (one representative page per domain, resolved from
the CommonCrawl columnar URL index), it fetches each page's WARC record by byte-range and
produces:

- **Industry** — a NACE industry classification per domain (embed the page on a remote GPU,
  cosine-match against a NACE reference matrix + page-type prototypes, apply confidence gates).
- **Technologies** — the tech stack per page (a fast, Aho-Corasick-gated Wappalyzer).
- **Contacts** — emails and social links scraped from the page.
- **Company identifiers** — LEIs scraped from the page (checksum-validated; from JSON-LD
  `leiCode` or text), the join key that links a domain to authoritative registry data
  (LEI → GLEIF). Extensible to VAT / registration numbers.

It is the production processor behind the index-driven enrichment design. It is **stateless** and
**resumable**: it reads a worklist shard, writes result Parquet, and exits. The wrapper
[`run_crawl.sh`](#full-crawl-driver) loops it over all shards and loads results into ClickHouse.

> Pages are **streamed through memory and discarded** — only the extracted result rows are kept
> and written to Parquet. There is no raw-page cache on disk. Re-processing means re-fetching.

---

## Two passes — what runs where

The work splits into two independent passes with very different bottlenecks. Run them as
**separate processes** (even on the same box) so each is sized to its own limit.

| pass | `--mode` | does | bottleneck | needs |
|---|---|---|---|---|
| **Industry** | `industry` | fetch primary page → embed → classify → `*-domains.parquet` | the **GPU** (embedding) | S3 + embedding endpoint + ClickHouse (reads reference) |
| **Tech** | `tech` | fetch pages → Wappalyzer → `*-tech.parquet` | the **CPU** (regex) | S3 only |
| both | `both` (default) | both in one fetch pass | mixed | S3 + embedder + ClickHouse |

Why split: embedding is GPU-bound (~milliseconds of GPU time per domain) while Wappalyzer is
CPU-bound (~hundreds of ms per page). Co-locating them throttles the GPU. As two processes, the
CPU pegs on tech while the GPU stays fed by industry.

```
                 CommonCrawl S3 (warc records, byte-range GET)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼ (industry)                                 ▼ (tech)
  fetch primary page                          fetch page(s)
  parse text / emails / socials               Wappalyzer (AC-gated)
  embed ──► remote GPU /v1/embeddings         union tech per domain
  classify vs NACE reference (from ClickHouse)        │
        │                                             ▼
        ▼                                      *-tech.parquet
  *-domains.parquet                                   │
        └───────────────► ClickHouse ◄────────────────┘
              corpscout.commoncrawl_domains / .commoncrawl_technologies
```

---

## Remote services

| service | used by | how | notes |
|---|---|---|---|
| **CommonCrawl S3** (`s3://commoncrawl`) | all modes | byte-range `GetObject` of one WARC record | **Use signed S3** (AWS creds). Anonymous S3 API is **denied**; the `data.commoncrawl.org` HTTPS CDN (`--s3-anonymous`) works but is latency-bound and **rate-limits** sustained load. CommonCrawl is AWS Open Data → reads are **free** (no requester-pays/egress). |
| **Embedding endpoint** | industry / both | OpenAI-compatible `POST {base}/embeddings` | Production: Qwen3-Embedding-8B served FP8 on a GPU (e.g. a rented 5090) reached over Tailscale. Set `COMMONCRAWL_EMBED_BASE_URL`. Model auto-detected from `/v1/models`. |
| **ClickHouse** (`corpscout` db) | industry / both read; driver writes | native protocol | Industry/both **read** the reference tables (`nace_category_embeddings`, `page_type_exemplars`) at startup. The worker does **not** write to ClickHouse — `run_crawl.sh` loads the output Parquet via `clickhouse-client`. |

The reference tables must be built/refreshed with the **same model** the endpoint serves (page
vectors and reference vectors must match) — see `dagster_v3/scripts/rebuild_reference_embeddings.py`.

---

## Configuration

### Environment variables
```bash
# Embedding endpoint (industry/both)
COMMONCRAWL_EMBED_BASE_URL=http://<gpu-host>:8000/v1
COMMONCRAWL_EMBED_MODEL=                 # optional; empty => auto-detect from /v1/models

# ClickHouse (industry/both read reference; the driver loads outputs)
CLICKHOUSE_HOST=companycollect
CLICKHOUSE_NATIVE_PORT=9002
CLICKHOUSE_USER=...
CLICKHOUSE_PASSWORD=...
CLICKHOUSE_DATABASE=corpscout

# Signed S3 (off-AWS: any IAM user's keys; reads are free)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...                # 40 chars; quote it
AWS_REGION=us-east-1
```
Load them with `set -a; source .env; set +a` (exports every var; `env | grep` to verify the
process will inherit them — `echo` shows set-but-unexported vars and lies).

### Flags (`./cc-enrich-worker -h`)
| flag | default | meaning |
|---|---|---|
| `--worklist` | *(required)* | worklist Parquet shard |
| `--crawl-id` | *(required)* | e.g. `CC-MAIN-2026-25`; stamped on every row |
| `--out` | `out` | output prefix → `<out>-domains.parquet`, `<out>-tech.parquet` |
| `--mode` | `both` | `industry` \| `tech` \| `both` |
| `--tech-engine` | `fast` | `fast` (Aho-Corasick gated, ~4.6×) \| `wappalyzer` (upstream full scan) |
| `--concurrency` | `32` | fetch/parse/tech goroutines (push to ~128 to hide off-AWS fetch RTT) |
| `--embed-concurrency` | `96` | concurrent embed requests in flight (a single stream leaves the GPU idle) |
| `--embed-batch` | `16` | texts per embed request — **keep small**; 128-text requests blow past the engine's `max-num-batched-tokens` → `Running:1`, ~5× slower |
| `--chunk` | `1024` | domains per fetch+embed chunk (lower = earlier GPU traffic) |
| `--tech-max-bytes` | `131072` | cap body bytes fed to Wappalyzer (0 = full body) |
| `--s3-anonymous` | `false` | fetch via the HTTPS CDN instead of signed S3 (off-AWS, no creds) — **rate-limits**, avoid for bulk |
| `--region` | `us-east-1` | S3 region |
| `--skip-tech` | `false` | alias for `--mode industry` |
| `--s3-bucket` / `--s3-prefix` | — | optionally upload outputs to S3 (in-AWS path; off-AWS the driver loads to local ClickHouse instead) |

---

## Build

Pure Go, static binary:
```bash
CGO_ENABLED=0 go build -o cc-enrich-worker .
```
Cross-compile for ARM (e.g. Graviton):
```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o cc-enrich-worker .
```
Or the distroless image: `docker build -t cc-enrich-worker .` (see `Dockerfile`).

---

## Usage

### 1. Generate a worklist shard
Worklists are produced off-AWS by `dagster_v3/scripts/make_worklist.py`, which resolves exact
index-part URLs from CommonCrawl's HTTPS manifest (anonymous S3 LIST is denied, so no globs).
One index part → one shard (~30k domains); the warc subset has ~300 parts/crawl.
```bash
cd ../dagster_v3
uv run python scripts/make_worklist.py --crawl CC-MAIN-2026-25 --part 0 \
    --where "" --out data/commoncrawl/shard0.parquet      # --where "" = all domains
```
Worklist columns: `root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages`.

### 2. Run a pass
```bash
set -a; source ../dagster_v3/.env; set +a

# Tech (CPU-bound) — no embedder/ClickHouse needed
./cc-enrich-worker --mode tech --worklist ../dagster_v3/data/commoncrawl/shard0.parquet \
    --crawl-id CC-MAIN-2026-25 --out run0 --concurrency 128

# Industry (GPU-bound) — needs the embedder + ClickHouse env
./cc-enrich-worker --mode industry --worklist ../dagster_v3/data/commoncrawl/shard0.parquet \
    --crawl-id CC-MAIN-2026-25 --out run0 --concurrency 16 --embed-concurrency 128
```
Each prints a per-chunk line: `timing/page (avg latency): fetch=… parse=… tech=… errs=N/total` and
`progress: N/total pages, … (X pages/s)`. `errs=0` confirms S3 is feeding cleanly.

### 3. Load into ClickHouse
```bash
clickhouse-client --host companycollect --port 9002 -u <user> --password <pw> -d corpscout \
  --query "INSERT INTO commoncrawl_technologies FROM INFILE 'run0-tech.parquet' FORMAT Parquet"
```
(The driver below does this automatically.)

---

## Full-crawl driver

[`run_crawl.sh`](./run_crawl.sh) loops a mode over a shard range — generating each worklist on
demand, running the pass, loading the result into ClickHouse, and dropping a `.loaded` marker.
**Resumable**: done shards are skipped; ClickHouse `ReplacingMergeTree` dedupes re-loads.

```bash
set -a; source ../dagster_v3/.env; set +a

./run_crawl.sh tech 0-2            # smoke test on 3 shards first

# full crawl — two processes (e.g. tmux panes), parallel passes:
./run_crawl.sh tech     0-299     # CPU-bound, most cores
./run_crawl.sh industry 0-299     # GPU-bound, feeds the embedding endpoint
```
Tunables via env: `CRAWL`, `WHERE` (worklist filter; empty = **all** domains), `DATA`,
`DAGSTER_DIR`, `TECH_CONC`, `IND_CONC`, `EMBED_CONC`. Requires `clickhouse-client` on the box.

---

## The fast tech engine (`--tech-engine fast`, default)

Upstream `wappalyzergo` runs ~439 HTML + ~1000 scriptSrc regexes over each page (~85% of CPU).
`FastMatcher` extracts each pattern's **required literal** (`literal.go`, a `regexp/syntax` walk),
builds two **Aho-Corasick** automata over the HTML-body and scriptSrc literals, and per page runs
one AC pass each so only patterns whose literal is actually present get their regex evaluated.

It **reuses** wappalyzergo's `ParsePattern`/`Evaluate` + category map (exact matching + versions)
and mirrors its header→cookie→html/script/meta orchestration (including **transitive** implies).
Soundness: a pattern is skipped only when a genuinely-required literal is absent; patterns without
an extractable literal always run — so **no detection is ever missed**.

Validated by A/B vs upstream on a 29k-domain shard: **`wappalyzer-only = 0`** (a strict superset),
~4.6× faster, and flat on script-heavy pages. `--tech-engine wappalyzer` runs the upstream full
scan for comparison.

---

## ClickHouse storage

Everything lives in the **`corpscout`** ClickHouse database (on-prem, `companycollect:9002`).

### Where the schema is defined
All table schemas are **golang-migrate** SQL migrations in
[`corpscout/clickhouse/migrations/`](../clickhouse/migrations) (`.up.sql` + `.down.sql` per
version). **The migration owns the schema — this worker never issues DDL.** It only reads the
reference tables and writes Parquet whose column order is **pinned to the migration** (a contract
test in `dagster_v3` greps the export column tuple against the migration file). New migrations are
also registered in `EXPECTED_MIGRATIONS` in `dagster_v3/tests/test_clickhouse_migrations.py`.

Apply / roll back (from `corpscout/`, uses `CLICKHOUSE_MIGRATE_URL` → `…/database=corpscout`):
```bash
make clickhouse-migrate-up        # apply pending migrations (golang-migrate via docker)
make clickhouse-migrate-down      # roll back one
```

### Tables this worker uses

**Outputs** — written as Parquet by the passes, loaded into ClickHouse by `run_crawl.sh`:

| table | migration | written by | grain | holds |
|---|---|---|---|---|
| `commoncrawl_domains` | `000046` (+`000049` `nace_confidence`) | **industry** pass | one row per **domain** | **the industry classification** (`nace_code`, `nace_label`, `nace_division`, `nace_confident`, `nace_confidence`, `nace_margin`, `nace_score`, `nace_method`, `nace_top3_codes/labels/scores`) + `page_type`(+score) + contacts (`emails[]`, `email_count`) + lineage (`crawl_id`, `url`, `root_domain`, `subdomain`, `source_url`, `source_run_id`, `resolved_at`) |
| `commoncrawl_technologies` | `000047` | **tech** pass | one row per **(domain, technology)** | `technology`, `category`, `version`, `confidence` + the same lineage columns |
| `commoncrawl_company_identifiers` | `000051` | **tech** pass | one row per **(domain, identifier)** | `id_type` (`'lei'`), `id_value`, `valid` (checksum), `source` (`'jsonld'`/`'text'`) + lineage. Join `WHERE id_type='lei'` to `gleif_reference_data` for the verified legal entity. |

> **Naming note:** there is no `commoncrawl_industries` table. The "industry" pass writes
> `commoncrawl_domains` — the NACE industry fields are columns *on the per-domain row*, not a
> separate table. `Go struct ↔ column` mappings are `DomainRow`/`TechRow` in `output.go`.

> **Confidence:** `nace_confident` (UInt8) is the cheap binary gate `(margin ≥ 0.03 OR top-3
> share one division) AND not junk`. `nace_confidence` (Float32, 0–1) is the continuous score:
> `softmax(top-10 standardized scores / T)[0]`, where scores are standardized by **median + MAD**
> (robust to the top outliers, removes the per-page baseline) — so a domain whose top industry
> stands clearly above the bulk scores high, a mushy/spread one scores low. `T` (`confTemp` in
> `consts.go`) is a temperature to calibrate against a labeled sample. Use `nace_confidence` to
> rank/threshold the LLM-escalation tail (`WHERE nace_confidence < :T AND page_type NOT IN (junk)`)
> — the threshold lives in the query, decided after the scan, re-runnable without re-scanning.

**Reference** — read by the industry/both passes at startup (`reference.go`), **not** written by
this worker (built by the `commoncrawl_classify` Dagster assets in `dagster_v3` and rebuilt with
`dagster_v3/scripts/rebuild_reference_embeddings.py` whenever the served embedding model changes):

| table | migration | read for |
|---|---|---|
| `nace_category_embeddings` | `000044` | the NACE matrix (`code`, `label`, `division`, `embedding Array(Float32)`, …) cosine-matched against the page vector |
| `page_type_exemplars` | `000045` | page-type prototype vectors (`page_type`, `embedding`, …) |

(`commoncrawl_page_signals`, migration `000048`, also exists in the `corpscout` schema but is not
produced or read by this worker.)

### Engine / dedup
All output and reference tables are **`ReplacingMergeTree`** — re-running a shard, or re-loading
its Parquet, is safe and dedupes on the sort key. This is what makes the driver restartable.

---

## Source layout

| file | responsibility |
|---|---|
| `main.go` | flags/env, mode dispatch, reference + embedder setup, chunked run loop, ClickHouse-free output |
| `worker.go` | the goroutine pipeline (`ProcessShard`): group by domain, fetch+parse+tech across cores, batch-embed, classify |
| `fetch.go` | S3 byte-range getter (signed) + HTTPS-CDN getter (`--s3-anonymous`) → gunzip WARC record → `(headers, body)` |
| `embed.go` | batched, concurrent `/v1/embeddings` client (L2-normalized, ordered) |
| `classify.go` | cosine top-3 vs NACE matrix + margin/division-consensus/page-type gates |
| `signals.go` | keyword page-type signals (parked/for-sale/default-server/…) |
| `parse.go` | HTML → visible text, emails, social links |
| `tech.go` | Wappalyzer wrapper; dispatches to `fastTech` when `--tech-engine fast` |
| `techfast.go` | the Aho-Corasick-gated matcher (`FastMatcher`) |
| `lei.go` | LEI extraction (JSON-LD `leiCode` + text regex) with ISO 7064 checksum validation |
| `literal.go` | required-literal extraction from a regex (`regexp/syntax` walk) |
| `reference.go` | load NACE matrix + page-type prototypes from ClickHouse |
| `output.go` | Parquet writers pinned to migrations 046/047 + S3 upload |
| `types.go` / `consts.go` | shared types + thresholds |
| `run_crawl.sh` | resumable full-crawl driver (worklist → pass → ClickHouse load) |

---

## Testing

```bash
go test ./...              # unit + parity tests (offline; fakes for S3/embedder/ClickHouse)
go test -race ./...        # concurrency
go test -bench . -run=NONE # fast-vs-wappalyzer benchmark
```
Tests are offline: `httptest` embed server, fake byte-range getter with gzipped WARC fixtures,
synthetic ClickHouse rows, real `wappalyzergo` for the tech parity test.

---

## Deployment (reference topology)

All-local + a rented GPU, no cloud compute box:

- **Worker host** — a multi-core box (e.g. a 2× Xeon, or Graviton) on Tailscale. Runs both passes
  (tech on most cores, industry on a few + the GPU). Fetches CommonCrawl over **signed S3**.
- **GPU** — a rented 5090 serving Qwen3-Embedding-8B FP8, on Tailscale, via `COMMONCRAWL_EMBED_BASE_URL`.
- **ClickHouse** — on-prem (`companycollect`), reachable over Tailscale; holds reference + outputs.

The embedding model and the ClickHouse reference tables are coupled — rebuild the reference
whenever the served model/precision changes.
