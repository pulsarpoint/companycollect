# CommonCrawl Enrichment — Go Single-Pass Worker — Design

**Status:** design. The production worker for index-driven domain enrichment. The Python
`index_enrich` package remains as the **reference/validation harness** (and the Dagster
`commoncrawl_classify` assets still **build** the reference data in ClickHouse). This Go
binary is the thing that processes a crawl at scale.

## Why Go (decided)
- **wappalyzergo is already Go** → native tech detection, no Python↔Go HTTP sidecar.
- **No GIL** → one binary parses + fingerprints across all cores (the Python worker is
  single-core per process; we'd otherwise run N processes).
- **Single pass over one fetch**: the WARC record we pull from the index is the full HTTP
  response (headers + body), so the same fetch yields industry, technology, and contacts.
- **Use the GPU's shadow**: embedding on the DGX is the bottleneck; while it runs, the CPU
  fetches + Wappalyzer-scans *extra* pages per domain for per-page tech — free in wall-clock,
  and free in S3 cost (CommonCrawl is AWS Open Data: no request/transfer charges in us-east-1).

## What it produces (one pass, per domain)
From the index worklist of **top-K shallowest pages per domain** (`rn = 1` is the primary):
- **Industry** — embed the primary page's text (HTTP → DGX), cosine-match vs the NACE
  reference matrix + page-type prototypes, apply keyword page-type signals + the confidence
  gate. One embed per domain (the GPU cap).
- **Technologies** — wappalyzergo on **all K pages** (headers+body), unioned per domain.
- **Emails / socials** — from the primary page body.

Outputs Parquet → S3 → ClickHouse `commoncrawl_domains` (046) + `commoncrawl_technologies`
(047). Reference data + the embedding model are shared with Python (ClickHouse + the DGX);
only the *classify logic* (matmul + gates + keyword regexes) is ported to Go.

## Concurrency model (GPU-bound, CPU/IO in its shadow)
```
worklist shard (top-K pages/domain, grouped by domain)
        │
  ┌─────┴───────── fetch+parse+wappalyzer goroutines (all cores) ───────────┐
  │ per page: S3 byte-range GET → gunzip record → html+headers              │
  │           → text/emails/socials (primary) + wappalyzergo tech (all K)   │
  └──────────────────────────── domain results channel ────────────────────┘
        │  primary texts batched
        ▼
  batched embed client → POST DGX /v1/embeddings (the bottleneck)
        ▼
  classify (gonum/manual matmul vs NACE M + prototypes P; keyword signals; gates)
        ▼
  build rows → Parquet writer → S3 → (CH s3() load, separate step)
```
K is tuned "scan until the embed queue needs feeding": the fetch/tech goroutines run ahead of
the embedder, so per-page tech fills the CPU slack the GPU wait creates. Bound K (e.g. 5–10)
as a safety cap, not for cost.

## Components (Go module `corpscout/cc-enrich-worker`)
- `config.go` — flags/env (S3, DGX `/v1/embeddings` URL, ClickHouse DSN, worklist path, out
  prefix, K, batch sizes, concurrency, thresholds).
- `reference.go` — load `nace_category_embeddings` (code/label/division + `Array(Float32)`) and
  `page_type_exemplars` (page_type + vector) from ClickHouse into normalized `[]float32`
  matrices (clickhouse-go v2).
- `fetch.go` — S3 byte-range GET (`aws-sdk-go-v2`) → gunzip the single WARC record → parse the
  HTTP response into `(headers map[string][]string, body []byte)`.
- `parse.go` — `x/net/html` → visible text; `regexp` emails; social links.
- `tech.go` — `wappalyzergo.New()` once; `FingerprintWithInfo(headers, body)` per page →
  `[]Technology{name,category,version}`.
- `embed.go` — batched HTTP client to the OpenAI-compatible `/v1/embeddings`; truncates text
  to `EMBED_MAX_CHARS`; returns L2-normalized `[]float32` vectors.
- `classify.go` — port of the gates: cosine `page·M[j]` top-3 + margin + division consensus;
  `page·P[j]` max → page_type if ≥ threshold; keyword `match_signal` (~28 regexes); combine
  keyword → embedding-page-type → NACE.
- `output.go` — `parquet-go` writers for the `commoncrawl_domains` + `commoncrawl_technologies`
  column orders (migrations 046/047); upload to S3.
- `worker.go` — orchestration + the goroutine pipeline above.
- `main.go` — wire it together; read a worklist shard, process, write, exit (Phase-2: a NATS
  consumer wraps the same core).

## Data flow / error handling
- A page that fails to fetch/parse is **skipped** (logged); the domain still emits a row if its
  primary page succeeded. A domain whose primary fails is dropped (no industry).
- Deterministic output names per shard (`{crawl}/{shard}.parquet`) → retries overwrite;
  `ReplacingMergeTree` dedupes on the CH side.
- The embed endpoint is required; the LLM tail is **not** ported (the embedding+page-type gate
  is the classifier; low-confidence domains keep their best NACE + the top-3 audit fields).

## Testing
- `reference_test.go` — build matrices from synthetic CH rows (fake rows), assert normalization.
- `classify_test.go` — synthetic vectors: confident-by-margin, division-consensus, page-type
  threshold, keyword precedence (mirror the Python `test_nace_embed`/`test_classifier` cases).
- `embed_test.go` — `httptest` mock `/v1/embeddings`, assert batching + normalization.
- `fetch_test.go` — a gzipped WARC record served by a fake S3 (interface), assert headers+body.
- `parse_test.go` / `tech_test.go` — known HTML → expected emails + WordPress/Nginx tech.
- `worker_test.go` — fake S3 + fake embedder + real classify → expected domain + tech rows.
- `output_test.go` — round-trip a row set through the Parquet writer; schema/column order match.

## Decisions
- **One fetch per page; K pages/domain**; embed only the primary (GPU cap); Wappalyzer all K
  (CPU slack). Per-page tech is the *only* tech pass — no separate 100 TB WARC stream.
- **Reference data + embedding stay shared with Python** (ClickHouse + DGX); port only classify.
- **Thresholds** (`margin 0.03`, `page_type 0.55`, `EMBED_MAX_CHARS 2000`) are config, defaulting
  to the validated Python values, so both stacks agree.
- Output = Parquet → S3 → CH `s3()` load (not direct WAN insert), per the AWS↔on-prem split.

## Out of scope (later)
NATS consumer + orchestrator + SQLite ledger (Phase 2 control plane); the AWS-local GPU
embedder (only if one DGX becomes the limit); the LLM tail.
