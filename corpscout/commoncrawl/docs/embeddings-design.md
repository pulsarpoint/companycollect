# Domain embeddings — storage & vector-search design

Stop discarding the industry-pass page embedding. The embedding is the **expensive** artifact (the GPU
forward pass); the NACE classification is a cheap cosine matmul derived from it. Persisting it unlocks
two things the current pipeline throws away:

1. **Re-classify for free, forever** — re-run NACE (a revision, a tuned gate, an entirely different
   taxonomy: SIC/ISIC, a custom scheme) as a **batch matmul over stored vectors**, no GPU, no re-fetch.
2. **Similarity / ANN** — "domains like X", peer/competitor discovery, clustering, near-duplicate &
   parked-site detection — filtered by `country`/`nace_division`/etc.

## Two access patterns → two stores

| Use | Access pattern | Store |
|---|---|---|
| re-classification, ML feature | full-scan **batch matmul** (all vectors × the 1025-cat matrix) | **Parquet** (columnar, the source of record) |
| similarity search | **ANN** nearest-neighbor, filtered | **Qdrant** (built *from* the parquet) |

**Qdrant is an index built from the parquet, not the source of record.** That means we can rebuild it,
re-quantize it, or change ANN params anytime without re-embedding. The vectors do **not** go into
ClickHouse — at full-crawl scale they'd add hundreds of GB of incompressible (high-entropy) data;
ClickHouse keeps holding the cheap *derived* tables (`industries`, `page_signals`, …), the vectors live
in Parquet (on S3, like the worker's other outputs) + Qdrant.

## What to store + format

- The Qwen3-Embedding-8B vector (native 4096-dim), **L2-normalized** (cosine ⇒ dot; quantizes far
  cleaner on normalized vectors).
- **Compact**: dim + dtype is gated by an eval (below). Default candidate: **2048-dim int8** (≈2 KB/domain)
  via Matryoshka truncation (Qwen3 is MRL-trained) + per-vector int8 scale. Re-embed the **NACE matrix at
  the same dim+quant** so re-classification cosine is apples-to-apples.
- Raw fp32/4096 (16 KB/domain → 1.6 TB at 100M) is the lossless ceiling, kept only if the eval says the
  compact form loses too much.

## `embeddings_fp32.parquet` (new worker output) → S3

One row per domain (industry mode = 1 representative page/domain). Fixed filename `embeddings_fp32.parquet`
in the shard output dir, uploaded to S3 with the other outputs.

| Column | Type | What |
|---|---|---|
| `crawl_id` | string | snapshot id |
| `root_domain` | string | domain (the key / Qdrant point id source) |
| `embedding` | `list<int8>` | the L2-normalized, int8-quantized vector (dim = the eval's choice) |
| `scale` | float32 | per-vector dequant scale (`fp ≈ int8 * scale`) |
| `embed_model` | string | e.g. `qwen3-embedding-8b` |
| `embed_dim` | uint16 | stored dim (e.g. 2048) |
| `source_url` | string | page embedded |
| `source_run_id` | string | worker run |
| `resolved_at` | `timestamp[ms,UTC]` | produced-at |

(If the eval picks fp16 instead of int8, `embedding` is `list<float16>` and `scale` is dropped.)

## Qdrant collection

- **Collection** `commoncrawl_domains` (single, `crawl_id` in the payload — or per-crawl if you snapshot).
- **Vector**: size = stored dim, **distance = Cosine** (vectors pre-normalized).
- **Quantization**: Qdrant's built-in **scalar (int8)** on-disk, original kept for optional rescore; for
  100M+ consider **binary quantization** (32× smaller, rescored against int8) — Qdrant handles this, so
  the fp8/int8/binary question is a *config*, not a re-export.
- **Payload** (for filtered search): `root_domain`, `crawl_id`, `nace_code`, `nace_division`,
  `is_primary`, `country`, `page_type`.
- **Point id**: `UUIDv5(root_domain)` (stable, dedupes re-runs to the same point).

## Pipeline integration

- **Worker (industry pass)** — `worker.go` already computes the vector (`vecs[vi]`) and discards it after
  classify. Instead: L2-normalize → int8-quantize → emit `embeddings_fp32.parquet` (new `output.EmbeddingRow`
  + `WriteEmbeddings`; `ShardResult.Embeddings`; `main.go` writes it in industry/both). No new fetch/GPU
  cost — the vector is already in hand.
- **Indexer** — a small step that reads `embeddings_fp32.parquet` and **upserts to Qdrant** (Go Qdrant client,
  or a Python loader). Decoupled from the ClickHouse load, so the Qdrant index is rebuildable from the
  parquet. Could be a `cc-crawl` third step (`index`) or a standalone `qdrant-loader`.
- **Re-classification** (future, no GPU) — read all `embeddings_fp32.parquet` (S3), dequantize, matmul against
  the re-embedded NACE matrix at the same dim, write `commoncrawl_industries`. Turns "reclassify the whole
  crawl" from a multi-day GPU job into a CPU matmul.

## New components / migrations

- `internal/output`: `EmbeddingRow` + `WriteEmbeddings` (worker — the one cc-enrich-worker change).
- `internal/worker`: keep + quantize the vector, add to `ShardResult`.
- `cmd`: write `embeddings_fp32.parquet`; S3 upload already covers it.
- A vector **indexer** (new small binary or script) → Qdrant.
- **No ClickHouse migration for vectors** (they don't go to CH). Optional: a thin
  `commoncrawl_domain_embeddings` table *only if* you later want SQL-joined access — not in v1.

## Eval — gates dim & dtype (do first)

~30 min, then it's decided, not guessed:
1. Sample ~10k domains; compute the **fp32/4096** NACE top-1 + nearest-neighbor set as ground truth.
2. Compare **1024-fp16 / 2048-fp8 / 2048-int8** (and binary, for Qdrant) on (a) top-1 NACE agreement,
   (b) recall@10 of neighbors vs the fp32 baseline.
3. Pick the smallest within tolerance → that's `embed_dim`/dtype for the schema above.

## Costs

| Form | bytes/domain | 3M | 100M |
|---|---|---|---|
| fp32 / 4096 | 16 KB | 48 GB | 1.6 TB |
| fp16 / 1024 | 2 KB | 6 GB | 200 GB |
| **int8 / 2048** | **2 KB** | **6 GB** | **200 GB** |
| Qdrant binary index (2048-bit) | 256 B | 0.8 GB | 25 GB (+ on-disk originals) |

## Open decisions

1. **Dim/dtype** — the eval decides (default 2048-int8).
2. **Indexer placement** — `cc-crawl index` step vs standalone `qdrant-loader`. Lean standalone (rebuild
   the index from S3 anytime, independent of crawl runs).
3. **Qdrant only, or pgvector/ClickHouse-ANN?** — Qdrant for self-host + built-in quantization +
   filtering + 100M scale (see README/notes). pgvector if you want to colocate with the existing Postgres
   and stay under ~50M.
4. **Snapshot semantics** — one Qdrant collection with `crawl_id` payload (overwrite per domain) vs
   per-crawl collections (history). Lean single + overwrite (latest wins), matching ReplacingMergeTree.
