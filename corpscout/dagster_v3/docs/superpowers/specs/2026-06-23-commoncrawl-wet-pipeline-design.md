# CommonCrawl Processing — Worker Package + Control Plane — Design

**Status:** design (plan-only). Built in two phases:
- **Phase 1 (buildable now):** a stateless, dockerized **WET worker** — WET S3 path in → one
  Parquet out (local or S3) — plus a one-file benchmark. Independent of the control plane.
- **Phase 2 (next spec):** the **control plane** — NATS JetStream work queue, an orchestrator,
  and a SQLite ledger — that drives N workers in parallel.

The WARC/technology worker reuses the same shape (separate spec; adds the wappalyzer sidecar
and multi-host scale).

## Context

One crawl per month, id `CC-MAIN-YYYY-WW` (year + ISO week; `CC-MAIN-2026-25` = June 2026),
125 crawls back to 2008. Each crawl = **100,000 WET + 100,000 WARC + 100,000 WAT** files (1:1).
WET ≈ 150 MB/file (~15 TB/crawl, plaintext, no HTTP headers). We process **one crawl per
month**; historical backfill is optional later via the same machinery.

The classification stack is built and tested: `nace_embed` (NACE reference matrix +
`classify_pages` + page-type prototypes), `page_types` (keyword `match_signal`),
`classifier.PageClassifier` (keyword → embedding page-type/NACE → LLM tail), and `ingest`
(`process_wet_to_clickhouse`, `DOMAINS_COLUMNS`). Reference data lives in ClickHouse
(044/045) and as local `.npz`. Output tables exist (046 `commoncrawl_domains`,
047 `commoncrawl_technologies`, 048 `commoncrawl_page_signals`). Industry classification calls
the shared **DGX embedding endpoint** (Qwen3-Embedding-8B), which **degrades under
concurrency** — this is the hard cap on parallelism (see Decisions).

## Architecture (full, both phases)

```
                 ┌──────────────┐   list crawl files, enqueue pending
                 │ orchestrator │───────────────────────────────────┐
                 │ (1 process)  │                                    ▼
                 │  owns SQLite │                            ┌───────────────┐
                 │   ledger     │◀── done/failed (NATS) ──────│ NATS JetStream│
                 └──────────────┘                            │  work queue   │
                        ▲                                    └───────┬───────┘
                        │ updates ledger (sole writer)               │ pull task
                        │                                    ┌───────┴────────┐
                        └──────────────────── ack ───────────│  worker × N    │
                                                             │ WET→Parquet    │──▶ Parquet (local/S3)
                                                             │ (stateless)    │
                                                             └────────────────┘
                                                                  │ calls
                                                                  ▼
                                                        DGX embedding endpoint (shared, caps N)

   (separate periodic step) INSERT INTO corpscout.commoncrawl_domains SELECT * FROM s3(glob)
```

- **Worker (Phase 1):** stateless. Input = one WET file (S3 path or CDN url) + config; output =
  one Parquet (local or S3), named deterministically by `(crawl_id, file_index)`. Loads the
  reference matrices **once** (baked into the image). Never touches the ledger or ClickHouse.
- **NATS JetStream (Phase 2):** durable work-queue stream (orchestrator publishes file tasks) +
  a completion subject (workers publish `done`/`failed`). Explicit ack, ack-wait redelivery on
  crash, `max-deliver` + dead-letter so a permanently-bad file stops after K tries.
- **Orchestrator (Phase 2):** single process; **sole writer** of the SQLite ledger. Monthly:
  resolve latest crawl, list `wet.paths.gz`, enqueue files not `done`, consume completions,
  update the ledger, signal "crawl complete" when `done == file_count`.
- **SQLite ledger (Phase 2):** one file on the orchestrator host;
  `commoncrawl_file_ledger(crawl_id, kind, file_index, status, attempts, error, finished_at)`.
  Single-writer ⇒ no lock contention; workers stay multi-host via NATS. Plain SQL behind a
  small interface ⇒ swap to Postgres later if the orchestrator ever needs HA.
- **ClickHouse loading:** a **separate** periodic step globs the Parquet shard into
  `commoncrawl_domains`. Not the worker's job (keeps the worker retry-safe and decoupled).

## Phase 1 — the worker package (build now)

### Components
1. **`commoncrawl_enrich.ingest` refactor** — extract `build_wet_domain_rows(source, *,
   classifier, crawl_id, …) -> list[tuple]` out of `process_wet_to_clickhouse` (stream
   homepages → classify → `commoncrawl_domains` tuples). `process_wet_to_clickhouse` becomes
   `build_wet_domain_rows(...) + _insert(...)` (behaviour-preserving). Add
   `DOMAINS_PARQUET_SCHEMA` (column order == `DOMAINS_COLUMNS`, list/timestamp types matching
   the migration) and `write_domain_rows_parquet(rows, out_path)`.

2. **`commoncrawl_enrich.worker`** — the worker core + CLI:
   - `run_wet_task(task: WetTask, *, classifier, s3=None) -> dict` — download the WET (S3 path
     via boto3, or `https://data.commoncrawl.org` url), `build_wet_domain_rows`,
     `write_domain_rows_parquet` to a temp file, upload to the output dest (local path or S3),
     return stats (rows, industries, page_types, with_email, parquet_bytes, timings).
   - `WetTask` (from JSON): `crawl_id, file_index, wet_path` (s3:// or https url),
     `output` ({`kind`: "local"|"s3", `path`/`bucket`+`prefix`}), optional `limit`.
   - CLI **one-shot** mode: `python -m commoncrawl_enrich.worker --task task.json`
     (or `--wet … --out …`) → run once, print stats, exit. (Phase 2 adds a `serve` subcommand
     that consumes NATS and calls the same `run_wet_task`.)
   - Reference: load `NaceReference`/`PrototypeSet` from `.npz` baked into the image (path via
     env, default `/refs/*.npz`); `EmbeddingClient.from_env()`; LLM optional via env.

3. **Docker image** (`corpscout/commoncrawl-worker/`): Python + `commoncrawl_enrich` + the
   reference `.npz` baked in. Built in **GitHub CI** → registry (GHCR). Entry = the worker CLI.
   Config via env (`COMMONCRAWL_EMBED_BASE_URL`, S3 creds/endpoint, `COMMONCRAWL_REFS_DIR`, …).

4. **`run.sh`** — pull the image from the registry and run it with a task JSON
   (`docker run … commoncrawl-worker --task /work/task.json`). Phase-1 convenience + the unit
   the Phase-2 orchestrator/compose will invoke.

5. **Benchmark** = run the one-shot worker on one real WET file; record download time, process
   time, homepages/file, Parquet bytes/row & bytes/file, then project ×100,000 → core-days,
   Parquet storage, download TB for one crawl. These size N and storage for Phase 2.

### Data flow (Phase 1)
```
WetTask(wet_path, output) ─▶ download (boto3 s3 / requests cdn) ─▶ temp .warc.wet.gz
   ─▶ build_wet_domain_rows(classifier)  # homepages → keyword/embedding/LLM-tail
   ─▶ write_domain_rows_parquet ─▶ temp .parquet
   ─▶ upload to output (local copy / s3 put)  # name: {crawl_id}/{file_index}.parquet
   ─▶ stats
```

### Error handling
- Streamed download via the existing `requests` retry session; malformed records never crash
  the loop (`decode(errors="replace")`). Empty file → empty-but-typed Parquet (still loadable).
- Embedding endpoint required; LLM tail optional. Worker reports which path ran.
- Deterministic output name ⇒ a retry **overwrites**, never duplicates; ReplacingMergeTree
  cleans any dup at the CH side.

### Testing
- Unit (offline, warcio fixtures): `build_wet_domain_rows` matches the rows
  `process_wet_to_clickhouse` builds; `write_domain_rows_parquet` round-trips to a Parquet
  whose schema == `DOMAINS_PARQUET_SCHEMA` / columns == `DOMAINS_COLUMNS`; `run_wet_task` with
  a fake classifier + a local WET fixture + local output writes the expected Parquet and stats;
  `WetTask` JSON parsing. The Docker image + the live benchmark are run manually.

## Phase 2 — control plane (next spec, documented here for shape)
NATS JetStream stream + consumer config (work-queue retention, ack-wait, max-deliver,
dead-letter), the orchestrator (file listing, enqueue, completion consumer, ledger updates,
crawl-complete signal), the SQLite ledger schema + interface, the worker `serve` subcommand,
the compose file (`worker` scaled to N, NATS, orchestrator), and the separate CH-load step.
Worker concurrency capped to the DGX embedding throughput (see Decisions).

## Decisions
- **1:1 file → Parquet**, deterministic name `{crawl_id}/{file_index}.parquet` (resumable,
  parallelizable, CH-loadable, retry-overwrites). Revisit batching only if bytes/file is tiny.
- **DGX embedding endpoint is the hard cap on N**, not CPU/bandwidth — it degrades under
  concurrency. Phase 2 caps worker/embedding concurrency to its sustained batched throughput;
  a central embedding-batching proxy is an optional later optimization.
- **Reference `.npz` baked into the image** (small, changes rarely) — workers never query CH at
  startup. Rebuild image to refresh.
- **NATS JetStream** (durable, ack, redelivery, dead-letter) — not core NATS.
- **SQLite ledger, orchestrator is sole writer**; workers report via NATS. Postgres is the
  swap-in if the orchestrator ever needs HA. ClickHouse is wrong for the ledger (no cheap
  single-row upsert; it's the OLAP *output* store, not operational state).
- **ClickHouse loading is a separate step**, not the worker.
- **WET worker is Python-only**; the WARC worker (separate spec) adds the wappalyzer sidecar.

## Open questions (resolved)
- Parquet granularity → 1:1. Ledger store → SQLite (orchestrator-owned). Queue → NATS
  JetStream. Benchmark classifier mode → embedding-only first, LLM tail optional.
