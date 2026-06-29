# Embed-only mode — backfill embeddings for already-processed segments

## Goal
The ~100 already-done industry parts (≈6.4M domains) discarded their page vectors. Add an **embed-only**
pass that re-fetches + re-embeds those pages and writes `embeddings.parquet` — **without** re-classifying
(NACE), **without** ClickHouse inserts, **without** the load step. Run it over the done parts to backfill
the embedding store. (Future parts already get vectors from the normal industry pass.)

The vectors are gone — there is no stored copy — so recovering them genuinely requires re-fetch +
re-embed. The existing **industry worklists** (`shard_industry_<p>.parquet`) still hold the exact page
coords, so it's a clean re-run, not a re-discovery.

## Shape: two pieces

### 1. Worker `embed` subcommand
```
cc-enrich-worker embed --worklist <shard_industry_p> --crawl-id <crawl> --out <embed-dir> \
                       [--concurrency N --embed-concurrency M]
```
Reuses the existing `ProcessIndustryStream` fetch→embed pipeline (GPU fed continuously) with a new
`EmbedOnly` flag. Skips: reference load, ClickHouse connect, NACE classify, `domains`/`industries`/
`page_signals`, and the load step. Writes **only** `embeddings.parquet` (the same `EmbeddingRow` with
page metadata: warc coords, subdomain, text_len, source_url) to `--out`.

`--out` is the **embed dir directly** (e.g. `data/embedding/out_industry_43`), so embed-only output lands
in the **same `data/embedding/` tree** as the industry pass — uniform for the future Qdrant indexer.

**Touch points:**
- `internal/worker/worker.go`
  - `ShardConfig`: add `EmbedOnly bool`.
  - `ProcessIndustryStream` flush: set `embByDi[b.di]` first, then `if cfg.EmbedOnly { continue }` — skip
    `MatchSignal`/`Classify` and the domain/industry/signal rows. `ref`/`protos` go unused (pass `nil`).
  - Final assembly: when `EmbedOnly`, include a domain iff `embByDi[di].RootDomain != ""` (don't gate on
    `domains[di]`, which is never filled in embed-only).
- `cmd/cc-enrich-worker/main.go`
  - `parse()` / the `os.Args[1]` switch (line ~122): accept `"embed"`; it takes the industry flags
    (`--concurrency`, `--embed-concurrency`) but **no** reference/CH flags.
  - `run()`:
    - Split the `if mode != "tech"` block (line ~265): load **reference + ClickHouse** only for
      `industry`/`both`; build the **embedder** for `industry`/`both`/`embed`; run `VerifyReference`
      only when the reference was actually loaded (embed-only logs "embed-only, no reference").
    - Process routing (line ~338): `industry || embed` → `ProcessIndustryStream` (with
      `cfg.EmbedOnly = (mode == "embed")`, `ref`/`protos` nil for embed).
    - Write block: for `embed`, write **only** `embeddings.parquet` to `outDir`; skip the
      domains/industries/page_signals/tech writes.

### 2. cc-crawl `-mode embed`
```
cc-crawl -mode embed -parts 0-99 -crawl CC-MAIN-2026-25
```
Loops parts; per part:
- **worklist** = the industry worklist `shard_industry_<p>.parquet` (embed uses the same primary-page
  list). Reuse if present — **done parts already have it, so no rebuild** — else build with
  `index_builder --mode industry`.
- **embed dir** = `data/embedding/out_industry_<p>` (sibling of `data/crawl`).
- **marker** = `<embed-dir>.loaded`.
- run `cc-enrich-worker embed --worklist shard_industry_<p> --crawl-id <crawl> --out <embed-dir>` +
  concurrency flags. **No load step.**
- gate the marker on exit 0 **and** `<embed-dir>/embeddings.parquet` exists.

**Touch points (`cc-crawl/main.go`):**
- mode validation (line ~94): allow `embed`.
- `wlMode := "industry"` when `mode == "embed"` (worklist naming + `index_builder --mode`).
- embed-dir path = `filepath.Join(filepath.Dir(data), "embedding", "out_industry_<p>")`.
- skip the load step (lines ~181–190) for embed; touch the marker straight after a good produce.
- `passArgs`: `embed` case returns the industry flags (`--concurrency`, `--embed-concurrency`).

## Resumability / idempotency
- Per-part marker `data/embedding/out_industry_<p>.loaded` → re-running skips done parts.
- Re-running a part overwrites `embeddings.parquet`; the future Qdrant indexer dedupes on `root_domain`.
- The empty-input refusal still applies (a part that yields zero embeddings fails rather than writing
  an empty file).

## Cost (the one-time backfill)
- Re-embed the ~100 done parts ≈ **6.4M pages** re-fetched + re-embedded — unavoidable (no stored copy).
  At ~90–105 domains/s that's **~17–20 h of GPU**, one-time.
- Storage: 6.4M × 16 KB fp32 ≈ **~103 GB** under `data/embedding/` (→ S3).

## Usage
```bash
make                                                          # builds worker + cc-crawl
./cc-crawl/bin/cc-crawl -mode embed -parts 0-99 -crawl CC-MAIN-2026-25   # backfill, no CH writes
```

## Out of scope (separate, later)
- Qdrant indexer over `data/embedding/` (see `embeddings-design.md`).
- Denormalizing `page_type` / `content_languages` into `EmbeddingRow`.
- The dim/dtype eval (the backfill writes raw fp32; compact forms derive on CPU).

## Open decisions
1. **`embed --out` semantics** — embed dir directly (recommended) vs deriving `data/embedding/` from a
   `data/crawl/` `--out` like the industry pass. Direct is simpler and avoids the empty-dir check
   colliding with the existing `data/crawl/out_industry_<p>`.
2. **cc-crawl `data` layout** — keep `data` = `data/crawl` and compute the sibling `data/embedding`
   (recommended), vs. promoting `data` to the parent with `crawl/` + `embedding/` under it.
