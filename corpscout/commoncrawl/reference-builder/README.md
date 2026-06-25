# cc-reference-builder

Builds the CommonCrawl **reference embeddings** the Go `cc-enrich-worker` loads to classify.
Standalone (no dagster); shares the embedding endpoint with the worker.

## What it does

1. Reads NACE source text from **`corpscout.nace_categories`** (populated by the dagster `nace`
   source) — one row per current non-section code, with the section/parent hierarchy text.
2. Embeds it + the committed **page-type seed** (`reference_builder/seeds/page_type_exemplars.jsonl`)
   via the env endpoint (`COMMONCRAWL_EMBED_*`).
3. **Atomically swaps** both reference tables (stage table + `EXCHANGE` — no empty window):
   - `corpscout.nace_category_embeddings` (migration 000044)
   - `corpscout.page_type_exemplars` (migration 000045)

The schema is owned by the migrations; this only replaces row data. The `*_COLUMNS` tuples are
pinned to the migration column order by a contract test.

## Why standalone (not a dagster asset)

The reference build is part of the CommonCrawl pipeline, not the country-registry batch
ingestion dagster runs. It reads `corpscout.nace_categories` *from ClickHouse* — a one-way,
decoupled read — so dagster's only role is to populate that source table.

## The one invariant

Run this **with the same `COMMONCRAWL_EMBED_MODEL` / `COMMONCRAWL_EMBED_BASE_URL` the worker
uses** — the reference vectors and the page vectors must come from the same model, or cosine
scores are meaningless. Swap the model → re-run this **and** re-run the worker. (Both read the
shared `commoncrawl/.env`.)

## Usage

```bash
cp ../.env.example ../.env && edit it      # COMMONCRAWL_EMBED_* + CLICKHOUSE_*
set -a; . ../.env; set +a
uv run python -m reference_builder          # rebuilds both reference tables
```
Run from a host that reaches BOTH the embedding endpoint and ClickHouse.

## Test

```bash
uv run --with pytest --with numpy pytest tests/    # offline: fake embedder + fake CH client
```
