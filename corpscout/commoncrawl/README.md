# commoncrawl

Global domain enrichment from [CommonCrawl](https://commoncrawl.org): every domain in a crawl →
**NACE industry** (+ confidence), **technology stack**, **contacts**, **company identifiers**
(LEI / VAT → registry join keys), and a **company profile** (firmographics from schema.org
JSON-LD). Self-contained: pure Go + Python, **no dagster**. The only upstream dependency is one
ClickHouse table (`corpscout.nace_categories`) that dagster populates.

> If you only read one thing: **build the reference embeddings before you run the worker**
> (§3 step 4). The industry pass refuses to start otherwise.

---

## 1. Components

| Dir | Lang | Role |
|---|---|---|
| [`index-builder/`](index-builder/README.md) | Python (duckdb, pyarrow) | Build **worklists** from the CommonCrawl URL index — the "what to fetch" list (WARC file + byte offset per page). Two shapes: `industry` (1 page/domain) and `tech` (many). |
| [`reference-builder/`](reference-builder/README.md) | Python (numpy, openai, clickhouse) | Build the **reference embeddings** — embed NACE categories + the page-type seed → `corpscout.nace_category_embeddings` / `page_type_exemplars`. The thing the worker matches pages against. |
| [`cc-enrich-worker/`](cc-enrich-worker/README.md) | Go | The **processor**: byte-range-fetch WARC pages → classify (industry) / Wappalyzer + identifiers + profiles (tech) → Parquet → ClickHouse. |
| `.env` | — | Shared config: the embedding endpoint (`COMMONCRAWL_EMBED_*`), ClickHouse, AWS. Read by the worker **and** the reference-builder so both use the **same model**. Copy `.env.example`. |

The ClickHouse **schema** lives in `../clickhouse/migrations/` (golang-migrate), not in any of these
packages — code reads/writes tables whose DDL the migrations own.

---

## 2. How it fits together

```
 dagster_v3 ──(populates)──► corpscout.nace_categories         [the only upstream dependency]
                                      │
        reference-builder ───────────┘   embed NACE + page-type seed (COMMONCRAWL_EMBED_*)
              │
              ▼
   corpscout.nace_category_embeddings (044)   corpscout.page_type_exemplars (045)
              │                                        │
              └──────────────► loaded at startup ◄─────┘
                                      │
 CommonCrawl URL index ──► index-builder ──► worklist shards ──► cc-enrich-worker ──► ClickHouse
   (cc-index parquet)        (industry|tech)   (WARC locators)     (fetch + enrich)     (results)
```

- **dagster's only role**: produce `corpscout.nace_categories`. The reference-builder reads it
  *from ClickHouse* — one-way, decoupled.
- The reference embeddings and the page embeddings **must come from the same model** (matching
  vector space). The shared `.env` + the worker's startup check enforce this.

---

## 3. Run order (first run on a fresh box)

Everything below is from `corpscout/` unless noted.

**1. Apply the ClickHouse schema**
```bash
make clickhouse-migrate-up          # creates 044/045 reference tables + 046–053 result tables
```

**2. Fill in the shared config**
```bash
cp commoncrawl/.env.example commoncrawl/.env
$EDITOR commoncrawl/.env            # COMMONCRAWL_EMBED_BASE_URL + _MODEL, CLICKHOUSE_*, AWS_*
```

**3. Bring up the embedding endpoint** (OpenAI-compatible, e.g. vLLM serving Qwen3-Embedding)
and make sure `COMMONCRAWL_EMBED_BASE_URL` points at it.

**4. Build the reference embeddings** — REQUIRED before the industry pass
```bash
cd commoncrawl/reference-builder
set -a; . ../.env; set +a
uv run python -m reference_builder         # nace_category_embeddings + page_type_exemplars
```

**5. Verify the reference** (§4).

**6. Build the worker + run the passes** (resumable driver, from `cc-enrich-worker/`)
```bash
cd ../cc-enrich-worker
CGO_ENABLED=0 go build -o cc-enrich-worker .
./run_crawl.sh tech     0-0        # CPU-bound: Wappalyzer + identifiers + profiles (many pages/domain)
./run_crawl.sh industry 0-0        # GPU-bound: embed homepage → NACE classify (1 page/domain)
```
`run_crawl.sh` generates the per-mode worklist on demand, runs the pass, loads the output Parquet
into ClickHouse, and marks the shard done. Re-running skips completed shards (idempotent;
ReplacingMergeTree dedupes). Run the two modes as **separate processes** (tech pegs the cores,
industry feeds the GPU).

---

## 4. Verifying the NACE reference embeddings

The worker checks this **automatically at industry startup** (`cc-enrich-worker/check.go`) and
**refuses to run** on a mismatch — coverage, a single `(embedding_model, embedding_dim)`, that the
model equals `COMMONCRAWL_EMBED_MODEL`, and that the dim equals what the endpoint produces. Look for:
```
reference check OK: 1024/1024 categories, model=Qwen/Qwen3-Embedding-8B dim=4096
```
or it aborts with `reference check failed: …`.

To check by hand (`clickhouse-client --database corpscout`):
```sql
-- one model + dim, and how many categories are embedded
SELECT embedding_model, embedding_dim, uniqExact(code) AS embedded
FROM nace_category_embeddings FINAL GROUP BY 1, 2;

-- coverage: embedded vs the categories that SHOULD be embedded (must be equal)
SELECT
  (SELECT uniqExact(code) FROM nace_category_embeddings)                         AS embedded,
  (SELECT uniqExact(code) FROM nace_categories
     WHERE is_current = 1 AND level != 'section' AND description_en != '')       AS expected;

-- page-type prototypes present?
SELECT count() FROM page_type_exemplars FINAL;
```
If `embedded < expected`, or the model/dim differ from your `.env`, **re-run the reference-builder**
(step 4). Always rebuild it after changing the served model — page vectors and reference vectors
must match.

---

## 5. The two passes

| Pass | Worklist (`index-builder --mode`) | Reads | Produces (ClickHouse) |
|---|---|---|---|
| **industry** | `industry` — 1 homepage/domain | reference tables + embed endpoint + S3 | `commoncrawl_domains` (NACE code/label/division, `nace_confident`, `nace_confidence`, page_type, emails) |
| **tech** | `tech` — many pages/domain (≤`MAX_PAGES`, default 25) | S3 only | `commoncrawl_technologies`, `commoncrawl_company_identifiers` (LEI/VAT/…), `commoncrawl_company_profile` |

Why two worklists: industry embeds **one** representative page (the homepage); tech wants
**coverage** — Wappalyzer sees more stacks, and **LEI/VAT + JSON-LD profiles live on `/imprint`,
`/legal`, `/contact`, `/about` — never the homepage**. The tech worklist ranks homepage → legal/
contact pages → shallowest, so the per-domain cap keeps the pages that carry identifiers.
`MAX_PAGES=0` = every HTML page (the whole crawl, ~3B pages — expensive; the default 25 captures
~all tech + the legal pages).

---

## 6. Output tables (schema in `../clickhouse/migrations/`)

| Table | Migration | Grain |
|---|---|---|
| `commoncrawl_domains` | 046 (+049 `nace_confidence`) | one row / domain — industry + contacts |
| `commoncrawl_technologies` | 047 | one row / (domain, technology) |
| `commoncrawl_company_identifiers` | 051 | one row / (domain, identifier) — `id_type` lei/vat/… → join `gleif_reference_data` / country registries |
| `commoncrawl_company_profile` | 053 | one row / domain — name, country, founding, employees, sameAs |
| `nace_category_embeddings` / `page_type_exemplars` | 044 / 045 | the reference (written by reference-builder, read by the worker) |

### Coverage queries (after a run — these decide what to build next)
```sql
SELECT id_type, count() rows, uniqExact(root_domain) domains, sum(valid) valid
FROM commoncrawl_company_identifiers GROUP BY id_type;            -- LEI/VAT hit rate

SELECT count() FROM commoncrawl_company_profile WHERE name != ''; -- JSON-LD profile coverage

SELECT round(nace_confidence, 1) conf, count()
FROM commoncrawl_domains GROUP BY conf ORDER BY conf;             -- confidence distribution → LLM-tail size
```

---

## 7. Operational notes

- **Model-match invariant** — the reference and the worker must use the same
  `COMMONCRAWL_EMBED_MODEL`/`BASE_URL`. Both read `commoncrawl/.env`; the startup check enforces it.
  Swap the model → rebuild the reference (§3.4) **and** re-run the worker.
- **Resumable** — `run_crawl.sh` skips shards whose `.loaded` marker exists; a partial shard
  re-runs cleanly (ReplacingMergeTree dedupes). Refuses to replace a table on empty input.
- **Identifier/profile provenance** — rows currently carry `source_url =` the domain's first
  fetched page (homepage), not the exact page the LEI/VAT was found on. The `root_domain` join key
  is unaffected; per-page provenance is a future worker change.
- **No DDL in code** — migrations own the schema; the worker asserts tables exist and loads
  Parquet via `clickhouse-client`.

## 8. Build & test
```bash
# Go worker
cd cc-enrich-worker && go test ./... && CGO_ENABLED=0 go build -o cc-enrich-worker .
# Python packages (offline tests, fakes — no endpoint/CH needed)
cd ../index-builder    && uv run --with pytest --with duckdb --with pyarrow pytest tests/
cd ../reference-builder && uv run --with pytest --with numpy pytest tests/
```
