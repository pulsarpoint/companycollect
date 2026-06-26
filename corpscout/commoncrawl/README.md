# commoncrawl

Global domain enrichment from [CommonCrawl](https://commoncrawl.org). For every domain in a crawl
it produces **two independent things**:

- **An industry category** — a NACE code per domain (+ confidence), by embedding the page and
  matching it against the NACE taxonomy. → `commoncrawl_domains`
- **Technologies & company info** — the tech stack (Wappalyzer), company identifiers (LEI / VAT →
  registry join keys), and a firmographic profile (schema.org JSON-LD). → `commoncrawl_technologies`,
  `commoncrawl_company_identifiers`, `commoncrawl_company_profile`

These are **two separate workflows** (this README is organised around that split — §3 and §4). They
share the fetch infrastructure but otherwise have nothing in common: industry is GPU/embedding-bound
and needs a prebuilt reference; tech is CPU-bound and needs only S3. Run them as separate processes.

Self-contained: pure Go + Python, **no dagster**. The only upstream dependency is one ClickHouse
table (`corpscout.nace_categories`) that dagster populates; the industry workflow reads it.

---

## 1. Components

| Dir | Lang | Role |
|---|---|---|
| [`index-builder/`](index-builder/README.md) | Python (duckdb, pyarrow) | Build **worklists** from the CommonCrawl URL index — the "what to fetch" list (WARC file + byte offset per page). Two shapes: `industry` (1 page/domain) and `tech` (many pages/domain). |
| [`reference-builder/`](reference-builder/README.md) | Python (numpy, openai, clickhouse) | Build the **reference embeddings** (industry workflow only) — embed NACE categories + the page-type seed → `nace_category_embeddings` / `page_type_exemplars`. What the worker matches pages against. |
| [`cc-enrich-worker/`](cc-enrich-worker/README.md) | Go | The **processor**. Subcommand CLI: `cc-enrich-worker <industry\|tech\|both>`. Byte-range-fetches WARC pages and runs the chosen workflow → Parquet → ClickHouse. |
| `.env` | — | Shared config: embedding endpoint (`COMMONCRAWL_EMBED_*`), ClickHouse, AWS. Read by the worker **and** reference-builder so both use the **same model**. Copy `.env.example`. |

The ClickHouse **schema** lives in `../clickhouse/migrations/` (golang-migrate), not in any of these
packages — code reads/writes tables whose DDL the migrations own.

---

## 2. Setup (shared, do once)

Everything below is from `corpscout/` unless noted.

**a. Apply the ClickHouse schema**
```bash
make clickhouse-migrate-up     # 044/045 reference tables + 046–053 result tables
```

**b. Fill in the shared config**
```bash
cp commoncrawl/.env.example commoncrawl/.env
$EDITOR commoncrawl/.env        # COMMONCRAWL_EMBED_*, CLICKHOUSE_*, AWS_*
```

**c. Build the worker**
```bash
cd commoncrawl/cc-enrich-worker && CGO_ENABLED=0 go build -o cc-enrich-worker .
```

`cc-enrich-worker` is subcommand-based — the command **is** the workflow:
```
cc-enrich-worker industry [flags]    # workflow A (§3)
cc-enrich-worker tech     [flags]    # workflow B (§4)
cc-enrich-worker both     [flags]    # both in one fetch pass (discouraged — throttles the GPU)
```
Mode-specific flags appear only under their command (`cc-enrich-worker tech -h`).

> In practice you don't call the binary directly — **`run_crawl.sh` (§5)** generates the worklist,
> runs the pass, and loads ClickHouse, per shard. The two workflows below explain what each pass does.

---

## 3. Workflow A — categorize domains by industry  (`industry`)

**Goal:** assign each domain a NACE industry code. **Needs:** the reference embeddings + the
embedding endpoint + ClickHouse. **Bottleneck:** the GPU. **Output:** `commoncrawl_domains`.

```
nace_categories (dagster) ─► reference-builder ─► nace_category_embeddings + page_type_exemplars
                                                            │  loaded + model-checked at startup
index-builder --mode industry (1 page/domain) ─► worker industry ─► embed page ─► cosine-match NACE
                                                                                   ─► commoncrawl_domains
```

### A1. Build the reference embeddings — REQUIRED first
The worker matches each page against a fixed NACE matrix; build it (and refresh it whenever you
change the served model — page and reference vectors **must** come from the same model):
```bash
cd commoncrawl/reference-builder
set -a; . ../.env; set +a
uv run python -m reference_builder      # writes nace_category_embeddings + page_type_exemplars
```

### A2. Verify the reference
The industry pass **checks this at startup** (`cc-enrich-worker/check.go`) and **refuses to run** on
a mismatch — full category coverage, a single `(embedding_model, embedding_dim)`, that the model
equals `COMMONCRAWL_EMBED_MODEL`, and that the dim equals what the endpoint produces. Success logs:
```
reference check OK: 1024/1024 categories, model=Qwen/Qwen3-Embedding-8B dim=4096
```
To check by hand (`clickhouse-client --database corpscout`):
```sql
SELECT embedding_model, embedding_dim, uniqExact(code) AS embedded
FROM nace_category_embeddings FINAL GROUP BY 1, 2;            -- expect exactly ONE row

SELECT (SELECT uniqExact(code) FROM nace_category_embeddings) AS embedded,
       (SELECT uniqExact(code) FROM nace_categories
          WHERE is_current = 1 AND level != 'section' AND description_en != '') AS expected;  -- must be equal
```
If `embedded < expected` or the model/dim differ from your `.env`, **re-run A1**.

### A3. Run it
```bash
./run_crawl.sh industry 0-0      # or a range, e.g. 0-299
```
Per domain it fetches the **one** representative page (the main-site homepage, from the `industry`
worklist), embeds it, cosine-matches the NACE reference + page-type prototypes, and applies the
confidence gate (`nace_confident` binary + `nace_confidence` 0–1). Junk/parked pages are tagged via
`page_type` and skip NACE.

### A4. What lands in `commoncrawl_domains`
One row per domain: `nace_code/label/division`, `nace_confident`, `nace_confidence`, `nace_margin`,
`nace_top3_*`, `page_type`(+score), and contacts (`emails[]`).

---

## 4. Workflow B — resolve technologies & company info  (`tech`)

**Goal:** from a domain's pages, resolve its tech stack + company identifiers + firmographic
profile. **Needs:** S3 only (no GPU, no reference, no embedding endpoint). **Bottleneck:** the CPU.
**Output:** `commoncrawl_technologies`, `commoncrawl_company_identifiers`, `commoncrawl_company_profile`.

```
index-builder --mode tech (MANY pages/domain, multilingual legal-page ranking) ─► worker tech ─►
   Aho-Corasick-gated Wappalyzer    ─► commoncrawl_technologies
   LEI / VAT (checksum-validated)   ─► commoncrawl_company_identifiers   (→ GLEIF / country registries)
   schema.org Organization JSON-LD  ─► commoncrawl_company_profile
```

### B1. Why a different worklist than industry
Industry needs *one* representative page; tech wants **coverage**. Wappalyzer sees more of the stack
across pages, and — crucially — **LEI/VAT and the Organization JSON-LD live on `/imprint`, `/legal`,
`/contact`, `/about`, never the homepage**. So the `tech` worklist returns up to `MAX_PAGES`
(default 25) pages/domain, ranked **homepage → legal/contact/about → shallowest**, multilingually
(`impressum`, `mentions-legales`, `quienes-somos`, `chi-siamo`, `uber-uns`, …) so non-English sites
aren't missed. `MAX_PAGES=0` = every HTML page (the whole crawl, ~3B pages — expensive).

### B2. Run it
```bash
./run_crawl.sh tech 0-0          # or a range; MAX_PAGES=25 by default
```
The worker fetches each domain's pages and, per page, runs the fast tech matcher + the LEI/VAT
extractors + the JSON-LD profile parser, **unioning results per domain**.

### B3. What lands in ClickHouse
- `commoncrawl_technologies` — one row / (domain, technology): `technology`, `category`, `version`.
- `commoncrawl_company_identifiers` — one row / (domain, identifier): `id_type` (`lei`/`vat`/`tax`/
  `duns`/`naics`), `id_value`, `valid` (checksum), `source` (`jsonld`/`text`). Join `WHERE
  id_type='lei'` to `gleif_reference_data`; `vat` → the country company tables.
- `commoncrawl_company_profile` — one row / domain: `name`, `country`, `founding_year`,
  `employee_count`, `email`, `phone`, `same_as[]` (LinkedIn/Wikidata/…).

---

## 5. The driver (`run_crawl.sh`) — runs either workflow, resumable

```bash
cd commoncrawl/cc-enrich-worker
set -a; source ../.env; set +a
./run_crawl.sh tech     0-299      # workflow B over shards 0..299 (CPU pass)
./run_crawl.sh industry 0-299      # workflow A over shards 0..299 (GPU pass)
```
For each shard it: generates the **per-mode** worklist on demand (`shard_<mode>_<part>.parquet`,
cached/skipped if present), runs the worker pass, loads each output Parquet into ClickHouse, and
touches a `.loaded` marker. Re-running **skips completed shards** (idempotent; ReplacingMergeTree
dedupes). Run the two workflows as **separate processes** so tech pegs the cores while industry
feeds the GPU. Tunables via env: `CRAWL`, `WHERE`, `MAX_PAGES`, `TECH_CONC`, `IND_CONC`, `EMBED_CONC`.

---

## 6. Output tables & coverage queries

Schema in `../clickhouse/migrations/` (the migrations own the DDL; the worker never issues DDL):

| Table | Migration | Workflow | Grain |
|---|---|---|---|
| `commoncrawl_domains` | 046 (+049) | A | one row / domain — industry + contacts |
| `commoncrawl_technologies` | 047 | B | one row / (domain, technology) |
| `commoncrawl_company_identifiers` | 051 | B | one row / (domain, identifier) |
| `commoncrawl_company_profile` | 053 | B | one row / domain |
| `nace_category_embeddings` / `page_type_exemplars` | 044 / 045 | A (reference) | written by reference-builder, read by the worker |

After a run, these numbers decide what to build next:
```sql
-- B: identifier hit-rate (worth adding more id_types? VAT vs LEI coverage?)
SELECT id_type, count() rows, uniqExact(root_domain) domains, sum(valid) valid
FROM commoncrawl_company_identifiers GROUP BY id_type;

-- B: JSON-LD profile coverage
SELECT count() FROM commoncrawl_company_profile WHERE name != '';

-- A: confidence distribution → how big is the low-confidence LLM-escalation tail?
SELECT round(nace_confidence, 1) conf, count()
FROM commoncrawl_domains GROUP BY conf ORDER BY conf;
```

---

## 7. Operational notes

- **Model-match invariant (workflow A)** — the reference and the worker must use the same
  `COMMONCRAWL_EMBED_MODEL`/`BASE_URL` (both read `commoncrawl/.env`; the startup check enforces it).
  Swap the model → rebuild the reference (A1) **and** re-run the industry pass.
- **Resumable** — `run_crawl.sh` skips shards with a `.loaded` marker; a partial shard re-runs
  cleanly. The worker refuses to replace a table on empty input.
- **Identifier/profile provenance (workflow B)** — rows carry `source_url =` the domain's first
  fetched page, not the exact page a given LEI/VAT was found on. The `root_domain` join key is
  unaffected; per-page provenance is a future change.
- **No DDL in code** — migrations own the schema; the worker loads Parquet via `clickhouse-client`.

## 8. Build & test
```bash
cd cc-enrich-worker     && go test ./... && CGO_ENABLED=0 go build -o cc-enrich-worker .
cd ../index-builder     && uv run --with pytest --with duckdb --with pyarrow pytest tests/
cd ../reference-builder && uv run --with pytest --with numpy pytest tests/
```
