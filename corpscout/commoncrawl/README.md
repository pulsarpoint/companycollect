# commoncrawl

Global domain enrichment from [CommonCrawl](https://commoncrawl.org). For every domain in a crawl
it produces **two independent things**:

- **An industry category** — a NACE code per domain (+ confidence), by embedding the page and
  matching it against the NACE taxonomy. → `commoncrawl_domains`
- **Technologies & company info** — the tech stack (Wappalyzer), company identifiers (LEI / VAT →
  registry join keys), and a firmographic profile (schema.org JSON-LD). → `commoncrawl_technologies`,
  `commoncrawl_company_identifiers`, `commoncrawl_company_profile`

These are **two separate workflows** (this README is organised around that split — §3 and §4). They
share the fetch path but otherwise differ completely: industry is GPU/embedding-bound and needs a
prebuilt reference; tech is CPU-bound and needs only S3. Run them as separate processes.

Self-contained: pure Go + Python, **no dagster**. The only upstream dependency is one ClickHouse
table (`corpscout.nace_categories`, populated by dagster) that the industry workflow reads.

This is the single doc for all three packages — the worker (Go) and the two Python helpers.

---

## 1. Components

| Dir | Lang | Role |
|---|---|---|
| `cc-enrich-worker/` | Go | The **processor**. Subcommand CLI `cc-enrich-worker <industry\|tech\|both>`; byte-range-fetches WARC pages, runs the chosen workflow → Parquet → ClickHouse. Reference §6. |
| `index-builder/` | Python (duckdb, pyarrow) | Builds **worklists** from the CommonCrawl URL index — the "what to fetch" list. Two shapes: `industry` (1 page/domain), `tech` (many). Reference §7. |
| `reference-builder/` | Python (numpy, openai, clickhouse) | Builds the **reference embeddings** (industry only) — NACE categories + page-type seed → `nace_category_embeddings`/`page_type_exemplars`. Reference §8. |

The ClickHouse **schema** lives in `../clickhouse/migrations/` (golang-migrate), not in any package —
code reads/writes tables whose DDL the migrations own; the worker never issues DDL.

---

## 2. Setup (shared, do once)

From `corpscout/` unless noted.

**a. Apply the ClickHouse schema**
```bash
make clickhouse-migrate-up     # 044/045 reference tables + 046–053 result tables
```

**b. Fill in the shared config** (`commoncrawl/.env`, copied from `.env.example`). It is read by the
worker **and** reference-builder, so both hit the same model:
```bash
# Embedding endpoint (industry/both) — OpenAI-compatible, e.g. vLLM serving Qwen3-Embedding
COMMONCRAWL_EMBED_BASE_URL=http://<gpu-host>:8000/v1
COMMONCRAWL_EMBED_MODEL=                 # optional; empty => auto-detect from /v1/models
COMMONCRAWL_EMBED_API_KEY=x
COMMONCRAWL_EMBED_MAX_CHARS=2000

# ClickHouse (industry/both read the reference; the driver loads outputs)
CLICKHOUSE_HOST=companycollect
CLICKHOUSE_NATIVE_PORT=9002
CLICKHOUSE_USER=...  CLICKHOUSE_PASSWORD=...  CLICKHOUSE_DATABASE=corpscout

# Signed S3 (off-AWS: any IAM user's keys; CommonCrawl is Open Data → reads are free)
AWS_ACCESS_KEY_ID=AKIA...  AWS_SECRET_ACCESS_KEY=...  AWS_REGION=us-east-1
```
Load with `set -a; source ../.env; set +a` (exports every var; `env | grep` to verify — `echo`
shows set-but-unexported vars and lies). `AWS_REGION` is the only place the S3 region is set (the
CommonCrawl bucket is permanently `us-east-1`); there is no region flag.

**c. Build the worker**
```bash
cd commoncrawl/cc-enrich-worker && CGO_ENABLED=0 go build -o cc-enrich-worker .   # pure static binary
# ARM (Graviton): GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o cc-enrich-worker .
# or: docker build -t cc-enrich-worker .   (distroless image, see Dockerfile)
```

**d. Worklists — the worker's input (`--worklist`).** Every pass consumes a *worklist shard*: a
Parquet file with one row per page to fetch (`root_domain, url, warc_filename, warc_record_offset,
warc_record_length, content_languages`), built by `index-builder` from the CommonCrawl URL index.
The crawl's index is ~300 parts; one part → one shard. **`run_crawl.sh` (§5) builds them
automatically per shard**, so you usually don't touch this. To build one by hand:
```bash
cd commoncrawl/index-builder
uv run python -m index_builder --crawl CC-MAIN-2026-25 --list                                     # how many parts
uv run python -m index_builder --crawl CC-MAIN-2026-25 --mode industry --part 0 --out i0.parquet  # 1 page/domain
uv run python -m index_builder --crawl CC-MAIN-2026-25 --mode tech     --part 0 --out t0.parquet  # many pages/domain
```
Then e.g. `cc-enrich-worker industry --worklist i0.parquet --crawl-id CC-MAIN-2026-25`. Full
index-builder flags in §7.

---

## 3. Workflow A — categorize domains by industry  (`industry`)

**Goal:** a NACE industry code per domain. **Needs:** the reference embeddings + the embedding
endpoint + ClickHouse. **Bottleneck:** the GPU. **Output:** `commoncrawl_domains`.

```
nace_categories (dagster) ─► reference-builder ─► nace_category_embeddings + page_type_exemplars
                                                            │  loaded + model-checked at startup
index-builder --mode industry (1 page/domain) ─► worker industry ─► embed page ─► cosine-match NACE
                                                                                   ─► commoncrawl_domains
```

### A1. Build the reference embeddings — REQUIRED first
The worker matches each page against a fixed NACE matrix; build it (and refresh whenever you change
the served model — page and reference vectors **must** come from the same model):
```bash
cd commoncrawl/reference-builder
set -a; . ../.env; set +a
uv run python -m reference_builder      # writes nace_category_embeddings + page_type_exemplars
```

### A2. Verify the reference
The industry pass **checks this at startup** and **refuses to run** on a mismatch — full category
coverage, a single `(embedding_model, embedding_dim)`, that the model equals `COMMONCRAWL_EMBED_MODEL`,
and that the dim equals what the endpoint produces. Success logs:
```
reference check OK: 1024/1024 categories, model=Qwen/Qwen3-Embedding-8B dim=4096
```
By hand (`clickhouse-client --database corpscout`):
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
confidence gate. Junk/parked pages are tagged via `page_type` and skip NACE.

### A4. Output — `commoncrawl_domains` (one row/domain)
`nace_code/label/division`, `nace_confident` (binary), `nace_confidence` (0–1), `nace_margin`,
`nace_top3_*`, `page_type`(+score), contacts (`emails[]`).

---

## 4. Workflow B — resolve technologies & company info  (`tech`)

**Goal:** from a domain's pages, resolve its tech stack + identifiers + firmographic profile.
**Needs:** S3 only (no GPU, no reference, no endpoint). **Bottleneck:** the CPU. **Output:**
`commoncrawl_technologies`, `commoncrawl_company_identifiers`, `commoncrawl_company_profile`.

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

The tech matcher is a fast path (`--tech-engine fast`, default): a pure-Go Aho-Corasick pre-filter
runs only the Wappalyzer regexes whose literal appears in the page (~4.6× faster, a strict superset
of upstream). `--tech-engine wappalyzer` runs the upstream full scan.

### B2. Run it
```bash
./run_crawl.sh tech 0-0          # MAX_PAGES=25 by default
```
The worker fetches each domain's pages and, per page, runs the tech matcher + LEI/VAT extractors +
the JSON-LD profile parser, **unioning results per domain**.

### B3. Output
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
cached/skipped if present), runs the worker pass, loads each output Parquet into ClickHouse via
`clickhouse-client`, and touches a `.loaded` marker. Re-running **skips completed shards**
(idempotent; ReplacingMergeTree dedupes). Run the two workflows as **separate processes** so tech
pegs the cores while industry feeds the GPU. Env tunables: `CRAWL`, `WHERE` (worklist SQL filter),
`MAX_PAGES`, `TECH_CONC`, `IND_CONC`, `EMBED_CONC`.

---

## 6. Reference — `cc-enrich-worker` CLI

Subcommand-based: `cc-enrich-worker <industry|tech|both> [flags]`. The command **is** the workflow;
mode-specific flags appear only under their command (`cc-enrich-worker tech -h`). `both` runs both in
one fetch pass (discouraged — co-locating throttles the GPU; prefer two processes).

**Common to every command:**
| flag | default | meaning |
|---|---|---|
| `--worklist` | *(required)* | worklist Parquet shard (§2d) |
| `--crawl-id` | *(required)* | e.g. `CC-MAIN-2026-25`; stamped on every row |
| `--out` | `out` | output prefix → `<out>-domains.parquet`, `<out>-tech.parquet`, … |
| `--concurrency` | `32` | fetch/parse goroutines (push to ~128 to hide off-AWS fetch RTT) |
| `--chunk` | `1024` | domains per fetch+process chunk (lower = earlier GPU traffic) |
| `--s3-anonymous` | `false` | fetch via the HTTPS CDN instead of signed S3 — **rate-limits**, avoid for bulk |
| `--s3-bucket` / `--s3-prefix` | — | optionally upload outputs to S3 (in-AWS path) |

**`industry` (and `both`):** `--embed-concurrency` (96; in-flight embed requests), `--embed-batch`
(16; texts/request — keep small, big batches overflow the engine's token budget).

**`tech` (and `both`):** `--tech-engine` (`fast`|`wappalyzer`), `--tech-max-bytes` (131072; cap body
fed to Wappalyzer, 0 = full body).

**Fetch path:** **signed S3** (AWS creds) is the only reliable bulk source — the anonymous S3 API is
denied, and the `data.commoncrawl.org` HTTPS CDN (`--s3-anonymous`) works but is latency-bound and
rate-limits. Reads are free (Open Data). The worker streams each page through memory and discards it
(no on-disk page cache); it does **not** write to ClickHouse — the driver loads the Parquet.

---

## 7. Reference — `index-builder` CLI

Pure duckdb + pyarrow (no dagster). Resolves exact index-part URLs from CommonCrawl's HTTPS manifest
(anonymous S3 LIST is denied, so no globs). One part → one shard, cached at
`data/commoncrawl/index/<crawl>/shard_<mode>_<part>.parquet`, skipped if present.

```bash
python -m index_builder --crawl CC-MAIN-2026-25 --list                       # how many parts (~300)
python -m index_builder --crawl CC-MAIN-2026-25 --part 0                      # industry (default), cache
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --part 0          # tech (≤25 pages/domain)
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --max-pages 0 --part 0   # every HTML page
python -m index_builder --crawl CC-MAIN-2026-25 --parts 0-9                   # a range, skip cached
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --out shard0.parquet # explicit path (bypass cache)
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --where "content_languages like '%eng%'"
```
| flag | default | meaning |
|---|---|---|
| `--crawl` | `CC-MAIN-2026-25` | crawl id |
| `--mode` | `industry` | `industry` (1 page/domain) \| `tech` (many) |
| `--max-pages` | `25` | tech only: max pages/domain (`0` = every 200/HTML page = ~3B, expensive) |
| `--part` / `--parts` | — | single 0-based part, or inclusive range `A-B` |
| `--list` | — | print the part count and exit |
| `--where` | *(empty = all)* | extra SQL filter on the index rows |
| `--out` | *(cache)* | explicit output path (single part) instead of the cache |

Both modes pick each domain's **main-site homepage** (apex/`www`, shallowest path) over functional
subdomains (`shop.`/`blog.`/`api.`); tech additionally keeps the legal/contact/about pages (§4 B1).

---

## 8. Reference — `reference-builder`

Pure numpy + openai + clickhouse-driver (no dagster). Reads NACE source text from
`corpscout.nace_categories`, embeds it + the committed page-type seed via the env endpoint
(`COMMONCRAWL_EMBED_*`, same as the worker), and **atomically swaps** both reference tables
(stage + `EXCHANGE`, no empty window). Schema owned by migrations 044/045; this only replaces rows.
Usage in §3 A1; run it on a host that reaches both the endpoint and ClickHouse. **Re-run after any
model change** so the reference matches the vectors the worker produces.

---

## 9. Output tables & coverage queries

Schema in `../clickhouse/migrations/`:

| Table | Migration | Workflow | Grain |
|---|---|---|---|
| `commoncrawl_domains` | 046 (+049) | A | one row / domain — industry + contacts |
| `commoncrawl_technologies` | 047 | B | one row / (domain, technology) |
| `commoncrawl_company_identifiers` | 051 | B | one row / (domain, identifier) |
| `commoncrawl_company_profile` | 053 | B | one row / domain |
| `nace_category_embeddings` / `page_type_exemplars` | 044 / 045 | A (reference) | written by reference-builder, read by the worker |

After a run, these decide what to build next:
```sql
-- B: identifier hit-rate (worth more id_types? VAT vs LEI coverage?)
SELECT id_type, count() rows, uniqExact(root_domain) domains, sum(valid) valid
FROM commoncrawl_company_identifiers GROUP BY id_type;

-- B: JSON-LD profile coverage
SELECT count() FROM commoncrawl_company_profile WHERE name != '';

-- A: confidence distribution → size of the low-confidence LLM-escalation tail
SELECT round(nace_confidence, 1) conf, count()
FROM commoncrawl_domains GROUP BY conf ORDER BY conf;
```

---

## 10. Operational notes

- **Model-match invariant (workflow A)** — reference and worker must use the same
  `COMMONCRAWL_EMBED_MODEL`/`BASE_URL` (both read `commoncrawl/.env`; the startup check enforces it).
  Swap the model → rebuild the reference (A1) **and** re-run the industry pass.
- **Resumable** — `run_crawl.sh` skips shards with a `.loaded` marker; a partial shard re-runs
  cleanly. The worker refuses to replace a table on empty input.
- **Identifier/profile provenance (workflow B)** — rows carry `source_url =` the domain's first
  fetched page, not the exact page a given LEI/VAT was found on. `root_domain` (the join key) is
  unaffected; per-page provenance is a future change.
- **No DDL in code** — migrations own the schema; the worker loads Parquet via `clickhouse-client`.

---

## 11. Build, test & code layout

```bash
cd cc-enrich-worker     && go test ./... && CGO_ENABLED=0 go build -o cc-enrich-worker .
cd ../index-builder     && uv run --with pytest --with duckdb --with pyarrow pytest tests/
cd ../reference-builder && uv run --with pytest --with numpy pytest tests/
```

The Go worker is `main.go` (entry point: flags, env, ClickHouse/embedder/getter wiring, the chunked
process loop) + `internal/` packages: `model` (shared structs), `vec` (math), `fetch` (WARC fetch),
`embed` (embedding client), `parse` (HTML→text), `tech` (Wappalyzer + Aho-Corasick), `extract`
(LEI/VAT/profile), `classify` (NACE reference + classify + confidence + startup check), `output`
(Parquet + S3), `worker` (`ProcessShard` orchestration).
