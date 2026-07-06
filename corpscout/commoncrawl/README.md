# commoncrawl

Global domain enrichment from [CommonCrawl](https://commoncrawl.org). For every domain in a crawl
it produces **two independent things**:

- **Industry** — NACE code(s) per domain, by embedding the page and matching the NACE taxonomy.
  → `commoncrawl_industries` (many rows/domain) + `commoncrawl_page_signals`
- **Technologies & contacts** — the tech stack (Wappalyzer), registry identifiers (LEI / VAT →
  join keys), self-reported metadata (schema.org JSON-LD), and contacts. → `commoncrawl_technologies`,
  `commoncrawl_domain_identifiers`, `commoncrawl_domain_metadata`, `commoncrawl_domain_contact_info`

Both write the identity master `commoncrawl_domains` (one row/domain). Full schema: `docs/schema.md`.

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
| `cc-crawl/` | Go | The **orchestrator** (replaces `run_crawl.sh`). Drives the per-part loop: worklist → produce → load, structured JSON logs. Reference §5, design `docs/cc-crawl-design.md`. |
| `cc-enrich-worker/` | Go | The **processor**. Subcommand CLI `cc-enrich-worker <industry\|tech\|both>` + `load`; byte-range-fetches WARC pages, runs the chosen workflow → Parquet, and (separately) loads it. Reference §6. |
| `index-builder/` | Python (duckdb, pyarrow) | Builds **worklists** from the CommonCrawl URL index — the "what to fetch" list. Two shapes: `industry` (1 page/domain), `tech` (many). Reference §7. |
| `reference-builder/` | Python (numpy, openai, clickhouse) | Builds the **reference embeddings** (industry only) — NACE categories + page-type seed → `nace_category_embeddings`/`page_type_exemplars`. Reference §8. |

The ClickHouse **schema** lives in `../clickhouse/migrations/` (golang-migrate), not in any package —
code reads/writes tables whose DDL the migrations own; the worker never issues DDL.

---

## 2. Setup (shared, do once)

From `corpscout/` unless noted.

**a. Apply the ClickHouse schema**
```bash
make clickhouse-migrate-up     # 044/045 reference tables + 046–068 result tables
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

**c. Build the binaries** — one `make` at `commoncrawl/` builds **both** Go binaries (gitignored `bin/`)
```bash
cd commoncrawl && make             # -> cc-enrich-worker/bin/cc-enrich-worker  +  cc-crawl/bin/cc-crawl
# make worker  /  make crawl       # build just one
# make vet  /  make test           # vet both / test the worker
# make -C cc-enrich-worker arm     # cross-compile the worker linux/arm64 (Graviton)
# or: docker build -t cc-enrich-worker cc-enrich-worker/   (distroless image, see Dockerfile)
```

**d. Worklists — the worker's input (`--worklist`).** Every pass consumes a *worklist shard*: a
Parquet file with one row per page to fetch (`root_domain, url, warc_filename, warc_record_offset,
warc_record_length, content_languages`), built by `index-builder` from the CommonCrawl URL index.
The crawl's index is ~300 parts; one part → one shard. **`cc-crawl` (§5) builds them automatically
per part**, so you usually don't touch this. To build one by hand:
```bash
cd commoncrawl/index-builder
uv run python -m index_builder --crawl CC-MAIN-2026-25 --list                                     # how many parts
uv run python -m index_builder --crawl CC-MAIN-2026-25 --mode industry --part 0   # -> ../data/index/<crawl>/shard_industry_0.parquet
uv run python -m index_builder --crawl CC-MAIN-2026-25 --mode tech     --part 0   # -> ../data/index/<crawl>/shard_tech_0.parquet
```
Shards land under **`commoncrawl/data/`** (gitignored — *never* in the code folders). Then e.g.
`./bin/cc-enrich-worker industry --worklist ../data/index/CC-MAIN-2026-25/shard_industry_0.parquet --crawl-id CC-MAIN-2026-25` (§5). Full
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
cc-crawl/bin/cc-crawl -mode industry -parts 0-0 -crawl CC-MAIN-2026-25   # or a range, e.g. 0-299
```
Per domain it fetches the **one** representative page (the main-site homepage, from the `industry`
worklist), embeds it, cosine-matches the NACE reference + page-type prototypes, and applies the
confidence gate. Junk/parked pages are tagged via `page_type` and skip NACE.

### A4. Output (multi-row split)
- `commoncrawl_industries` — **many rows/domain**, one per candidate NACE code: `nace_code/label/
  division`, `rank`, `is_primary`, `score`, `nace_method`.
- `commoncrawl_page_signals` — one row/domain: `page_type`(+score), `nace_confident`, `nace_margin`.
- `commoncrawl_domains` — the identity master row (also written by the tech pass).
- **`data/embedding/out_industry_<part>/embeddings.parquet`** (on disk, **not** ClickHouse) — the raw
  fp32 page vector + page metadata, saved on every industry run so it's never recomputed. Source of
  record for re-classification + similarity/Qdrant (see §5b, `docs/embeddings-design.md`).

---

## 4. Workflow B — resolve technologies & company info  (`tech`)

**Goal:** from a domain's pages, resolve its tech stack + identifiers + metadata + contacts.
**Needs:** S3 only (no GPU, no reference, no endpoint). **Bottleneck:** the CPU. **Output:**
`commoncrawl_technologies`, `commoncrawl_domain_identifiers`, `commoncrawl_domain_metadata`,
`commoncrawl_domain_contact_info`.

```
index-builder --mode tech (MANY pages/domain, multilingual legal-page ranking) ─► worker tech ─►
   Aho-Corasick-gated Wappalyzer    ─► commoncrawl_technologies
   LEI / VAT (checksum-validated)   ─► commoncrawl_domain_identifiers    (→ GLEIF / country registries)
   schema.org JSON-LD "about"       ─► commoncrawl_domain_metadata
   emails (regex) + phone + same_as ─► commoncrawl_domain_contact_info
```

### B1. Why a different worklist than industry
Industry needs *one* representative page; tech wants **coverage**. Wappalyzer sees more of the stack
across pages, and — crucially — **LEI/VAT and the Organization JSON-LD live on `/imprint`, `/legal`,
`/contact`, `/about`, never the homepage**. So the `tech` worklist returns up to `MAX_PAGES`
(default 25) pages/domain, ranked **homepage → legal/contact/about → shallowest**, multilingually
(`impressum`, `mentions-legales`, `quienes-somos`, `chi-siamo`, `uber-uns`, …) so non-English sites
aren't missed. `MAX_PAGES=0` = every HTML page (the whole crawl, ~3B pages — expensive).

Both tech engines detect the **same** technologies — the difference is *how* they evaluate the
fingerprints, not *how many*. `--tech-engine fast` (default) is a pure-Go Aho-Corasick pre-filter
that runs only the Wappalyzer regexes whose required literal actually appears in the page (a strict
**superset** of the library's output — nothing dropped — ~4.6× faster). `--tech-engine wappalyzer`
runs the unmodified `wappalyzergo` library, which evaluates every fingerprint unconditionally —
identical results, just slower; it exists only as the parity/reference baseline, not for bulk runs.

### B2. Run it
```bash
cc-crawl/bin/cc-crawl -mode tech -parts 0-0 -crawl CC-MAIN-2026-25   # MAX_PAGES=25 by default
```
The worker fetches each domain's pages and, per page, runs the tech matcher + LEI/VAT extractors +
the JSON-LD parser + email regex, **unioning results per domain**.

### B3. Output
- `commoncrawl_technologies` — one row / (domain, technology): `technology`, `category`, `version`.
- `commoncrawl_domain_identifiers` — one row / (domain, identifier): `id_type` (`lei`/`vat`/`tax`/
  `duns`/`naics`), `id_value`, `valid` (checksum), `source` (`jsonld`/`text`). Join `WHERE
  id_type='lei'` to `gleif_reference_data`; `vat` → the country company tables.
- `commoncrawl_domain_metadata` — one row / domain: `name`, `description`, `logo`, `country`,
  `founding_year`, `employee_count` (self-reported JSON-LD).
- `commoncrawl_domain_contact_info` — **many** rows / domain: `contact_type` (`email`/`phone`/
  `social`), `value`, `source` (`regex`/`jsonld`).

---

## 5. The driver (`cc-crawl`) — runs either workflow, resumable

`cc-crawl` (Go binary in `cc-crawl/`, the replacement for the old `run_crawl.sh`) drives the per-part
loop. Design doc: `docs/cc-crawl-design.md`. It only orchestrates (`os/exec`) — it does **not** import
the worker. Build both binaries with one `make` (§2c), then run from `commoncrawl/`:

```bash
cc-crawl/bin/cc-crawl -mode tech     -parts 0-299 -crawl CC-MAIN-2026-25   # workflow B (CPU pass)
cc-crawl/bin/cc-crawl -mode industry -parts 0-299 -crawl CC-MAIN-2026-25   # workflow A (GPU pass)
cc-crawl/bin/cc-crawl -mode embed    -parts 0-70  -crawl CC-MAIN-2026-25   # embed-only backfill (§5b)
```
Flags (`flag` package; each defaults from the same-named env var). **Required:** `-mode`
(`industry|tech|embed`), `-parts` (`lo-hi` or a single `N`), `-crawl` (no default). Optional: `-data`,
`-builder-dir`, `-worker`, `-max-pages`, `-ind-conc`, `-embed-conc`, `-tech-conc` (**domains** in
flight, default 32 — each domain fetches up to 8 pages in parallel, so total fetches = tech-conc × 8),
`-tech-chunk` (pages per worker chunk, default 16384 — chunks count *pages*, ~13/domain; keep it ≫
`tech-conc × 13` so the domain pool stays full).

For each part it: builds the **per-mode** worklist on demand (`shard_<mode>_<part>.parquet`, cached),
then runs **two separate worker executions** — (1) the **pass** that produces `out_<mode>_<part>/`,
then (2) **`load --dir`** into ClickHouse — and touches a `.loaded` marker. **The loader runs only if
the pass exited 0 and wrote `domains.parquet`**, so a bad produce is never loaded. Re-running **skips
completed parts** (idempotent; ReplacingMergeTree dedupes). Run the two workflows as **separate
processes** so tech pegs the cores while industry feeds the GPU.

**Logging:** structured **JSON** (`log/slog`) to **stdout AND** `data/logs/crawl_<mode>_<lo>-<hi>_<ts>.log`.
Each command logs its full args + exit code; each part ends with `domains_from_s3` / `rows_to_clickhouse`;
the run ends with a `done/skipped/failed` summary. Example:
```json
{"level":"INFO","msg":"done","mode":"industry","part":5,"domains_from_s3":102804,"rows_to_clickhouse":408019}
```

### 5b. Embed-only backfill (`-mode embed`)

The industry pass embeds each page (the expensive GPU artifact) and now **persists the raw vector** to a
separate tree `data/embedding/out_industry_<part>/embeddings.parquet` (fp32, one row/domain, with the
page metadata — `source_url` + the WARC re-fetch coords + `subdomain` + `text_len`). Vectors are **not**
loaded into ClickHouse (too large, incompressible); they're the source of record for cheap
re-classification + similarity/Qdrant later (see `docs/embeddings-design.md`).

`-mode embed` exists to **fill segments that have no stored vector** — backfilling parts classified before
vectors were persisted, or finishing parts the industry pass hasn't reached. It:
- **reuses the industry worklist** `shard_industry_<part>.parquet` (no index rebuild for done parts),
- runs a **single** embed exec — fetch → embed → write `embeddings.parquet` — with **no NACE classify,
  no ClickHouse, no load step**,
- writes to `data/embedding/out_industry_<part>/`, and is **idempotent via verify-and-skip** (there is **no
  `.loaded` marker** for embed — the vector file *is* the marker): the worker skips a part whose embed folder
  already holds a complete vector file under **either** name — `embeddings.parquet` (fp32) **or**
  `embeddings_fp16.parquet` (converted offline once the fp32 was pruned, see `embedding-tools/`). So a
  converted part is **not** re-embedded.

**To embed only the missing segments, just run the full part range** — every part that already has a vector
(fp32 *or* fp16) is skipped, so only the gaps are fetched + embedded:
```bash
cc-crawl/bin/cc-crawl -mode embed -parts 0-299 -crawl CC-MAIN-2026-25   # fills only the parts with no vector
# or one segment straight through the worker:
cc-enrich-worker embed --worklist data/crawl/shard_industry_43.parquet \
                       --crawl-id CC-MAIN-2026-25 --out data/embedding/out_industry_43
```
Each already-done part logs `skip: embeddings already present …`; the gaps show the normal fetch→embed
progress. Cost is just the re-fetch + re-embed of the gaps (no stored copy exists): ≈16 KB/domain fp32.
Future industry parts save their vectors on the first pass. See `docs/embed-only-mode-plan.md`.

### Re-running, forcing, and reloading

The **`.loaded` marker** (`out_<mode>_<part>.loaded`) is an **empty file** — cc-crawl checks only its
*existence*, never its contents — and it gates the **whole** part (both produce and load):

| You run | `.loaded` present? | produce (fetch+embed) | load → ClickHouse |
|---|---|---|---|
| `cc-crawl … -parts N` | **yes** | — skipped | — skipped |
| `cc-crawl … -parts N` | no | ✅ | ✅ |
| `cc-enrich-worker load --dir out_<mode>_N` | (ignored) | — | ✅ |

- **Re-running a `.loaded` part does nothing** — it skips produce *and* load (no GPU, no ClickHouse
  writes). The marker exists precisely to avoid paying the fetch+embed twice.
- **Force a full re-run** (re-fetch + re-embed + reload): `rm data/crawl/out_<mode>_N.loaded`, then run
  cc-crawl — the produce step `rm -rf`s the dir and regenerates the parquet (also delete
  `shard_<mode>_N.parquet` for a fresh worklist).
- **Reload the existing parquet only** (no re-produce): `cc-enrich-worker load --dir
  data/crawl/out_<mode>_N` — it ignores the marker. Idempotent: ReplacingMergeTree dedupes on the sort
  key and the newest `resolved_at` wins, so you never double-count.

### Running the worker yourself (without `cc-crawl`)

The pass writes Parquet into an **output directory** with **fixed filenames** (`domains.parquet`,
`industries.parquet`, `page_signals.parquet` for industry; `tech/identifiers/metadata/contacts.parquet`
for tech). The dir is unique per shard. **The pass no longer loads** — loading is a separate step,
`load --dir`, so produce and load are independently checkable (both use the native driver, so
**`clickhouse-client` is not needed**).

```bash
cd commoncrawl/cc-enrich-worker
make build                                 # or `make` at commoncrawl/ to build both binaries
set -a; source ../.env; set +a

# 1. build a worklist (or reuse one under ../data/index/) — §2d
( cd ../index-builder && uv run python -m index_builder --mode tech --part 0 )
SHARD=../data/index/CC-MAIN-2026-25/shard_tech_0.parquet

# 2. produce, then load (two steps; check the produce before loading)
./bin/cc-enrich-worker tech --worklist "$SHARD" --crawl-id CC-MAIN-2026-25 --out ../data/crawl/run0
./bin/cc-enrich-worker load --dir ../data/crawl/run0
# or one file: ./bin/cc-enrich-worker load --file ../data/crawl/run0/tech.parquet
```
**Industry is identical** — same `--out` dir then `load --dir`; it produces `domains.parquet` +
`industries.parquet` + `page_signals.parquet` (needs the reference built first, §3 A1).
`load --dir` loads whichever fixed files are present. Tables must exist (`make clickhouse-migrate-up`);
ReplacingMergeTree dedupes, so re-loading is safe.

### 5c. Data flow — where every ClickHouse row comes from (produce vs load)

The system is **two separable stages, and every ClickHouse `INSERT` happens in stage 2 (`load`)** — never
during the fetch/embed compute in stage 1. This is the model to keep in your head:

**The domain list is the crawl itself, not a curated set.** `index-builder` builds the worklist straight
from the **CommonCrawl URL index** — every crawled URL grouped by registered domain, keeping the
representative page(s) (`industry` = one homepage/domain, `tech` = many; §7). So `commoncrawl_domains` ends
up being **every domain in the crawl** (that has an HTML page), materialized **shard by shard** as you
process parts — not a known-company subset. The worker only ever consumes this fixed worklist; **it never
discovers new domains** (a domain absent from the worklist never enters the system).

**Stage 1 — produce** (`cc-enrich-worker industry|tech|embed`) fetches those pages and writes **Parquet
only**. The *single* ClickHouse touch in this stage is `industry`/`tech` **reading** the NACE reference
matrix at startup — **no row is ever inserted here.** What each mode produces:

| mode | embeds? | classifies NACE? | Parquet written | →inserted by `load` into |
|---|---|---|---|---|
| `industry` | yes (instructed vector) | yes | `domains`, `industries`, `page_signals`, + `embedding/…/embeddings.parquet` | `commoncrawl_domains`, `_industries`, `_page_signals` |
| `tech` | no | no | `domains`, `technologies`, `identifiers`, `metadata`, `contacts` | the matching `commoncrawl_*` tables |
| `embed` | yes (vector only) | no | **only** `embedding/…/embeddings.parquet` | **nothing — no load step** |

**Stage 2 — load** (`cc-enrich-worker load --dir …`, in `internal/load/load.go`) reads those Parquet files
and `INSERT`s each into its matching table (filename → table). **This is the only place rows enter
ClickHouse.** `cc-crawl` runs it automatically after a clean `industry`/`tech` produce — **but not after
`embed`** (§5b), which has no load step.

So, to answer the questions this section exists for:
- **`commoncrawl_domains`** is populated by the **load** step of an `industry` *or* `tech` pass (both write
  `domains.parquet`) — i.e. as you process index parts; its universe is the whole crawl (above).
- **`commoncrawl_industries`** (the **top-3** NACE candidates per domain — `rank`/`is_primary`/`score`; §3 A4)
  and **`commoncrawl_page_signals`** (the per-domain `nace_confident`/`nace_margin` quality, 1 row/domain)
  are inserted by the **load** step of an **`industry`** pass only — and they're produced by the produce
  step, but **inserted only at load**, never during the embed compute.
- **The embed (embedding) phase inserts NOTHING into ClickHouse.** It writes only the raw vectors to
  `data/embedding/…` and runs no load; it re-embeds domains the industry pass *already* inserted, so it adds
  no rows to any `commoncrawl_*` table. (Re-classifying NACE later from those stored vectors regenerates the
  `commoncrawl_industries` + `page_signals` rows as a CPU matmul — see §5b / `docs/embeddings-design.md`.)

---

## 6. Reference — `cc-enrich-worker` CLI

Subcommand-based: `cc-enrich-worker <industry|embed|tech|both> [flags]`. The command **is** the workflow;
mode-specific flags appear only under their command (`cc-enrich-worker tech -h`). `both` runs both in
one fetch pass (discouraged — co-locating throttles the GPU; prefer two processes). `embed` is industry's
fetch→embed stream **without** the NACE classify or ClickHouse — it writes only the raw vectors (§5b).

**Common to every command:**
| flag | default | meaning |
|---|---|---|
| `--worklist` | *(required)* | worklist Parquet shard (§2d) |
| `--crawl-id` | *(required)* | e.g. `CC-MAIN-2026-25`; stamped on every row |
| `--out` | `../data/crawl/<worklist-name>/` | output **directory** (fixed per-mode filenames); default unique per shard; an explicit dir must be empty |
| `--concurrency` | `32` | fetch/parse goroutines (push to ~128 to hide off-AWS fetch RTT) |
| `--chunk` | `1024` | domains per fetch+process chunk (lower = earlier GPU traffic) |
| `--s3-anonymous` | `false` | fetch via the HTTPS CDN instead of signed S3 — **rate-limits**, avoid for bulk |
| `--s3-bucket` / `--s3-prefix` | — | optionally upload outputs to S3 (in-AWS path) |

**`industry`, `embed` (and `both`):** `--embed-concurrency` (96; in-flight embed requests), `--embed-batch`
(16; texts/request — keep small, big batches overflow the engine's token budget). `embed`'s `--out` is the
embed dir directly (e.g. `data/embedding/out_industry_<p>`) and it writes only `embeddings.parquet` — no
reference, no ClickHouse, no `load`.

**`tech` (and `both`):** `--tech-engine` (`fast` = Aho-Corasick-gated, ~4.6×, default | `wappalyzer` =
unmodified library, every fingerprint, slower reference — both detect the same techs), `--tech-max-bytes` (131072; cap body
fed to Wappalyzer, 0 = full body).

**Fetch path:** **signed S3** (AWS creds) is the only reliable bulk source — the anonymous S3 API is
denied, and the `data.commoncrawl.org` HTTPS CDN (`--s3-anonymous`) works but is latency-bound and
rate-limits. Reads are free (Open Data). The worker streams each page through memory and discards it
(no on-disk page cache). A pass **only writes Parquet**; loading is the separate **`load` command**
over the native driver (no `clickhouse-client`), so produce and load are independently checkable.

**`load`** — `cc-enrich-worker load --dir <shard-dir>` loads every
`{domains,industries,page_signals,metadata,contacts,tech,identifiers}.parquet` present in the
directory (the fixed name maps to the table); `--file <path>` loads one (kind from its filename, or
`--kind`). An **old fat `domains.parquet`** (pre-split shard output) is detected and fanned into the
split tables automatically. Reads via the env-driven ClickHouse connection (`CLICKHOUSE_*`); tables
must exist (`make clickhouse-migrate-up`).

---

## 7. Reference — `index-builder` CLI

Pure duckdb + pyarrow (no dagster). Resolves exact index-part URLs from CommonCrawl's HTTPS manifest
(anonymous S3 LIST is denied, so no globs). One part → one shard, cached at
the gitignored `commoncrawl/data/index/<crawl>/shard_<mode>_<part>.parquet`, skipped if present.

```bash
python -m index_builder --crawl CC-MAIN-2026-25 --list                       # how many parts (~300)
python -m index_builder --crawl CC-MAIN-2026-25 --part 0                      # industry (default), cache
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --part 0          # tech (≤25 pages/domain)
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --max-pages 0 --part 0   # every HTML page
python -m index_builder --crawl CC-MAIN-2026-25 --parts 0-9                   # a range, skip cached
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --out ../data/index/my-shard.parquet  # explicit path (must be under data/)
```
| flag | default | meaning |
|---|---|---|
| `--crawl` | `CC-MAIN-2026-25` | crawl id |
| `--mode` | `industry` | `industry` (1 page/domain) \| `tech` (many) |
| `--max-pages` | `25` | tech only: max pages/domain (`0` = every 200/HTML page = ~3B, expensive) |
| `--part` / `--parts` | — | single 0-based part, or inclusive range `A-B` |
| `--list` | — | print the part count and exit |
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

Full per-column schema: **`docs/schema.md`**. DDL in `../clickhouse/migrations/`. The schema is now
**domain-centric** — `commoncrawl_*` hold only crawl-derived domain data; everything is
`ReplacingMergeTree`, so **read with `FINAL`**.

| Table | Migration | Workflow | Grain |
|---|---|---|---|
| `commoncrawl_domains` | 046 (slim 066) | every | identity master — one row / domain (the spine) |
| `commoncrawl_industries` | 063 | A | NACE — **many** rows / domain (one per candidate, rank/is_primary/score) |
| `commoncrawl_page_signals` | 064 | A | one row / domain — page_type + NACE decision quality |
| `commoncrawl_technologies` | 047 | B | one row / (domain, technology) |
| `commoncrawl_domain_identifiers` | 051 | B | one row / (domain, identifier) — LEI/VAT/… (ex `company_identifiers`) |
| `commoncrawl_domain_metadata` | 067 | B | one row / domain — self-reported JSON-LD (name, description, logo, country, founding, size) |
| `commoncrawl_domain_contact_info` | 068 | B | **many** rows / domain — email \| phone \| social |
| `commoncrawl_domain_graph_signals` | 073 | **external** | one row / (domain, crawl) — webgraph PageRank + harmonic centrality (loaded by `load-domain-ranks.sh`, §9b — not the worker) |
| `nace_category_embeddings` / `page_type_exemplars` | 044 / 045 | A (reference) | written by reference-builder, read by the worker |

`commoncrawl_company_profile` was **dropped** (split into `domain_metadata` + `domain_contact_info`).
Authoritative company facts (legal name from GLEIF, …) are a **separate, general** concern keyed by
the identifier — not a `commoncrawl_*` table (see `docs/schema.md`). After a run, these decide what to
build next:
```sql
-- B: identifier hit-rate (worth more id_types? VAT vs LEI coverage?)
SELECT id_type, count() rows, uniqExact(root_domain) domains, sum(valid) valid
FROM commoncrawl_domain_identifiers FINAL GROUP BY id_type;

-- B: contact / metadata coverage
SELECT contact_type, count() FROM commoncrawl_domain_contact_info FINAL GROUP BY contact_type;
SELECT count() FROM commoncrawl_domain_metadata FINAL WHERE name != '';

-- A: NACE confidence → size of the low-confidence escalation tail
SELECT nace_confident, count() FROM commoncrawl_page_signals FINAL GROUP BY nace_confident;
```

### 9b. Domain graph signals — PageRank + harmonic centrality (external ingest)

`commoncrawl_domain_graph_signals` (migration 073) holds CommonCrawl's **domain-level webgraph** authority
scores — one row / (domain, crawl). It is **not** produced by the worker; it's an external file you load
directly. Use the **domain-level** graph (not host-level) — it matches the `root_domain` grain and joins with
no aggregation. It's a satellite keyed on `root_domain` (separate source/cadence from the worker, so it does
**not** go on `commoncrawl_domains`; mirrors `open_page_rank_domains`).

- **`cc_harmonic_centrality`** — shortest-path reachability; the **spam-resistant** complement to PageRank
  (link farms can't fabricate real short paths). **`cc_pagerank`** — PageRank over CommonCrawl (mostly
  redundant with `open_page_rank`). Plus the rank positions (`*_rank`) and `n_hosts`.

Download the **domain** ranks file from <https://commoncrawl.org/web-graphs> (`*-domain-ranks.txt.gz`,
gunzip), then load it with **`load-domain-ranks.sh`** — it streams the file with `clickhouse-local`,
un-reverses `host_rev` (`com.example` → `example.com`), and bulk-inserts via `remote()`:
```bash
curl -s https://clickhouse.com/ | sh                 # one-time: the static clickhouse binary (or set CLICKHOUSE_BIN)
(cd .. && make clickhouse-migrate-up)                # one-time: create the table (073)

DRY=1 ./load-domain-ranks.sh /opt/cc-main-2026-apr-may-jun-domain-ranks.txt CC-MAIN-2026-apr-may-jun  # preview 5 rows
./load-domain-ranks.sh       /opt/cc-main-2026-apr-may-jun-domain-ranks.txt CC-MAIN-2026-apr-may-jun  # load
```
`crawl_id` is the **webgraph release** (it spans several monthly crawls), e.g. `CC-MAIN-2026-apr-may-jun` —
not a single `CC-MAIN-2026-NN`. Idempotent (ReplacingMergeTree dedup on `(root_domain, crawl_id)`), so
re-loading a release is safe. Join it like any satellite:
```sql
SELECT d.root_domain, g.cc_harmonic_centrality, g.cc_pagerank
FROM commoncrawl_domains d FINAL
LEFT JOIN commoncrawl_domain_graph_signals g FINAL USING (root_domain);
```

---

## 10. Operational notes

- **Model-match invariant (workflow A)** — reference and worker must use the same
  `COMMONCRAWL_EMBED_MODEL`/`BASE_URL` (both read `commoncrawl/.env`; the startup check enforces it).
  Swap the model → rebuild the reference (A1) **and** re-run the industry pass.
- **Resumable** — `cc-crawl` skips parts with a `.loaded` marker; a partial part re-runs cleanly, and
  its loader runs only after a clean produce. The worker refuses to replace a table on empty input.
- **Identifier/profile provenance (workflow B)** — rows carry `source_url =` the domain's first
  fetched page, not the exact page a given LEI/VAT was found on. `root_domain` (the join key) is
  unaffected; per-page provenance is a future change.
- **No DDL in code** — migrations own the schema; the worker's `load` command INSERTs rows over the
  native driver (it asserts nothing about DDL; tables must already exist).

---

## 11. Build, test & code layout

```bash
make                                                    # at commoncrawl/: build BOTH Go binaries (gitignored bin/)
make test                                               # worker go test ./...
cd ../index-builder     && uv run --with pytest --with duckdb --with pyarrow pytest tests/
cd ../reference-builder && uv run --with pytest --with numpy pytest tests/
```

`cc-crawl/` is a tiny standalone module (`main.go`, stdlib only) — the orchestrator; it shells out to
`cc-enrich-worker` + `index_builder` and does the JSON logging (§5).

The Go worker is `cmd/cc-enrich-worker/main.go` (entry point: flags, env, ClickHouse/embedder/getter
wiring, the chunked process loop) + `internal/` packages: `model` (shared structs), `vec` (math),
`fetch` (WARC fetch), `embed` (embedding client), `parse` (HTML→text), `tech` (Wappalyzer +
Aho-Corasick), `extract` (LEI/VAT/profile), `classify` (NACE reference + classify + confidence +
startup check), `output` (Parquet rows + S3), `load` (native-driver INSERT for the `load` command),
`worker` (`ProcessShard` orchestration). `Makefile` builds
into `bin/`; binaries and `data/` are gitignored — code dirs never hold binary output.




./cc-crawl/bin/cc-crawl -crawl CC-MAIN-2026-25 -mode industry -parts 64-220


./cc-crawl/bin/cc-crawl -base /opt/companycollect/corpscout/commoncrawl/data -mode tech -tech-conc 32 -parts 30-50 -crawl CC-MAIN-2026-25