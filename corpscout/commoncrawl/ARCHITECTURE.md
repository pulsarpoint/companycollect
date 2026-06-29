# CommonCrawl enrichment — Architecture

How the `commoncrawl` system works end to end: components, the data-flow model, the storage schema, and
the invariants that hold it together. This is the **conceptual** doc — companion to two others:

- **`README.md`** — how to *run* it (setup, commands, flags).
- **`docs/schema.md`** — the **per-column** ClickHouse schema (column types, defaults). This doc gives the
  table relationships and grain; `schema.md` gives every column.

---

## 1. What the system does

For every domain in a CommonCrawl snapshot it produces three independent kinds of output:

1. **Industry** — a NACE code (with runner-up candidates) per domain, by embedding the domain's homepage
   and cosine-matching it against a NACE category matrix.
2. **Technologies & company info** — tech stack (Wappalyzer), registry identifiers (LEI/VAT — the join keys
   to external company masters), self-reported metadata (schema.org JSON-LD), and contacts (email/phone/social).
3. **Raw page embeddings** — the expensive GPU vector for each domain's page, persisted so industry can be
   re-derived (and similarity/discovery built) later on CPU without re-fetching or re-embedding.

It is **self-contained** (pure Go + Python, no dagster). Its only upstream dependency is one
dagster-populated ClickHouse table, `corpscout.nace_categories`, which `reference-builder` reads to build the
NACE matrix.

---

## 2. Components

| Component | Lang | Role | Touches ClickHouse? |
|---|---|---|---|
| **`index-builder/`** | Python (duckdb, pyarrow) | Builds the **worklist** — "what pages to fetch" — from the CommonCrawl URL index. | no (reads the CC index only) |
| **`reference-builder/`** | Python (numpy, openai, clickhouse) | Builds the **NACE reference matrix** + page-type exemplars (industry only). | **writes** the 2 reference tables |
| **`cc-enrich-worker/`** | Go | The **processor**. Subcommands `industry`/`tech`/`both`/`embed` (produce) + `load`. Fetches WARC pages, runs the workflow → Parquet; the `load` subcommand inserts Parquet → ClickHouse. | **reads** reference (produce), **writes** result tables (load) |
| **`cc-crawl/`** | Go (stdlib only) | The **orchestrator**. Per-part loop: worklist → produce → load → marker, with JSON logging. `os/exec` only — it does **not** import the worker and **never** touches ClickHouse itself. | no |

**Boundary that matters:** `cc-crawl` orchestrates by shelling out; **all** ClickHouse I/O lives in
`cc-enrich-worker`. So "where does X get written" is always answered inside the worker, never in `cc-crawl`.

ClickHouse **DDL is owned by `../clickhouse/migrations/`** (golang-migrate), never by code — the worker
`INSERT`s into tables it assumes already exist and issues no DDL.

---

## 3. The central model: two stages — *produce* and *load*

The single most important thing to understand: the pipeline is **two separable stages, and every ClickHouse
`INSERT` happens in stage 2 (`load`)** — never during the fetch/embed/classify compute.

```
                STAGE 1: PRODUCE (compute → files)            STAGE 2: LOAD (files → ClickHouse)
                ─────────────────────────────────            ──────────────────────────────────
  worklist ──►  cc-enrich-worker <industry|tech|embed>  ──►   cc-enrich-worker load --dir <out>
  (parquet)     • fetch WARC pages (signed S3)                • read each <kind>.parquet
                • embed (industry/embed) / classify (industry)• INSERT INTO commoncrawl_<table>
                • detect tech / extract ids (tech)              over the native protocol
                • WRITE Parquet only  ◄── no CH INSERT here   • the ONLY place rows enter ClickHouse
                  (sole CH touch: industry READS the NACE
                   reference matrix at startup)
```

Why split: produce is the expensive, failure-prone part (network + GPU); load is cheap and idempotent.
Separating them means a produce can be inspected before it's loaded, a load can be re-run without
re-fetching, and a crashed produce never half-writes ClickHouse. Both use the native driver, so
`clickhouse-client` is never required.

**Code path for one table** (`commoncrawl_domains` as the example):

```
produce: worker.domainRow(...)            -> []output.DomainRow    -> output.WriteDomains -> domains.parquet
load:    load.FromDir -> FromFile["domains"] -> parquet.ReadFile[DomainRow]
                                              -> load.Insert -> "INSERT INTO commoncrawl_domains (...)"
```

- Row built in memory: `cc-enrich-worker/internal/worker/worker.go` (`domainRow`, and the per-mode assembly).
- Read + insert: `cc-enrich-worker/internal/load/load.go` (`FromFile` `case "domains"` → `Insert`).
- There is **no `UPDATE`** — see §7 (ReplacingMergeTree).

---

## 4. End-to-end data flow (per part)

`cc-crawl` drives a per-part loop (`runProduceLoad` for industry/tech, `runEmbedPart` for embed):

```
for each part P in the requested range:
  0. marker check   — if out_<mode>_<P>.loaded exists -> SKIP the whole part
  1. worklist       — ensure shard_<mode>_<P>.parquet (index-builder builds it on demand, cached)
  2. produce        — exec: cc-enrich-worker <mode> --worklist shard --out out_<mode>_<P>
                      (clears the out dir first; worker requires an empty --out)
  3. gate           — require exit 0 AND domains.parquet present, else FAIL (loader skipped)
  4. load           — exec: cc-enrich-worker load --dir out_<mode>_<P>   [industry/tech only]
  5. marker         — touch out_<mode>_<P>.loaded
```

- **The crawl index is ~300 parts**; one part → one worklist shard → one output dir.
- **Resumable / idempotent:** the empty `.loaded` marker gates the *whole* part (produce + load); re-running
  a done part does nothing. A partial part re-runs cleanly.
- **Run the two heavy workflows as separate processes** — `tech` pegs the CPU, `industry` feeds the GPU;
  co-locating throttles the GPU.

---

## 5. The three produce modes

All three share the fetch path; they differ in what they compute, write, and load. (`both` runs industry+tech
in one fetch pass — discouraged; it throttles the GPU.)

| mode | input worklist | embeds? | classifies NACE? | Parquet written | loaded into ClickHouse |
|---|---|---|---|---|---|
| **`industry`** | `shard_industry_*` (1 page/domain) | yes (instructed vector) | yes | `domains`, `industries`, `page_signals` + `embedding/…/embeddings.parquet` | `commoncrawl_domains`, `_industries`, `_page_signals` |
| **`tech`** | `shard_tech_*` (many pages/domain) | no | no | `domains`, `technologies`, `identifiers`, `metadata`, `contacts` | the matching `commoncrawl_*` tables |
| **`embed`** | `shard_industry_*` (reused) | yes (vector only) | no | **only** `embedding/…/embeddings.parquet` | **nothing — no load step** |

Key consequences:

- **`commoncrawl_industries`** (top-3 NACE candidates/domain) and **`commoncrawl_page_signals`** (NACE
  confidence/margin) come from the **industry** pass's load step — not produce, not embed.
- **The embed phase touches ClickHouse zero times.** It only writes the raw vectors and runs no load; it
  re-embeds domains the industry pass already inserted (see §6, §8). `cc-crawl` runs no loader for embed.
- `domains.parquet` (the identity master) is written by industry **and** tech, not by embed.

---

## 6. The domain universe — where the domain list comes from

There is **no curated company list.** `index-builder` builds the worklist directly from the **CommonCrawl
URL index**: every crawled URL, grouped by registered domain, keeping the representative page(s):

- **industry** worklist → the *one* most-representative page per domain (main-site homepage: apex/`www`,
  shallowest path).
- **tech** worklist → *many* pages per domain (homepage + legal/contact/about, multilingual ranking, capped
  at `MAX_PAGES`), because LEI/VAT and the Organization JSON-LD live on `/imprint`, `/legal`, `/contact`.

So `commoncrawl_domains` ends up being **every domain in the crawl** (with an HTML page), materialized **shard
by shard** as parts are processed. The worker **only ever consumes this fixed worklist — it never discovers
new domains**; a domain absent from the worklist never enters the system. (Link-graph–based discovery of new
domains would be a separate, future stage; it does not exist today.)

---

## 7. Storage model & schema

All result tables live in the `corpscout` database, prefixed `commoncrawl_`, and are **`ReplacingMergeTree`**.
Two implications run through everything:

- **Insert-only, no `UPDATE`.** A re-load appends rows; ReplacingMergeTree dedupes on the sort key at merge
  time, keeping the newest `resolved_at`. Because merges are asynchronous, **every read uses `FINAL`**.
- **The migration owns the schema.** Columns, engine, and `ORDER BY` are defined in
  `../clickhouse/migrations/`; the worker's `INSERT` column list is derived from the `ch:"…"` struct tags, so
  a table may carry extra defaulted columns the worker doesn't write.

### Tables at a glance

Full per-column detail: **`docs/schema.md`**. Grain + provenance:

| Table | Grain | Pass | Migration | Holds |
|---|---|---|---|---|
| `commoncrawl_domains` | 1 / domain | every (industry+tech) | 046, slim 066 | identity master — the spine everything joins to |
| `commoncrawl_industries` | **many** / domain | industry | 063 | NACE candidates — one row per code, with `rank` / `is_primary` / `score` |
| `commoncrawl_page_signals` | 1 / domain | industry | 064 | `page_type` + the NACE decision quality (`nace_confident`, `nace_margin`) |
| `commoncrawl_technologies` | many / (domain, tech) | tech | 047 | detected technologies (Wappalyzer) |
| `commoncrawl_domain_identifiers` | many / (domain, id) | tech | 051 | scraped LEI / VAT / tax / duns / naics (the domain→identifier bridge) |
| `commoncrawl_domain_metadata` | 1 / domain | tech | 067 | self-reported JSON-LD (name, description, logo, country, founding, size) |
| `commoncrawl_domain_contact_info` | **many** / domain | tech | 068 | email / phone / social |
| `nace_category_embeddings` / `page_type_exemplars` | reference | (reference-builder) | 044 / 045 | the NACE matrix + page-type seed the industry pass reads at startup |

### Relationships

```
                        ┌──────────────────────────┐
                        │   commoncrawl_domains     │  1 / domain  (identity master / spine)
                        │   key: root_domain        │
                        └──────────┬───────────────┘
        root_domain ┌──────────────┼───────────────────────────┬───────────────┬───────────────┐
                    ▼              ▼                            ▼               ▼               ▼
   commoncrawl_industries  commoncrawl_page_signals  commoncrawl_technologies  ..._identifiers  ..._domain_metadata
     (N: top-3 NACE)         (1: NACE quality)          (N: tech stack)        (N: LEI/VAT…)      (1: JSON-LD)
                                                                                     │
                                                              ..._domain_contact_info (N: email/phone/social)
                                                                                     │
                                                            id_value (where id_type='lei') ─► external company master
                                                                                              (GLEIF / country registries)
```

Everything joins on **`root_domain`**. `commoncrawl_domain_identifiers.id_value` (LEI/VAT) is the bridge to
authoritative company data, which is a **separate, general** concern — not a `commoncrawl_*` table.

---

## 8. The embedding store (on disk, not ClickHouse)

The industry pass's page vector is the **expensive GPU artifact**; the NACE classification is just a cheap
cosine matmul derived from it. So the raw vector is persisted — but **to Parquet, not ClickHouse** (at
full-crawl scale the vectors are hundreds of GB of incompressible data; nothing relational queries them).

- **Location:** `data/embedding/out_industry_<part>/embeddings.parquet` — a separate tree from `data/crawl/`.
- **Shape (`output.EmbeddingRow`):** `root_domain`, `subdomain`, `embedding` (fp32, 4096-dim), `embed_dim`,
  `text_len`, `source_url`, and the **WARC re-fetch triple** (`warc_filename`, `warc_offset`, `warc_length`)
  — enough to re-fetch the *exact* archived page that produced the vector without re-resolving the index.
- **Written:** by the **industry** pass on every run (and **back-filled** for older parts by **`embed`** mode,
  which re-fetches + re-embeds the same worklist and writes only this file).
- **Not loaded to ClickHouse, ever.** It is the source of record for cheap downstream work:
  **re-classification** (re-run NACE — a tuned gate, a revision, a different taxonomy — as a CPU matmul over
  stored vectors, regenerating `commoncrawl_industries` + `page_signals`), and similarity / vector search.

The embedding is **instruction-conditioned for classification** (the query carries a classify instruction;
the NACE matrix is embedded plain). That makes it optimal for NACE but less ideal for general similarity.
fp16 is lossless for NACE, so the store can be halved offline (see `embedding-tools/`). Design detail:
`docs/embeddings-design.md`.

---

## 9. Key invariants & design decisions

- **Two stages, all inserts in `load`.** Produce never inserts (it only *reads* the NACE reference); the
  `load` subcommand is the sole writer of every `commoncrawl_*` table (§3).
- **`cc-crawl` orchestrates, the worker does the work.** No ClickHouse, no parquet I/O in `cc-crawl` — only
  `os/exec` + JSON logging. ClickHouse lives entirely in `cc-enrich-worker`.
- **No DDL in code.** Migrations own the schema; the worker asserts tables exist and `INSERT`s only.
- **ReplacingMergeTree everywhere → read with `FINAL`.** Insert-only; re-running is safe (dedup on sort key,
  newest `resolved_at` wins). There is no `UPDATE`.
- **Model-match invariant (industry).** Page vectors and the NACE reference matrix **must** come from the
  same embedding model/dim. The industry pass checks this at startup and refuses to run on a mismatch; change
  the served model → rebuild the reference **and** re-run industry.
- **Idempotent + resumable.** The `.loaded` marker gates each part; the worker refuses to replace a table on
  empty input (a bad fetch can't blank a populated table).
- **Two workflows, two processes.** Industry (GPU-bound) and tech (CPU-bound) share only the fetch path; run
  them separately so neither starves the other.
- **Self-contained.** No dagster; the only upstream coupling is reading `corpscout.nace_categories` to build
  the reference.

---

## 10. Reference map

| Want | Read |
|---|---|
| How to run it (setup, commands, flags) | `README.md` |
| Every column of every table | `docs/schema.md` |
| The orchestrator's design | `docs/cc-crawl-design.md` |
| The embedding store + re-classification / vector search | `docs/embeddings-design.md` |
| The embed-only backfill mode | `docs/embed-only-mode-plan.md` |
| fp16 conversion / verify / prune of the embedding store | `embedding-tools/README.md` |
| The produce→load code path | `cc-enrich-worker/internal/{worker,output,load}/` |
