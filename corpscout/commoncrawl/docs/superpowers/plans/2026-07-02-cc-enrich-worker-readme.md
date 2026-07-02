# cc-enrich-worker README Documentation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `cc-enrich-worker/README.md` — a complete, worker-specific reference covering every subcommand, every flag and env var (with exact defaults), the per-mode processing logic, the output files, and the limits/gotchas/invariants.

**Architecture:** This is a **documentation** plan, so the normal TDD cycle is replaced by a **doc cycle**: each task (1) writes one README section with the exact content given below, (2) runs a `rg`/build **verification** that proves the section's facts still match the source, (3) commits. The content below was extracted from the source on **2026-07-02** with `file:line` citations; the verification step re-checks the volatile facts (flag names/defaults, env vars, table map) so a stale doc fails loudly.

**Tech Stack:** Markdown. Source is Go 1.26 (`cc-enrich-worker/go.mod:3`). Verification uses `rg` and `go build`.

## Global Constraints

- **Single output file:** `corpscout/commoncrawl/cc-enrich-worker/README.md` (create new). Do not touch other files except this plan's checkboxes.
- **Exactness over prose:** every flag name, default, env var, and table name MUST match the source verbatim. When in doubt, `rg` the cited `file:line` and copy what's there — do not paraphrase defaults.
- **No invented behavior:** document only what the code does. Where a value is hardcoded/non-configurable, say so (see the "stale/ambiguous" notes in Task 8).
- **Relative paths in prose** are from `commoncrawl/` (the parent), matching the existing `commoncrawl/README.md` convention (e.g. run `cd commoncrawl/cc-enrich-worker && make`).
- **Cross-link, don't duplicate:** the whole-pipeline story (cc-crawl orchestration, workflows A/B, the ClickHouse schema) lives in `../README.md` and `../docs/schema.md`; this README is the **worker CLI + internals** reference and links out for the rest.
- **Commit by explicit path** (`git add cc-enrich-worker/README.md`), never `git add -A` — the tree carries unrelated WIP. Co-author footer on every commit:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

- **Create:** `corpscout/commoncrawl/cc-enrich-worker/README.md` — the only deliverable. Built up section-by-section across Tasks 1–9; each task appends its section and commits.

Section order (top to bottom of the final README): 1 Overview → 2 Build & layout → 3 Subcommands → 4 Flags → 5 Env vars → 6 Processing logic → 7 Output files → 8 Limits & invariants → 9 Internal packages.

---

## Task 1: Overview + the two-stage model

**Files:**
- Create: `cc-enrich-worker/README.md`

- [ ] **Step 1: Write the section.** Create the file with this content:

````markdown
# cc-enrich-worker

The **processor** of the CommonCrawl enrichment pipeline: a single Go binary that byte-range-fetches
WARC pages from S3, runs one of two workflows over them (industry classification or technology/company-info
extraction), writes **Parquet**, and — as a separate step — **loads** that Parquet into ClickHouse.

It is a subcommand CLI: `cc-enrich-worker <industry|embed|tech|both|load> [flags]`. The command **is** the
workflow. It does not orchestrate parts or build worklists — that's `cc-crawl` and `index-builder`
(see [`../README.md`](../README.md)).

**Two stages, and every ClickHouse `INSERT` is in stage 2 (`load`):**

| Stage | Command | Does | ClickHouse |
|---|---|---|---|
| **produce** | `industry` / `embed` / `tech` / `both` | fetch → (embed/classify \| detect tech) → write Parquet | only **reads** the NACE reference (industry/both) |
| **load** | `load` | read the produced Parquet → `INSERT` into `commoncrawl_*` | the **only** writer |

Splitting them means a produce can be inspected before it's loaded, a load can re-run without re-fetching,
and a crashed produce never half-writes ClickHouse. Both stages use the native ClickHouse protocol, so
`clickhouse-client` is never required.

> Full pipeline context (cc-crawl driver, workflows A/B, per-part loop, the ClickHouse schema):
> [`../README.md`](../README.md) and [`../docs/schema.md`](../docs/schema.md). This doc is the worker's
> CLI + internals reference.
````

- [ ] **Step 2: Verify the subcommand list is exact.**

Run: `rg -n 'case "industry"|case "embed"|case "tech"|case "both"|case "load"|os\.Args\[1\]' cc-enrich-worker/cmd/cc-enrich-worker/main.go`
Expected: the dispatch on `os.Args[1]` around `main.go:123-134` with exactly these five cases. If a case is missing/renamed, fix the table.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — overview + produce/load model"
```

---

## Task 2: Build, run, and module layout

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Build & run

Go **1.26+**, no cgo. From `commoncrawl/`:

```bash
make -C cc-enrich-worker            # -> cc-enrich-worker/bin/cc-enrich-worker  (static, CGO_ENABLED=0)
# or, at commoncrawl/:  make worker
make -C cc-enrich-worker arm        # cross-compile linux/arm64 (Graviton)
make -C cc-enrich-worker test       # go test ./...
make -C cc-enrich-worker vet        # go vet ./...
make -C cc-enrich-worker clean      # rm -rf bin
# amd64 cross-compile (no target):  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/cc-enrich-worker ./cmd/cc-enrich-worker
```

A minimal run (produce, then load) — see §Flags for every option:
```bash
set -a; source ../.env; set +a
./bin/cc-enrich-worker tech --worklist ../data/index/CC-MAIN-2026-25/shard_tech_0.parquet \
                            --crawl-id CC-MAIN-2026-25 --out ../data/crawl/run0
./bin/cc-enrich-worker load --dir ../data/crawl/run0
```

## Module layout

```
cc-enrich-worker/
├── cmd/cc-enrich-worker/main.go   # subcommand dispatch, flag parsing, run() + runLoad()
├── internal/
│   ├── classify/   # NACE cosine classify + page-type signals + startup model-match check
│   ├── embed/      # OpenAI-compatible embedding client (batching, concurrency, retry, L2-normalize)
│   ├── extract/    # schema.org JSON-LD profile, LEI, VAT (checksum-validated)
│   ├── fetch/      # RangeGetter: signed S3 + anonymous CDN; FetchRecord unpacks a WARC record
│   ├── load/       # Parquet -> ClickHouse (native protocol); legacy fat-domains fan-out
│   ├── model/      # shared types (Reference, Prototypes, WorklistItem, Technology, Identifier, …)
│   ├── output/     # *Row structs (parquet+ch tags), Write* funcs, S3 upload
│   ├── parse/      # HTML -> visible text, emails, social links
│   ├── tech/       # DetectTech: fast Aho-Corasick gating | upstream wappalyzergo
│   ├── vec/        # Dot, L2-normalize
│   └── worker/     # ShardConfig, ProcessIndustryStream, FetchChunk + Finalize
├── bin/            # build output (gitignored)
├── Makefile  ·  Dockerfile  ·  go.mod
```
````

- [ ] **Step 2: Verify the Makefile targets + Go version are exact.**

Run: `rg -n 'CGO_ENABLED|GOOS=linux GOARCH|go test|go vet|rm -rf bin' cc-enrich-worker/Makefile; rg -n '^go ' cc-enrich-worker/go.mod`
Expected: `build` uses `CGO_ENABLED=0 go build -o bin/cc-enrich-worker ./cmd/cc-enrich-worker`; `arm` adds `GOOS=linux GOARCH=arm64`; `go.mod` says `go 1.26.x`. Fix any drift.

- [ ] **Step 3: Verify the package list matches the tree.**

Run: `ls cc-enrich-worker/internal`
Expected: exactly `classify embed extract fetch load model output parse tech vec worker`. Add/remove rows to match.

- [ ] **Step 4: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — build, run, module layout"
```

---

## Task 3: Subcommands reference

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Subcommands

`cc-enrich-worker <command> [flags]`. The command is the workflow; mode-specific flags appear only under
their command.

| Command | Fetches | Embeds? | Classifies NACE? | Needs | Writes | Bottleneck |
|---|---|---|---|---|---|---|
| **`industry`** | 1 primary page/domain | yes (instructed) | yes | embed endpoint + ClickHouse reference + S3 | `domains`, `industries`, `page_signals` + `embeddings.parquet` | GPU |
| **`embed`** | 1 primary page/domain | yes (vector only) | no | embed endpoint + S3 | **only** `embeddings.parquet` — no ClickHouse, no load | GPU |
| **`tech`** | many pages/domain | no | no | S3 only | `domains`, `technologies`, `identifiers`, `metadata`, `contacts` | CPU |
| **`both`** | many pages/domain | yes | yes | everything industry+tech need | all of the above | GPU (discouraged) |
| **`load`** | — (reads Parquet) | — | — | ClickHouse | `INSERT`s into `commoncrawl_*` | — |

- **`both` is discouraged** — co-locating the CPU tech pass with the GPU industry pass throttles the GPU;
  prefer two separate processes.
- **`embed` is `industry` minus the classify + ClickHouse** — its whole job is to persist the raw page
  vector for parts that were processed before vectors were stored (backfill) or that industry hasn't
  reached. It is **idempotent**: it skips a shard whose output dir already holds a complete
  `embeddings.parquet` **or** `embeddings_fp16.parquet` (§Limits).
- **`load` is decoupled** from produce so you can inspect Parquet before inserting and re-load without
  re-fetching (§Processing).
````

- [ ] **Step 2: Verify the per-mode write set.**

Run: `rg -n 'mode != "embed"|mode == "embed"|WriteDomains|WriteIndustries|WriteTech|WriteEmbeddings' cc-enrich-worker/cmd/cc-enrich-worker/main.go`
Expected: `domains.parquet` written for every non-embed mode; `industries`/`page_signals` gated to non-tech; embed writes only embeddings. Fix the table if the guards differ.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — subcommands reference"
```

---

## Task 4: Flags reference (the critical, exact tables)

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.** Transcribe these tables verbatim.

````markdown
## Flags

Parsed per-subcommand via `flag.NewFlagSet(..., flag.ExitOnError)` in `parse()` / `runLoad()`
(`cmd/cc-enrich-worker/main.go`). `-h` under any command prints its own set.

### Common to `industry`, `embed`, `tech`, `both`

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--worklist` | string | **required** | worklist Parquet shard (one row per page to fetch) |
| `--crawl-id` | string | **required** | e.g. `CC-MAIN-2026-25`; stamped on every output row |
| `--out` | string | `../data/crawl/<worklist-stem>/` | output **directory** (fixed per-mode filenames); an explicit dir must be **empty** (except `embed`, which reuses it) |
| `--concurrency` | int | `32` | fetch/parse goroutines; push toward ~128 to hide off-AWS fetch RTT |
| `--chunk` | int | `1024` | domains per fetch+process chunk (lower = earlier GPU traffic) |
| `--s3-anonymous` | bool | `false` | fetch via the anonymous HTTPS CDN instead of signed S3 — **rate-limited**, no upload |
| `--s3-bucket` | string | (unset) | if set, upload outputs to this bucket (in-AWS path; signed S3 only) |
| `--s3-prefix` | string | (unset) | key prefix for uploaded outputs |

### `industry`, `embed`, `both` only

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--embed-batch` | int | `16` | texts per embed request — keep small; big batches overflow the engine's token budget |
| `--embed-concurrency` | int | `96` | embed requests in flight (saturate the GPU) |

### `tech`, `both` only

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--tech-engine` | string | `fast` | `fast` = Aho-Corasick-gated Wappalyzer (a strict superset of the library's output, ~4.6× faster) \| `wappalyzer` = unmodified `wappalyzergo` (every fingerprint; parity baseline, slower) |
| `--tech-max-bytes` | int | `131072` | cap body bytes fed to the tech matcher (`0` = full body; full-body regex ≈ 1.2 s/page) |

### `load` only

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--dir` | string | — | a shard output directory; loads every `{domains,industries,page_signals,metadata,contacts,tech,identifiers}.parquet` present |
| `--file` | string | — | a single Parquet file to load |
| `--kind` | string | (inferred) | override the kind for `--file`: `domains\|industries\|page_signals\|metadata\|contacts\|tech\|identifiers` |

Pass **exactly one** of `--dir` / `--file`.
````

- [ ] **Step 2: Verify EVERY flag name + default against source (this is the highest-risk section).**

Run: `rg -n 'fs\.String|fs\.Int|fs\.Bool|flag\.String|flag\.Int|flag\.Bool' cc-enrich-worker/cmd/cc-enrich-worker/main.go`
Expected — every flag above appears with the stated default: `--concurrency` 32, `--chunk` 1024, `--embed-concurrency` 96, `--embed-batch` = `embed.DefaultBatch`, `--tech-max-bytes` 131072, `--tech-engine "fast"`, `--s3-anonymous false`. Also run `rg -n 'DefaultBatch' cc-enrich-worker/internal/embed/embed.go` and confirm `DefaultBatch = 16`. **Fix any default that differs — do not trust this plan over the source.**

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — flags reference"
```

---

## Task 5: Environment variables

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Environment variables

Read via `envOr()` / `envInt()` in `main.go`, plus the fetch/embed clients. Load them with
`set -a; source ../.env; set +a` (see [`../README.md`](../README.md) §2).

| Var | Default | Controls |
|---|---|---|
| `COMMONCRAWL_EMBED_BASE_URL` | `http://localhost:8000/v1` | OpenAI-compatible embedding endpoint |
| `COMMONCRAWL_EMBED_MODEL` | (empty → auto-detect) | model name; empty ⇒ probe `/v1/models` for the served model |
| `COMMONCRAWL_EMBED_MAX_CHARS` | `0` → **2000** | chars per text before embedding (`0` means use the built-in cap 2000) |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_NATIVE_PORT` | `9000` | ClickHouse native protocol port |
| `CLICKHOUSE_DATABASE` | `corpscout` | database |
| `CLICKHOUSE_USER` | `default` | user |
| `CLICKHOUSE_PASSWORD` | (empty) | password |
| `AWS_REGION` | `us-east-1` | S3 region (the CommonCrawl bucket is permanently `us-east-1`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | (none) | signed-S3 credentials; validated upfront (5 s) in `NewS3Getter`. Off-AWS you must export these |
| `CC_BASE_URL` | (empty → `data.commoncrawl.org/`) | anonymous CDN base for `--s3-anonymous` |

**Connection gating:** `embed` uses **no** ClickHouse (logs `embed-only mode: no NACE reference, no
ClickHouse, no load`). `industry`/`both` connect to **read** the reference + run the startup model-match
check. `load` always connects.
````

- [ ] **Step 2: Verify env var names + defaults.**

Run: `rg -n 'envOr\(|envInt\(|Getenv' cc-enrich-worker/cmd/cc-enrich-worker/main.go cc-enrich-worker/internal/fetch/fetch.go cc-enrich-worker/internal/embed/embed.go`
Expected: the names + defaults above, e.g. `envOr("CLICKHOUSE_HOST","localhost")`, `envOr("CLICKHOUSE_NATIVE_PORT","9000")`, `envOr("COMMONCRAWL_EMBED_BASE_URL","http://localhost:8000/v1")`, `MaxChars` fallback `2000`. Correct any mismatch.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — environment variables"
```

---

## Task 6: Processing logic

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Processing logic

### industry / embed — `ProcessIndustryStream` (continuous fetch→embed)

One primary page per domain, streamed so the GPU is fed non-stop (no per-chunk "embed all then wait"
barrier):
1. **Fetch workers** (semaphore = `--concurrency`, default 32) each fetch the domain's `Primary` page
   (the worklist is ordered shallowest-first, so the first row per `root_domain` is the homepage),
   30 s timeout/record, parse HTML → visible text, and stream a `streamItem` (domain index, text, WARC
   coords) onto a channel.
2. **Embed workers** (pool = `--embed-concurrency`, default 96) batch texts into `--embed-batch`
   (default 16) and POST to the endpoint.
   - **embed mode:** store the raw vector only — skip classify and the domain/industry/signal rows.
   - **industry/both:** page-type **signal matching** runs first (keyword regexes; a matched
     junk/parked type wins and skips NACE), else `classify.Classify` does cosine over the NACE matrix and
     the row is fanned into `domains` + `industries` (top-3) + `page_signals`.

Per-domain result slices are indexed by domain order (no shared writes).

### tech / both — `FetchChunk` + `Finalize` (chunked pipeline)

Tech wants **coverage**, so it fetches *all* of a domain's pages and aggregates:
- **FetchChunk** (semaphore = `--concurrency`): per page — detect tech (body capped at `--tech-max-bytes`,
  deduped by tech name per domain), email regex, schema.org JSON-LD profile, LEI (text+jsonld), VAT
  (format+checksum). First surviving page becomes the domain's `primaryURL`; the `Primary` page's text is
  kept for embedding (`both`).
- **Finalize** (`runtime.NumCPU()` classify workers, non-overlapping index ranges): for `both`, embed +
  classify the primary texts; fan each domain into tech / identifier / metadata / contact rows.
- **Chunking** (`--chunk`, default 1024): the next chunk is **prefetched over the network while the
  current chunk embeds on the GPU**, so the GPU isn't idle during fetch.

### embed verify-and-skip (idempotency)

Before doing any work, `embed` checks the output dir for a complete vector file under **either** name —
`embeddings.parquet` (fp32) **or** `embeddings_fp16.parquet` (converted offline). It reads only the
Parquet **footer row count**; `> 0` ⇒ the shard is done and is skipped (logs `skip: embeddings already
present …`). If the footer can't be read (fp16) but the file is non-empty, it's accepted as done. There is
**no `.loaded` marker** for embed — the vector file *is* the marker. Non-embed modes instead require
`--out` to be **empty**.

### load — Parquet → ClickHouse

`load --dir` reads each fixed-name Parquet and `INSERT`s it into its table over the native protocol
(§Output). `load --file [--kind]` loads one. The column list is derived from each struct's `ch` tags, so a
table may carry extra defaulted columns the worker doesn't write. All target tables are
**ReplacingMergeTree**, so re-loading is safe (dedupe on the sort key, newest `resolved_at` wins) — **read
with `FINAL`**. An **old fat `domains.parquet`** (pre-split, detected by a `nace_top3_codes` column) is
fanned into the split tables automatically.
````

- [ ] **Step 2: Verify the entry points + the fp16-aware skip exist.**

Run: `rg -n 'func ProcessIndustryStream|func FetchChunk|func Finalize' cc-enrich-worker/internal/worker/worker.go; rg -n 'embeddings.parquet|embeddings_fp16.parquet|parquetRows' cc-enrich-worker/cmd/cc-enrich-worker/main.go`
Expected: the three functions exist; the embed skip loops over **both** `embeddings.parquet` and `embeddings_fp16.parquet`. If the skip only checks one name, the "verify-and-skip" paragraph is stale — fix it.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — processing logic per mode"
```

---

## Task 7: Output files

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Output files

Each produce mode writes fixed-name Parquet into `--out`. Every `*Row` struct carries dual tags —
`parquet` (write) and `ch` (the `load` INSERT column). Full per-column schema:
[`../docs/schema.md`](../docs/schema.md).

| File | Struct | Modes | ClickHouse table | Grain |
|---|---|---|---|---|
| `domains.parquet` | `DomainRow` | every non-embed | `commoncrawl_domains` | 1 / domain (identity master) |
| `industries.parquet` | `IndustryRow` | industry, both | `commoncrawl_industries` | **many** / domain (top-3 NACE, `rank`/`is_primary`/`score`) |
| `page_signals.parquet` | `PageSignalRow` | industry, both | `commoncrawl_page_signals` | 1 / domain (`page_type`, `nace_confident`, `nace_margin`) |
| `technologies.parquet` (`tech.parquet`) | `TechRow` | tech, both | `commoncrawl_technologies` | many / (domain, tech) |
| `identifiers.parquet` | `IdentifierRow` | tech, both | `commoncrawl_domain_identifiers` | many / (domain, id) — LEI/VAT/… |
| `metadata.parquet` | `MetadataRow` | tech, both | `commoncrawl_domain_metadata` | 1 / domain (schema.org JSON-LD) |
| `contacts.parquet` | `ContactRow` | tech, both | `commoncrawl_domain_contact_info` | many / domain (email/phone/social) |
| `embeddings.parquet` | `EmbeddingRow` | industry, both, embed | **Parquet only — never ClickHouse** | 1 / domain (fp32 4096-dim vector) |

**Embeddings are special.** They go to a **separate sibling tree** `../data/embedding/<stem>/` for
industry/both (for `embed`, `--out` *is* that dir), not `../data/crawl/`, and are **never** loaded into
ClickHouse (too large, incompressible). `EmbeddingRow` carries the WARC re-fetch coords
(`warc_filename`, `warc_offset`, `warc_length`) so the exact archived page can be re-fetched without
re-resolving the index. The write is **atomic** (one `WriteFile` at the very end). Rationale + downstream
use (re-classification, vector search): [`../docs/embeddings-design.md`](../docs/embeddings-design.md).
````

- [ ] **Step 2: Verify the struct → filename → table mapping.**

Run: `rg -n 'type \w+Row struct' cc-enrich-worker/internal/output/output.go; rg -n '"domains":|"industries":|"page_signals":|"metadata":|"contacts":|"tech":|"identifiers":' cc-enrich-worker/internal/load/load.go`
Expected: the eight `*Row` structs above, and the `Tables` map exactly as in the table (note the parquet stem `tech` maps to table `commoncrawl_technologies`, and `contacts` → `commoncrawl_domain_contact_info`). Fix any mismatch, especially the `tech`/`technologies` filename note.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — output files"
```

---

## Task 8: Limits, gotchas & invariants

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Limits, gotchas & invariants

**Concurrency & batching**
- Fetch/parse: `--concurrency` (default 32) goroutines, bounded by a semaphore.
- Embed: `--embed-concurrency` (default 96) requests in flight; `--embed-batch` (default 16) texts/request
  — keep the batch small, big batches overflow the engine's token budget.
- Classify (tech/both `Finalize`): `runtime.NumCPU()` workers over non-overlapping index ranges.

**Timeouts & retries**
- Fetch: **30 s per WARC record** (a slow/bad page is skipped; the domain still emits if its primary
  survived; the first error is sampled into the log).
- Embed HTTP client: **120 s**, up to **4 retries** with backoff on transient (EOF/5xx) errors; fetch has
  the same backoff on transient/connection errors.

**Caps**
- Embed text: `COMMONCRAWL_EMBED_MAX_CHARS`, default **2000** chars/text.
- Tech body: `--tech-max-bytes`, default **131072** (`0` = full body ≈ 1.2 s/page).

**Output-dir rules**
- Non-embed modes **refuse a non-empty `--out`** (fatal) — point it at a fresh dir.
- `embed` **reuses** the dir on purpose (that's how verify-and-skip works).
- Embeddings land in the sibling `../data/embedding/<stem>/`, not the crawl dir.

**Refuse-to-replace / safety**
- `load --dir` with **no** matching Parquet files errors rather than doing nothing.
- Produce writes Parquet atomically (embeddings especially); a killed produce leaves no partial vector file
  the skip would mistake for complete.

**Model-match invariant (industry/both)**
- The NACE reference (`nace_category_embeddings`) MUST be built with the **same model + dim** as the embed
  endpoint. Startup probes the endpoint and **fails fast** on a model-name or dim mismatch. Change the
  served model ⇒ rebuild the reference (`reference-builder`) **and** re-run industry.

**S3 fetch**
- **Signed S3** (default) is the only reliable bulk source and validates credentials upfront (5 s). Reads
  are free (CommonCrawl is Open Data).
- **`--s3-anonymous`** uses the public CDN (`data.commoncrawl.org`) — latency-bound, **rate-limited**, and
  **cannot upload** (`--s3-bucket` needs signed S3).

**Primary-page selection**
- The worklist is shallowest-first; the first row per `root_domain` is `Primary`. industry/embed embed
  **only** the primary page; tech aggregates all pages but emits one primary URL/domain.

**Not configurable (hardcoded)** — flagged so you don't look for a knob that isn't there:
- Page-type decision threshold `0.55` and confidence temperature `1.0` (`classify.go`) are constants.
- The 30 s fetch timeout and 120 s embed timeout are constants.

**Legacy fat-domains** — `load` detects a pre-split `domains.parquet` (by a `nace_top3_codes` column) and
fans it into `domains` + `industries` + `page_signals` + `contacts`, skipping the normal split loads for
those kinds. Modern output never takes this path.
````

- [ ] **Step 2: Verify the constants + the empty-dir rule.**

Run: `rg -n '30\*time\.Second|Timeout:|MaxChars|TechMaxBytes|is not empty|nace_top3_codes|0\.55|confTemp' cc-enrich-worker/cmd/cc-enrich-worker/main.go cc-enrich-worker/internal/embed/embed.go cc-enrich-worker/internal/classify/classify.go cc-enrich-worker/internal/load/load.go`
Expected: the 30 s fetch timeout, the 120 s embed client timeout, the non-empty-dir fatal for non-embed modes, the `nace_top3_codes` legacy detection, and the `0.55`/`confTemp` constants. If any constant changed, update the "not configurable" list.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — limits, gotchas & invariants"
```

---

## Task 9: Internal packages + final review

**Files:**
- Modify: `cc-enrich-worker/README.md` (append)

- [ ] **Step 1: Append the section.**

````markdown
## Internal packages

| Package | Role |
|---|---|
| `fetch` | `RangeGetter` (signed `S3Getter` + anonymous CDN getter); `FetchRecord` byte-range-reads a WARC record and unpacks gzip → HTTP headers+body. 30 s/record, backoff on transient errors. |
| `embed` | `EmbedClient`: POSTs to the OpenAI-compatible `/embeddings` endpoint with batching + a concurrency pool; caps text length; retries transient failures; L2-normalizes the output. |
| `parse` | `ParseHTML` → visible text; email regex (deduped); social-platform link extraction. |
| `tech` | `DetectTech`: dispatches to the fast Aho-Corasick-gated matcher (`--tech-engine=fast`) or upstream `wappalyzergo`; returns `[]Technology{Name,Category,Version}`. |
| `extract` | `ExtractProfile` (schema.org Organization JSON-LD), `ExtractLEIs`, `ExtractVATs` (checksum-validated). |
| `classify` | `LoadReference` (NACE matrix + page-type prototypes from ClickHouse), `Classify` (cosine + page-type signals), `VerifyReference` (startup model/dim match). |
| `output` | `*Row` structs (parquet+ch tags), `Write*` funcs, S3 upload. |
| `load` | `FromFile`/`FromDir`, the generic `Insert[T]`, the filename→table map, legacy fan-out. |
| `worker` | `ShardConfig`, `ProcessIndustryStream`, `FetchChunk`+`Finalize`, per-domain aggregation. |
| `vec` | `Dot`, `Norm` (L2). |
| `model` | shared types: `Reference`, `Prototypes`, `WorklistItem`, `Technology`, `Identifier`, `CompanyProfile`, `DomainResult`. |

---

*Worker CLI + internals reference. Pipeline/driver: [`../README.md`](../README.md) · ClickHouse columns:
[`../docs/schema.md`](../docs/schema.md) · embeddings: [`../docs/embeddings-design.md`](../docs/embeddings-design.md).*
````

- [ ] **Step 2: Whole-doc verification — every documented flag/env/table is real, and the build is green.**

Run:
```bash
# no documented flag is missing from source, and vice-versa (spot the diff):
diff <(rg -o -- '--[a-z-]+' cc-enrich-worker/README.md | sort -u) <(rg -o 'fs\.\w+\("([a-z-]+)"|flag\.\w+\w*\("([a-z-]+)"' -r '--$1$2' cc-enrich-worker/cmd/cc-enrich-worker/main.go | sort -u) || true
make -C cc-enrich-worker build
rg -n 'TODO|TBD|FIXME|<.*>|XXX' cc-enrich-worker/README.md || echo "no placeholders"
```
Expected: the flag diff shows only intentional differences (e.g. `--out` appears in both); the build is green (proves the source cited still compiles as documented); the placeholder scan prints "no placeholders". Fix any flag the diff reveals as documented-but-absent or present-but-undocumented.

- [ ] **Step 3: Commit.**

```bash
git add cc-enrich-worker/README.md
git commit -m "docs(cc-enrich-worker): README — internal packages + cross-links"
```

---

## Self-review (run after all tasks)

1. **Spec coverage** — the request was "all options + logic + limits". Options → Tasks 3/4/5 (subcommands, flags, env). Logic → Task 6. Limits → Task 8. Output/packages → Tasks 7/9. No gap.
2. **Placeholder scan** — Task 9 Step 2 greps for `TODO|TBD|FIXME|<…>`. Must print "no placeholders".
3. **Fact consistency** — Tasks 4/5/7/8 each re-grep the exact defaults/tables from source; the values in this plan (concurrency 32, chunk 1024, embed-concurrency 96, embed-batch 16, tech-max-bytes 131072, MAX_CHARS 2000, NATIVE_PORT 9000, fetch 30 s, embed 120 s) must survive those greps. **Source wins over this plan** if they ever disagree.
