# NACE Classification of Activity Texts — Design

**Date:** 2026-07-03
**Status:** Approved approach, spec pending user review
**Scope:** `corpscout/dagster_v3` (new classifier lib + Latvia asset) + one ClickHouse migration
**First consumer:** Latvia UR (`corpscout.lv_companies.activity_text_original`)

## Problem

Latvia UR provides free-text activity descriptions but no bulk per-company
NACE codes (VID keeps them lookup-only; the UR open dataset has none —
verified 2026-07-03). Norway already carries NACE-family SN2007 codes, so a
cross-country industry dimension needs Latvian activity text mapped to NACE.

The texts are semantically close to NACE categories but never verbatim
classifier phrasing ("lopkopība", "lauksaimniecības produktu ražošana un
realizācija", multi-line lists like "zemkopība\nlopkopība\ntirdzniecība",
occasionally charter prose). Exact matching is dead; this is semantic
classification.

## Approach (decided in discussion)

**Loader pattern, not a service.** The workload is bursty (≈46k distinct
Latvian texts once, then a daily trickle), the retrieval corpus is tiny
(615 classes — brute-force cosine, no vector store), and most EU registries
publish NACE-derived codes natively so few sources will ever need this.
Resumability comes from incremental cache inserts + anti-join, not a queue.
If classification ever becomes continuous/multi-source or needs an API, the
cache table is the contract a service could later front.

**Distinct-text economics, mirrored from the translator:** classify each
distinct `(table, column, text)` once into a hash-keyed cache table;
companies join by `cityHash64` exactly like `lv_companies_translated`.
Head-heavy distribution works hard ("lopkopība" once ⇒ 1,667+ companies).

## Reference corpus

`data.gov.lv/dati/lv/dataset/nace2` → `nace_2.csv` (CC0, updated
2025-01-23). Columns `nosaukums, kods, vecaka_kods, limenis, apraksts`;
996 records: 21 sections (level 0), 88 divisions (2), 272 groups (3),
**615 classes (4)** with median 471-char Latvian descriptions including
includes/excludes guidance. 20 classes have empty descriptions — they
inherit their parent group's description at embed time.

**Revision note:** this dataset is NACE Rev. 2. NACE 2.1 became effective
2025-01-01 and Latvian companies re-declared under it; Norway's SN2007 is
Rev. 2-based. v1 classifies into Rev. 2 for cross-country consistency; the
cache carries `classifier_version` so a later 2.1 pass can supersede rows
without schema change.

## Storage

### ClickHouse migration: `corpscout.nace2_classifier`

The ingested classifier (all levels, one row per code): `code`,
`parent_code`, `level`, `name_lv`, `description_lv`, ingest version. Also
serves as the join target for human-readable NACE names in views.

### ClickHouse migration: `corpscout.text_classifications`

Mirrors `text_translations`' key discipline:

```
source_table        String   -- e.g. 'corpscout.lv_companies'
source_column       String   -- e.g. 'activity_text_original'
source_text         String
source_text_hash    UInt64   -- cityHash64(source_text), computed in SQL
nace_code           String   -- primary 4-digit class; '' = classified-unknown
nace_candidates     Array(String)  -- ranked candidate codes incl. secondaries
confidence          Float32  -- cosine similarity of the chosen candidate
method              String   -- 'embedding+llm' (v1's only value)
model               String   -- adjudicating LLM id
classifier_version  String   -- e.g. 'nace2-2025-01-23'
version             Int64    -- unix seconds, one per job run
```

Same table engine family as `text_translations` (match the existing
migration; readers pick max(version) per key). Unknowns are stored with
`nace_code = ''` so the anti-join does not retry them forever; superseding
them later = insert with a newer version (or delete-and-rerun).

### View

`lv_companies_nace`: `lv_companies` joined to `text_classifications` by
hash, plus `nace2_classifier` for the class name. Kept separate from
`lv_companies_translated` (different enrichment, independent lifecycles);
consumers join both if they need both.

## Pipeline (dagster)

### Shared lib: `src/dagster_v3/defs/classifier/lib.py`

Source-agnostic machinery, sibling of `translator_load/loader.py`:

1. **Corpus load + embed** (per run — 615 texts, seconds on the production
   embedder): build one embed-document per class:
   `"{code} {name_lv}\n{description_lv or parent group description}"`,
   embed with the production embedder (Qwen3-Embedding-8B endpoint; env
   var name resolved at plan time from the existing commoncrawl usage).
   Held in memory as a numpy matrix; nothing persisted.
2. **Anti-join scan**: distinct `(text, hash)` from the source column not
   yet in `text_classifications` (same shape as the translation loaders'
   scan, hash computed in ClickHouse SQL).
3. **Segment-aware retrieval**: embed the whole text AND its newline-split
   segments (texts are short); candidate set = union of whole-text top-8
   and per-segment top-3 by cosine.
4. **LLM adjudication, batched**: ~10 texts per prompt; for each text the
   prompt lists its candidate codes with names and asks for the primary
   class (must be from the candidate set) plus optional secondaries, or
   UNKNOWN. Same OpenAI-compatible local endpoint as the translator
   (`local_llm`, qwen3:6b); strict JSON response, invalid responses retried
   once then the batch's texts marked UNKNOWN (never crash the run).
5. **Incremental flush**: insert classified rows every N texts (default
   500). A crashed run resumes from the anti-join on the next
   materialization — the translator's recovery property without its queue.

### Assets

- `defs/classifier/assets.py` → `nace2_classifier_clickhouse`: download
  `nace_2.csv` (dlt requests helper), parse, replace-load
  `corpscout.nace2_classifier`. Rarely changes; manual/monthly.
- `defs/latvia_ur/classification.py` → `latvia_ur_nace_classification`:
  deps `latvia_ur_clickhouse_companies` + `nace2_classifier_clickhouse`;
  runs the lib pipeline for `corpscout.lv_companies.activity_text_original`
  (the Latvian originals — the embedder is multilingual, so no dependency
  on the translation pipeline); wired into `latvia_ur_register_job`'s
  selection like the translation loader. MaterializeResult metadata:
  scanned / classified / unknown counts.

Per-source files own per-source knowledge (established convention); the lib
owns everything generic.

## Design decisions (stated for review)

1. **Primary + candidates, one row per text** — not one row per code.
   Multi-activity texts ("ražošana un realizācija") get a primary
   `nace_code` (the LLM's pick) and the rest of the ranked candidate set in
   `nace_candidates`. Rationale: single-value join column for analytics;
   the array preserves multi-activity information without exploding rows.
2. **Latvian originals, not English translations**, as classification
   input: removes cross-pipeline coupling and the embedder is multilingual.
   English translations remain available if evaluation shows LV retrieval
   underperforms (a method-tagged re-run can supersede).
3. **UNKNOWN rows are stored** (empty code) rather than left pending —
   charter prose and noise would otherwise be re-adjudicated every run.
4. **No persisted embeddings**: 615 corpus vectors are recomputed per run;
   text vectors are used once and discarded. Nothing to migrate when the
   embedder changes — just bump `classifier_version`/`model` and supersede.

## Error handling

- Embedder/LLM endpoint down: asset run fails (dagster surfaces it);
  incremental flush means completed work is kept; re-run resumes.
- Invalid LLM output: one retry, then UNKNOWN for that batch's texts.
- Classifier download failure: `nace2_classifier_clickhouse` fails without
  touching the existing table (replace-load only on successful parse of
  ≥900 records — sanity floor).

## Testing

- Lib unit tests (repo fake-client style): corpus doc construction
  (empty-description inheritance), candidate-set union logic, adjudication
  prompt/response parsing incl. UNKNOWN and invalid-JSON paths, incremental
  flush batching — fake embedder returning fixed vectors, fake LLM, fake CH
  client.
- Anti-join SQL shape tests (mirror `test_translator_load.py`).
- Env-guarded integration test (`RUN_TRANSLATION_INTEGRATION_TESTS=1`
  reused or a sibling var): real ClickHouse + real embedder, classify a
  handful of seeded texts end-to-end ("lopkopība" must land in 01.4x
  animal-husbandry classes as a sanity anchor).
- `dg check defs` + job-membership test updates (register job gains the
  classification asset).

## Out of scope (v1)

- Any source other than Latvia (Norway/Estonia/Czech have official codes).
- NACE 2.1 migration (versioned cache makes it a re-run, not a redesign).
- A classification API/service (cache table is the contract if ever needed).
- Human review tooling for UNKNOWN/low-confidence rows (query the table).
- Backfilling `nace_candidates` into a Norway-comparable multi-industry
  table (revisit when a concrete consumer exists).
