# Dagster Source Pipeline Design — Country Registry Sources at Scale

Date: 2026-06-12
Status: Proposed
Scope: Design for the family of "country company-data source" pipelines (Finland
PRH XBRL is the first instance), structured to scale to 200+ countries with
several sources per country.

---

## 1. The problem family

Every source in corpscout is a variation of the same pipeline:

```
external registry API / bulk file
  → discover what exists (listing endpoint, date window, dump index)
  → download raw payloads (XML / JSON / CSV / ZIP)
  → store raw, immutable, in object storage (RustFS)
  → parse raw payloads into typed rows
  → load source-scoped tables in ClickHouse (raw-first schema)
  → derive curated/serving tables (rebuildable without re-download)
```

At target scale this is 400–1000 source packages. The design goal is **low
marginal cost per source**: adding source N+1 should mean writing a client, a
parser, and a spec — never orchestration plumbing, sensors, or glue.

The existing repo already has the right skeleton for this: `SourceBundle`,
`registry.py`, `source_scaffold.py`, the conventions test, and `prh_ytj` as the
first instance. This design extends that skeleton rather than replacing it.

---

## 2. What "idiomatic Dagster" means for this problem

These are the principles every source package follows. They are the design;
everything after this section is their application.

### 2.1 Model data at rest as assets, actions as jobs — and almost everything is data at rest

- An **asset** is a named, observable piece of data: an S3 prefix of XML files,
  a set of ClickHouse tables, a reference code map.
- The asset graph **is** the pipeline. There is no separate workflow definition,
  no sensor relaying state between systems, no hand-built "process completed
  run" trigger. If you find yourself writing a sensor that watches system A to
  launch a job in system B, the design is wrong — both sides should be assets
  in one graph.
- Plain ops/jobs are reserved for true *actions* that produce no data artifact
  (e.g., registering new dynamic partitions, requesting an ad-hoc pull).

### 2.2 A fixed layer vocabulary across all sources

Every asset carries a `layer` tag from a closed set. This is already started in
`prh_ytj`; make it the platform-wide contract:

| layer | meaning | storage | example |
|---|---|---|---|
| `external` | the remote system itself (`AssetSpec`, never materialized) | — | `source_system` |
| `raw` | byte-exact payloads as downloaded | RustFS | `raw_xml_documents` |
| `parsed` | lossless typed rows, raw-first schema | ClickHouse | `statement_tables` |
| `reference` | source/taxonomy code lists, labels | ClickHouse | `taxonomy_code_map` |
| `normalized` | source rows conformed to corpscout shapes | ClickHouse | `normalized_tables` |
| `mapping` | cross-source mapping tables | ClickHouse | `industry_nace_mappings` |
| `serving` | denormalized query/UI caches | ClickHouse | `company_explorer_cache` |

The value at 200 countries: a single Dagster UI filter (`layer=raw`,
`country=X`) gives a uniform operational view over every source, and asset
checks / dashboards can be written once against the vocabulary.

**Rule:** raw and parsed are separate assets, always. The raw layer is the
expensive, rate-limited, politeness-constrained step; the parsed layer must be
rebuildable from RustFS forever without touching the source API (re-parse after
a parser bug, schema extension, taxonomy update).

### 2.3 Partitions are the unit of work, retry, and backfill

The single most important per-source design decision is the **partition
dimension** of the raw asset. It determines what "retry this one piece" and
"backfill the last 5 years" mean in the UI. Three archetypes cover the sources
seen so far:

| archetype | partitioning | example sources |
|---|---|---|
| **Snapshot dump** — source publishes one full dataset | unpartitioned, run-keyed object layout (`runs/<run_id>/…`), "latest wins" downstream | `prh_ytj`, GLEIF, most national bulk CSV dumps |
| **Incremental window** — source is queryable by a date window | `MonthlyPartitionsDefinition` (or weekly/daily) on the window the *source API natively supports* | PRH XBRL (`all_financial_statements?registeredDateStart/End`), CVR changes feed |
| **Per-entity on demand** — targeted enrichment of specific companies | `DynamicPartitionsDefinition`, one partition per entity | targeted XBRL pull for a single business id, BRREG-style per-company enrichment |

Guidance for choosing:

- **Prefer static time windows over dynamic entity partitions for bulk
  coverage.** Time partitions are bounded (a few hundred months), backfills are
  first-class in the UI, and the discovery API usually paginates by date
  anyway. Dynamic partitions degrade in usability and performance somewhere in
  the tens of thousands; "one partition per company" does not survive a
  300k-company registry.
- Dynamic entity partitions are still right for the *on-demand* path (analyst
  wants company X now), as a secondary, low-volume mechanism.
- Both paths must converge on the same deterministic storage keys and the same
  parsed tables, so it never matters which path fetched a document.

### 2.4 Declarative automation instead of custom sensors

Layer-to-layer triggering uses `automation_condition=AutomationCondition.eager()`
on downstream assets (parsed follows raw, derived follows parsed) plus the one
built-in automation sensor. Cron enters the graph in exactly one place per
source: a schedule on the raw/discovery asset (e.g., "first of month, pull last
month's registration window"), default `STOPPED` per repo convention until the
source is production-ready.

Custom `@sensor` code is the escape hatch, not the default, and at this scale
it is also the thing that doesn't survive: 1000 hand-written sensors polling
external systems is an operational failure mode. Budget: zero custom sensors
for a standard source.

### 2.5 Idempotency lives in storage, not orchestration

Any partition of any asset must be safely re-runnable at any time, because at
this scale re-runs are constant (flaky registries, parser fixes, backfills).

- **RustFS:** deterministic object keys derived from business identity, never
  from run ids: `companies/<business_id>/<financial_date>.xml`. Re-download
  overwrites; identical bytes are a no-op. (Run-id-keyed layouts are only for
  the snapshot archetype, where the run *is* the identity.) No hashing of
  public identifiers in keys — readable keys are an operational feature.
- **ClickHouse:** every parsed/derived table is `ReplacingMergeTree(version)`
  keyed on the natural identity (`statement_key`, …) with a `parsed_at` /
  `derived_at` version column. Inserts are plain appends; dedup is the
  engine's job. No delete-before-insert, no mutations, no dlt-style load
  bookkeeping.
- Consequence: orchestration needs no memory. No cursors, no manifests, no
  ledger tables for "what was processed" — the answer is always derivable from
  storage (`raw objects under prefix` vs `statement_keys in ClickHouse`), and a
  cheap asset check can assert the two agree.

### 2.6 Resources own I/O; source code is pure

- `RustFSResource`, `ClickHouseResource` (both exist) are the only way assets
  touch storage. Extend the platform resources when a source needs a new verb
  (`ensure_bucket`, `list_keys`, `get_bytes`) rather than letting sources build
  their own boto3 clients.
- Source HTTP clients are plain classes in `client.py` taking `base_url` —
  trivially testable with `responses`, no Dagster import.
- Parsers are pure functions `bytes → rows` in `parser.py` — no I/O, no
  Dagster, testable with inline fixtures. This is where most of the per-source
  intellectual work lives, so it must be the most isolated, most tested file.
- Asset bodies are thin: resolve partition → call client/parser → write via
  resource → return `MaterializeResult` metadata. If an asset body exceeds
  ~40 lines, logic is leaking out of the pure modules.

### 2.7 Resilience with Dagster primitives, not a second orchestrator

- `retry_policy=RetryPolicy(max_retries=…, backoff=EXPONENTIAL)` on raw assets
  handles flaky registries.
- `op_tags={"dagster/concurrency_key": spec.SOURCE_NAME}` (already the scaffold
  convention) caps per-source parallelism for API politeness; Dagster
  concurrency pools give a global ceiling across all sources.
- **Temporal is not part of this pipeline family.** Decision rule for when an
  external workflow engine is justified: a *single* unit of work that cannot be
  decomposed into partitions and exceeds what one Dagster run should hold
  (multi-day streaming pulls needing mid-flight checkpointing/signals). Every
  source seen so far decomposes into windows or entities — so the answer is
  partitions, not Temporal. Temporal remains where it already earns its keep:
  the Go scheduler's interactive source actions. One pipeline, one
  orchestrator, one place to look when something fails.

### 2.8 Conventions are enforced by code, not review

This is the actual scaling mechanism for 200 countries:

- `SourceBundle` is the only registration contract (exists).
- `source_scaffold.py` generates the package; it should grow **one template per
  archetype** (§2.3) so a new source starts from the right shape, not from a
  blank page.
- `tests/test_source_conventions.py` (exists) is the law: layout, naming, tags,
  groups, layer vocabulary, "raw and parsed are separate assets", "no custom
  sensors", "schedules default STOPPED". Every new platform rule lands here
  first, so 400 packages cannot drift.

---

## 3. Per-source package shape

The scaffold's current layout, extended. One file, one responsibility:

```
dagster_corpscout/sources/<country>/<slug>/
├── __init__.py        # SourceBundle wiring only — nothing else
├── spec.py            # ALL declarative config: names, tags, bucket, base URL,
│                      #   table names, object-key functions, partition cadence.
│                      #   Pure constants + pure key functions. The only file
│                      #   another layer (UI, Go catalog) may mirror.
├── client.py          # HTTP client for the source API. No Dagster imports.
├── parser.py          # bytes → typed rows (raw-first). Pure. No I/O.
├── tables.py          # ClickHouse table names + ordered column lists
│                      #   (mirrors the migration; single source of truth for
│                      #   the insert path).
├── importer.py        # rows → ClickHouse via ClickHouseResource.insert_rows.
│                      #   Knows tables.py and the resource API, nothing else.
├── partitions.py      # the source's partition definitions
├── assets/
│   ├── external.py    # source_system AssetSpec
│   ├── raw.py         # download → RustFS (the only file that talks to the API)
│   ├── parsed.py      # RustFS → ClickHouse parsed tables
│   └── derived.py     # normalized / serving assets (later phases)
├── checks.py          # @asset_check quality gates (counts, identity match)
├── jobs.py            # asset jobs + the rare action op (register partitions)
└── schedules.py       # the source's cron spine, default STOPPED
```

ClickHouse migrations stay in `corpscout/clickhouse/migrations/` (platform
land, golang-migrate), one migration set per source, table prefix
`<cc>_<slug>_` (`fi_prh_xbrl_…`). `tables.py` mirrors the migration; the
conventions suite can later assert the mirror matches `system.columns`.

Naming contract (already established, restated as law):

- asset keys: `sources/<country>/<slug>/<asset_name>`
- group: `source_<country>_<slug>`
- tags: `country`, `source`, `source_name`, `layer`
- bucket: `source-<country>-<slug>`
- ClickHouse tables: `<cc>_<slug>_<table>`

---

## 4. Applied to Finland PRH XBRL

### 4.1 Asset graph

```
source_system (external)
   │
raw_xml_documents (raw, RustFS)                ── partitioned: monthly registration window
   │     companies/<business_id>/<financial_date>.xml
   │     eager ▼
statement_tables (parsed, ClickHouse)          ── same monthly partitions
   │     fi_prh_xbrl_statement_documents / _contexts / _units / _facts_raw
   │     eager ▼
taxonomy_code_map (reference, unpartitioned)   ── loaded from taxonomy artifacts
   │     eager ▼
metrics_long (normalized)                      ── derived from facts + code map,
         fi_prh_xbrl_metrics_long_v1              rebuildable without re-download
```

### 4.2 Why monthly registration-window partitions

The PRH discovery endpoint is natively
`all_financial_statements?registeredDateStart&registeredDateEnd&page` — the API
itself is windowed by registration date. So the partition dimension falls out
of the source: `MonthlyPartitionsDefinition(start_date=…)`. Each partition's
raw materialization = "list everything registered that month, download each
statement XML to its deterministic company key."

This gives, with zero extra code:

- full-registry backfill = a UI backfill over month partitions;
- "keep current" = a monthly schedule materializing the latest partition (plus
  a trailing re-run of the previous month to catch late registrations);
- bounded partition count forever;
- per-month retry/repair with visible per-partition status.

The earlier per-company/date-window design becomes the **secondary path**: a
small `pull_company_job` (op job, config = `business_id`) for targeted pulls.
It writes the same object keys and the parsed layer keys on
`statement_key = sha256(business_id:financial_date:xml_sha256)`, so the two
paths are idempotently convergent. Note the asymmetry is fine: raw objects are
keyed by *company*, partitions are a *download bookkeeping* dimension — the
parsed tables never care which partition fetched a file.

### 4.3 Parsed layer (from the schema spike)

The spike's raw-first schema (migration `000011`) is the right shape; the
design only adds the idempotency rule from §2.5: all four tables become
`ReplacingMergeTree(parsed_at)` on their natural keys (`statement_key` +
local id / `fact_ordinal`). Parser obligations, restated from the spike as the
contract for `parser.py`:

- preserve **all** dimensions on contexts and facts; denormalize `fi_dim:MCY`
  and `fi_dim:REF` members onto fact rows; `is_comparative` from presence of
  `REF`;
- never filter facts to known concepts in the raw layer;
- type values into `value_kind` + `numeric_value`/`date_value`/`text_value`
  while always keeping `raw_value`, `decimals`, `precision`;
- extract reported identity (`si289`, `si168`, `di120`, `di121`) into the
  document row; mismatches against requested identity become
  `validation_warnings`, never dropped rows;
- stamp `parser_version` so a parser fix + re-materialization visibly
  supersedes old rows through the replacing engine.

### 4.4 Quality gates as asset checks

`checks.py`, on `statement_tables` per partition:

- every raw XML object under the partition's downloaded keys has a
  `statement_key` in `fi_prh_xbrl_statement_documents` (raw↔parsed agreement —
  this *replaces* manifests and ledgers);
- `reported_business_id` matches requested for ≥ a threshold share, warn
  otherwise;
- `facts_count > 0` per document; documents with zero facts are flagged, not
  hidden.

Checks are the platform's answer to "how do we trust 1000 pipelines we don't
look at" — they surface in the UI per asset, uniformly.

### 4.5 What gets deleted from the current attempt

- The aborted Go scheduler workflow files/hunks (Task-0 cleanup from the
  previous plan still applies verbatim).
- From the previous Dagster plan: the entire Temporal strand (`temporal_*.py`,
  worker container, `TemporalResource`, completion sensor) and the dlt
  pipeline. `client.py`, storage-key helpers, and the test approach carry over.

---

## 5. Platform pieces this source should leave behind

Each first-of-archetype source pays a small platform tax so the next 198
countries don't:

1. **Scaffold archetype templates** — extend `source_scaffold.py` with
   `--archetype snapshot|window|entity`, generating the matching
   `partitions.py` + `assets/raw.py` + `assets/parsed.py` skeletons.
2. **Resource verbs** — `ensure_bucket`, `list_keys`, `get_bytes` on
   `RustFSResource`; nothing source-specific in resources, ever.
3. **Conventions test growth** — layer vocabulary enforcement, separate
   raw/parsed assets, partitioned-asset tagging, schedule-STOPPED rule.
4. **Standard metadata keys** — `documents_count`, `bytes_downloaded`,
   `rows_loaded_<table>`, `warnings_count` returned by every raw/parsed asset,
   so one health dashboard reads all sources.
5. **Concurrency model** — per-source `concurrency_key` (exists in scaffold) +
   a documented global pool budget; new sources inherit it by convention.
6. **Code-location sharding (later, structural)** — one `Definitions` for all
   sources is fine into the low hundreds of assets; before it isn't, split
   Dagster code locations by region (`dagster_corpscout.regions.europe`, …),
   each aggregating its `SourceBundle`s. `registry.py` already isolates this:
   the split is a registry refactor, not a source refactor. This is the one
   known cliff at 200-country scale; the SourceBundle contract is what makes
   it cheap to step over.

## 6. Testing strategy (per source, uniform)

- `client.py` — `responses`-mocked HTTP, asserts exact URLs/params.
- `parser.py` — inline XML fixtures modeled on the spike samples; this is the
  deepest test surface (dimensions, comparatives, value typing, warnings).
- `importer.py` — fake ClickHouse client recording `insert` calls; asserts
  table/column alignment with `tables.py`.
- assets — `dg.materialize()` with `moto` S3 + `responses`, one happy-path test
  per asset; thin bodies keep this cheap.
- platform — existing `test_definitions.py` / `test_source_conventions.py`
  grow assertions; they are the regression net for every future source.

## 7. Decision summary

| decision | choice | rejected alternative |
|---|---|---|
| orchestrator | Dagster only | Dagster + Temporal handoff (sensor glue, split failure domain) |
| pipeline model | partitioned assets, eager automation | jobs + custom sensors relaying run config |
| bulk unit of work | monthly registration-window partitions | dynamic partition per company (unbounded), per-run prefixes |
| raw storage keys | deterministic, company-keyed, readable | run-id prefixes, hashed ids |
| CH idempotency | ReplacingMergeTree + version column | append + dedup in orchestration; dlt load bookkeeping |
| load path | existing `ClickHouseResource` + `tables.py` mirror of migrations | dlt (schema-ownership conflict with golang-migrate) |
| processed-state tracking | derivable from storage + asset checks | manifests, ledger tables, sensor cursors |
| scaling mechanism | scaffold archetypes + conventions tests + SourceBundle registry | per-source bespoke wiring |
