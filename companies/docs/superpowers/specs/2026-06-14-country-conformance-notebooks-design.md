# Per-Country Conformance Notebooks — raw source files → canonical Parquet

Date: 2026-06-14
Status: design (awaiting review)

## Goal

Build, per country, a self-contained marimo notebook that parses that country's
**raw** source files directly into **canonical Parquet** matching the 8-table
contract (`companies/analysis/_canonical/canonical_schema.md`), with an
accompanying partition-strategy document grounded in that country's real data.

Finland is the first (reference) country. The work establishes the pattern;
it does **not** wire anything into Dagster — that comes later.

## Constraints (decided)

- **Before Dagster.** Notebooks, canonical Parquet, and the partition doc all
  land before any Dagster configuration. The notebook is, for now, the
  conformance mechanism, run by hand.
- **ClickHouse is ignored.** The existing `fi_prhytj_*` / `fi_prh_xbrl_*`
  normalized ClickHouse tables are legacy and will be removed. Nothing reads
  from them. ClickHouse only ever receives the final canonical Parquet, in a
  later, separate step out of scope here.
- **Self-contained per country.** Each country folder is its own universe —
  written as if it is the only country that exists. No shared/generic library,
  no cross-country abstraction. Pattern extraction across countries is a
  deliberate future decision, not done preemptively.
- **Functions are future assets, shaped like asset bodies.** Every transform is
  a pure function (data in → data out, no hidden IO), written to mirror a Dagster
  asset materialization function as closely as possible: parameters are the
  upstream assets' outputs (DataFrames), the return is this asset's output. The
  whole pipeline then copies into Dagster by wrapping each function in `@dg.asset`
  — no logic rewrite. IO (reading raw from S3, writing Parquet) lives only at the
  notebook edges.
- **Reuse parsing by copying.** The existing pure parse functions are reused —
  but **copied into the country folder**, not imported. prh_ytj/prh_xbrl are the
  only sources that have such functions today; every future source starts from
  nothing, so each country folder must own a complete, standalone copy. Reuse =
  copy the tested logic, retarget output to Parquet, drop the CH importer.
- **This notebook is the template.** Finland is the worked example every other
  country notebook is copied from. It must be self-contained end to end, with no
  dependency on `dagster_corpscout`.

## Architecture

Three function layers, all future Dagster assets:

```
source URLs                             prh_ytj companies API + code lists, prh_xbrl discovery + statement XML
  │  download_*()   URLs → raw files in S3                                  [Phase 3, first asset]
  ▼
raw files (S3)                          prh_ytj NDJSON + code lists, prh_xbrl XML + listings
  │  parse_*()      COPIED pure parsers → structured Parquet (drop CH importer)   [Phase 5]
  ▼
structured Parquet                      source-native, one dataset per source
  │  build_*()      per-country, self-contained, pure → canonical                [Phase 6]
  ▼
canonical Parquet (8-table contract)    company, registrations, financials, company_websites, …
        (ClickHouse load = later, separate, out of scope)
```

### Acquire layer (download, first asset)

- A `download_*()` function per source fetches raw files from the source URLs
  (constants in `download.py`, Phase 2) and writes them **directly to S3** — no
  local staging. This is the
  first asset. Download logic is reused-by-copying from the existing source
  clients, same as the parsers.
- prh_xbrl acquisition is bounded for the reference (e.g. one registration
  month); if eligibility filtering is applied, the eligible business-id list
  derives from the prh_ytj structured output, not from ClickHouse.

### Parse layer (reused by copying)

- Copy the existing pure parse functions into the country folder:
  - prh_ytj: the NDJSON record parser / normalizer logic from
    `dagster_corpscout.sources.finland.prh_ytj` (parser/normalizer modules).
  - prh_xbrl: `parse_statement_xml` from
    `dagster_corpscout.sources.finland.prh_xbrl.parser`.
- Copied, not imported: these are the only sources that have such functions, and
  every future country starts from scratch, so the folder must be a complete
  standalone example. The parse functions are already ClickHouse-independent
  (they return row dicts; we never call the CH importer).
- The only added parse-side code serializes parser output to Parquet
  (Polars/PyArrow), producing the **structured Parquet** datasets — themselves a
  future asset, hence persisted, not in-memory.

### Conform layer (self-contained per country)

- Pure functions, one per canonical output table, written locally:
  - `build_company()` + `build_registrations()` — entity + per-country record
  - `build_financials()` — tall metric rows
  - `build_websites()` — `company_websites` rows
- Signature mirrors a Dagster asset body: parameters are the upstream structured
  DataFrames (the future upstream assets), return is the canonical Polars
  DataFrame, validated against the contract. No IO inside — wrapping in
  `@dg.asset` later is the only change.

## Phased implementation

Implementation is **serialized into phases**. Each phase is a **separate commit**,
and each must be **confirmed working before the next begins** — no work runs ahead
of a confirmed phase.

| Phase | Deliverable | Confirm before proceeding |
|---|---|---|
| 1 — Environment | `notebook/pyproject.toml` + uv env (marimo, polars, pyarrow, duckdb, boto3, lxml) | `uv sync` succeeds; marimo + imports run |
| 2 — Source URLs | download endpoints for prh_ytj (companies + code lists) and prh_xbrl (discovery + statement XML), as constants in `conformance/download.py` | a probe request to each URL returns the expected shape |
| 3 — Download to S3 (first asset) | `conformance/download.py` — function(s) that download source files and store them directly in S3 | raw objects land at the expected S3 keys; re-runnable |
| 4 — Analysis method (pedagogical) | `notebook/analysis_method.md` — how to analyse the raw files with Polars and convert them to the proper Parquet shape | doc covers every source file/entity and the target structured schema |
| 5 — Parse to structured Parquet | `conformance/parse_*.py` (copied parsers + Parquet serialization) | structured Parquet produced and validates against the per-source structured schema |
| 6 — Build canonical tables | `conformance/build_*.py` — final canonical tables for ClickHouse | canonical Parquet validates against the 8-table contract; notebook runs end-to-end |

Phases map to the asset chain: Phase 3 = the download asset; Phase 5 = the
structured assets; Phase 6 = the canonical assets. Phase 4 is documentation that
gates Phase 5 — the analysis reasoning is written and agreed before transforms.

## File layout (`companies/analysis/finland/notebook/`)

All notebook-related files live under a `notebook/` child of the country folder,
keeping the conformance system separate from the country's other analysis docs
(investigation.md, data_model/, dossiers).

```
finland/
  notebook/
    conformance/
      __init__.py
      download.py         source URL constants + download → S3  (first asset)  (Phases 2–3)
      parse_prh_ytj.py    COPIED prh_ytj parser + Parquet serialization → structured Parquet   (Phase 5)
      parse_prh_xbrl.py   COPIED prh_xbrl parser + Parquet serialization → structured Parquet  (Phase 5)
      build_company.py    structured → canonical company + registrations   (pure, local)        (Phase 6)
      build_financials.py structured → canonical financials                (pure, local)        (Phase 6)
      build_websites.py   structured → canonical company_websites          (pure, local)        (Phase 6)
      validate.py         assert a DataFrame matches a canonical table's columns/types
    finland_conformance.py marimo notebook: download → parse → conform → validate → write
    analysis_method.md     pedagogical: analysing source files with Polars → Parquet   (Phase 4)
    output/                raw/ + structured/ + canonical/ sample artifacts
    partitioning.md        partition-strategy doc, grounded in Finland's real numbers
    pyproject.toml         analysis env: marimo, polars, pyarrow, duckdb, boto3, lxml (no dagster_corpscout)
```

Other countries get the same `notebook/` shape under their own folder, copied
from Finland and edited.

## Finland specifics

- **Sources:** `prh_ytj` (registry: company record, addresses, website,
  business lines) + `prh_xbrl` (financials). Joined on `business_id`.
- **Canonical tables populated (4 of 8):** `company`, `registrations`,
  `financials`, `company_websites` (registry-provided URL).
- **Empty by design (3 of 8):** `company_contacts`, `company_people`,
  `company_relationships` — Finland open data excludes email/phone, officers,
  owners, and group links. The notebook states this explicitly so empty reads as
  *known-absent*, not forgotten. (`persons` likewise empty.)
- **Entity resolution is trivial:** single key `business_id`, no source
  conflicts, no fuzzy matching. Per the contract, `registration_uid =
  "FI:" + business_id` (= `country:reg_no`), and `company_uid` is the LEI when
  present else the surrogate `"c:" + sha1("FI:" + business_id)` (Finnish open
  data rarely carries an LEI, so mostly the surrogate). This validates
  *structure*, not hard cross-country resolution — the correct thing to lock first.

## marimo notebook responsibilities

1. Download raw Finland files to S3 via `download_*` (or load existing): prh_ytj
   snapshot NDJSON + code lists, a bounded prh_xbrl sample. IO lives here only.
2. Run copied parsers → structured Parquet (write to `output/structured/`).
3. Run local `build_*` functions → canonical DataFrames.
4. Validate each against the canonical contract (`validate.py`).
5. Write canonical Parquet to `output/canonical/`.
6. Compute and display the cardinalities the partition doc needs (row counts per
   table, distinct companies, financial periods, sites per company).

## Partition doc (`partitioning.md`)

Reasons from the real numbers the notebook computes, refining the tentative
choices already in `canonical_schema.md`:

- per-table `PARTITION BY` + `ORDER BY` + `ReplacingMergeTree` version, with
  Finland's measured cardinalities as evidence;
- the ClickHouse partition-count concern projected across ~150 countries;
- small-country partition skew;
- the `company`-table partition tradeoff (`home_country` vs `cityHash64(uid)`).

## Validation

`validate.py` checks each canonical DataFrame against the contract: required
columns present, types compatible, declared key columns unique (e.g.
`registration_uid` unique, no null `company_uid`), and tall-table grain holds
(one row per metric per statement). Failures stop the notebook loudly.

## Out of scope (YAGNI)

- Fuzzy / cross-country entity resolution and survivorship (Finland needs none).
- Any shared/generic conformance library.
- ClickHouse table creation or loading.
- Dagster assets, schedules, or sensors.
- Sources beyond Finland's prh_ytj / prh_xbrl; corpscout-discovered websites/contacts.

## Dependencies & risks

- The download step needs network access to the PRH endpoints and S3 write
  credentials. prh_xbrl is large, so the reference uses a bounded sample (e.g.
  one registration month); eligibility filtering, if applied, derives from the
  prh_ytj structured output rather than ClickHouse.
- Parse functions are copied, not imported, so they can drift from the
  originals. Acceptable: the originals' ClickHouse path is being removed, so the
  country folder becomes the home of record for Finland's parse logic anyway.
  Each future country copies this folder as its starting template.

## Deliverables

1. `companies/analysis/finland/notebook/conformance/` pure functions (download +
   copied parse + local build + validate).
2. `companies/analysis/finland/notebook/finland_conformance.py` marimo notebook.
3. `companies/analysis/finland/notebook/analysis_method.md` (Phase 4 pedagogical).
4. `companies/analysis/finland/notebook/output/` sample raw + structured + canonical.
5. `companies/analysis/finland/notebook/partitioning.md`.
6. One commit per phase (6 phases), serialized and individually confirmed.
