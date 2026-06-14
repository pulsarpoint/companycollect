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
- **Functions are future assets.** Every transform is a pure function
  (data in → data out, no hidden IO). These exact functions become Dagster
  asset bodies later with no rewrite. IO (reading raw from S3, writing Parquet)
  lives only at the notebook edges.
- **Reuse parsing.** The existing pure parse functions in the source packages
  are reused as-is; only their output is retargeted from ClickHouse rows to
  Parquet. No parser is re-implemented.

## Architecture

Two pure-function layers, both future Dagster assets:

```
raw files (S3)                          prh_ytj NDJSON + code lists, prh_xbrl XML + listings
  │  parse_*()   REUSED pure parsers → structured Parquet  (drop CH importer only)
  ▼
structured Parquet (local sample / S3)  source-native, one dataset per source
  │  build_*()   per-country, self-contained, pure → canonical
  ▼
canonical Parquet (8-table contract)    company, registrations, financials, company_websites, …
        (ClickHouse load = later, separate, out of scope)
```

### Parse layer (reused)

- Import the existing pure parse functions:
  - prh_ytj: the NDJSON record parser / normalizer in
    `dagster_corpscout.sources.finland.prh_ytj` (parser/normalizer modules).
  - prh_xbrl: `parse_statement_xml` in
    `dagster_corpscout.sources.finland.prh_xbrl.parser`.
- The analysis environment installs `dagster_corpscout` as a path dependency so
  these are imported, not copied (single source of truth). The parse functions
  are already independent of ClickHouse (they return row dicts; the CH importer
  is a separate step we simply don't call).
- The only new parse-side code is serializing parser output to Parquet
  (Polars/PyArrow), producing the **structured Parquet** datasets.

### Conform layer (self-contained per country)

- Pure functions, one per canonical output table, written locally in the
  country folder:
  - `build_company()` + `build_registrations()` — entity + per-country record
  - `build_financials()` — tall metric rows
  - `build_websites()` — `company_websites` rows
- Signature shape: structured inputs (Polars DataFrames) → canonical Polars
  DataFrame validated against the contract. No IO inside.

## File layout (`companies/analysis/finland/`)

```
finland/
  conformance/
    __init__.py
    structured.py        reused parsers → structured Parquet (thin Parquet-serialization wrappers)
    build_company.py     structured → canonical company + registrations   (pure, local)
    build_financials.py  structured → canonical financials                (pure, local)
    build_websites.py    structured → canonical company_websites          (pure, local)
    validate.py          assert a DataFrame matches a canonical table's columns/types
  finland_conformance.py marimo notebook: load raw (S3) → parse → conform → validate → write
  output/                canonical Parquet datasets (sample artifact)
  partitioning.md        partition-strategy doc, grounded in Finland's real numbers
  pyproject.toml         analysis env: marimo, polars, pyarrow, duckdb, boto3, dagster_corpscout (path dep)
```

Other countries get the same shape under their own folder, independently.

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

1. Load raw Finland files from S3 (prh_ytj latest snapshot NDJSON + code lists;
   prh_xbrl window listings + XML). IO lives here only.
2. Run reused parsers → structured Parquet (write to `output/structured/`).
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
- New source ingestion or corpscout-discovered websites/contacts.

## Dependencies & risks

- Raw Finland files must be present in S3 and the notebook env needs S3
  credentials. prh_ytj snapshot is live (should exist); prh_xbrl raw may be
  thin, so financials may be a small sample — acceptable for a structure
  reference.
- Reusing parsers couples the analysis env to `dagster_corpscout` via a path
  dependency. The parse functions are CH-independent, so the coming CH removal
  shouldn't break them; if that coupling becomes a problem, the parse functions
  can later be vendored into the country folder.

## Deliverables

1. `companies/analysis/finland/conformance/` pure functions (reused parse +
   local build + validate).
2. `companies/analysis/finland/finland_conformance.py` marimo notebook.
3. `companies/analysis/finland/output/` sample canonical Parquet.
4. `companies/analysis/finland/partitioning.md`.
