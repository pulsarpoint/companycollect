# Romania ONRC + Ministry of Finance Implementation Plan

> **Scope:** Plan only. Do not implement from this document without first completing Task 0's
> source-semantics and licence gates. Implement task-by-task with focused tests before production code.

**Goal:** Add Romania as a complete `dagster_v3` country source: ONRC registry companies, statuses,
CAEN/NACE industries, canonical website contacts/domains, and Ministry of Finance annual financial
metrics with reporting-category-specific, versioned indicator mappings and separate RON→USD conversion.

**Architecture:** Two isolated source packages share one small concrete data.gov.ro CKAN helper:

```text
data.gov.ro CKAN
  ├─ ONRC dated register + nomenclature packages
  │    → large CSV checkpoints in romania_onrc_source.duckdb
  │    → normalized companies / industries / contacts / domains
  │    → atomic ClickHouse replacement
  │
  └─ Ministry of Finance annual packages
       → paired TXT data + CSV schema files preserved in object storage
       → long source facts in romania_financials_source.duckdb
       → schema-fingerprint-specific canonical metric mapping
       → one row per CUI/year/reporting category
       → separate RON→USD conversion
       → atomic ClickHouse replacement

Romania outputs
  ├─ domains graph
  ├─ ro_company_financials_latest
  └─ companies_all
```

**Tech stack:** Dagster assets/jobs/schedules, `dlt.sources.helpers.requests`, DuckDB native CSV
reader and set-based SQL, shared S3-compatible `ObjectStoreResource`, ClickHouse migrations and
stage/`EXCHANGE TABLES`, shared `ExchangeRateClient`, pytest, `uv run dg`.

## Source facts verified while planning

- ONRC publishes a new dated register package roughly monthly. The package dated **2026-07-08** is
  CC BY 4.0 and contains `OD_FIRME.CSV` (~690 MB), `OD_STARE_FIRMA.CSV` (~91 MB),
  `OD_CAEN_AUTORIZAT.CSV` (~425 MB), representative files, and branches:
  `https://data.gov.ro/dataset/firme-08-07-2026`.
- A matching **2026-07-08 nomenclature package** contains `N_STARE_FIRMA.CSV`, `N_CAEN.CSV`, and
  `N_VERSIUNE_CAEN.CSV`, also CC BY 4.0:
  `https://data.gov.ro/dataset/nomenclatoare-08-07-2026`.
- The Ministry of Finance publishes one annual financial package with paired TXT data and CSV
  schema files. The **2025 package** has 14 TXT + 14 CSV resources (~97 MB total):
  `https://data.gov.ro/dataset/situatii_financiare_2025`.
- Financial file families are reporting regimes, not legal forms: normal long/short statements,
  abbreviated statements, IFRS reporters, NGOs, banks, insurers, IFNs, pension funds, brokers, and
  other regulated financial entities.
- `I1`, `I2`, ... are positional source indicators, not global meanings. For example `I13` is net
  turnover in a normal company file, planned nonprofit income in the NGO file, and provisions in
  the IFN file. The mapping key must therefore include the reporting category and schema version.
- The 2025 Ministry package currently has no explicit dataset-level licence value in CKAN, while
  earlier financial packages have published CC BY/OGL metadata. This is a release gate, not an
  assumption to paper over.

## Design decisions locked in

1. **Two source modules, two DuckDB files.** Use `romania_onrc` and `romania_financials`; do not
   couple their refresh cadence or DuckDB writer pools.
2. **Current-data-first release.** Ship the latest ONRC snapshot and latest complete financial year
   first. Add historical financial backfill only after the current pipeline passes live quality gates.
3. **Discover packages at runtime.** Query CKAN, select the newest matching package by
   `metadata_modified`, and pair ONRC register/nomenclature packages by snapshot date. Never hardcode
   resource UUIDs.
4. **Whitelist source resources.** ONRC phase 1 uses companies, statuses, authorized CAEN activities,
   and the three nomenclatures. Do not ingest representative/person files or birth data.
5. **DuckDB-native wide-file loading.** Stream large files with the dlt retrying session, verify
   `Content-Length`, then use DuckDB `read_csv(..., all_varchar=true)`; never create Python dicts per
   ONRC row.
6. **Native identity is the registration number.** `COD_INMATRICULARE` is the stable ONRC record and
   companion-file join key. Preserve CUI as the tax/financial join key. Contacts/domains and
   `companies_all.company_id` use registration number; financial rows carry CUI and join through
   `ro_companies` where a registration-number identity is needed.
7. **No invented primary industry.** ONRC publishes authorized CAEN activities but does not mark one
   as primary. Preserve all activities with `is_primary=0`; any single-industry projection must use an
   explicit documented fallback, preferably the latest financial filing's CAEN when present.
8. **Canonical contact pair from day one.** `WEB` creates a website fact in
   `ro_company_contacts` and a website-derived row in `ro_company_domains`, using the standard column
   sets and primary-domain election. Invalid/unparseable URLs remain contact facts but do not become
   domains.
9. **Financial raw files are durable evidence.** Preserve each TXT/schema pair in object storage under
   a deterministic key containing reporting year, category, CKAN package id, and resource id. Revised
   packages produce new objects; they never overwrite earlier source bytes. Use bucket
   `source-romania-financials` and prefix `romania_financials/year=<year>/category=<category>/`.
10. **Mappings fail closed.** Parse every companion CSV into an indicator catalog, fingerprint the
    ordered `(indicator_code, original_label)` schema, and require an exact curated mapping for that
    `(reporting_category, schema_fingerprint)`. An unknown fingerprint fails metrics publication while
    leaving raw files/facts available for review.
11. **Loss/deficit polarity is explicit.** Mapping rows carry a multiplier. For ordinary companies,
    net result is `I18 - I19`; never assume source loss values are already negative.
12. **Set-based transforms only.** Unpivot source columns and pivot canonical metrics with DuckDB SQL.
    No financial-row loops in Python.
13. **RON is preserved, USD is separate.** Canonical metrics store `*_amount_original` in RON. A
    separate asset fills USD and FX provenance using report period end date.
14. **Migration-owned ClickHouse schema.** Python only asserts tables and atomically replaces them.
    Raw source rows and payload hashes remain in DuckDB/object storage.
15. **Allocate migration numbers at implementation time.** The migration ledger is active; use the
    next unused sequence then and update `EXPECTED_MIGRATIONS`. Do not reserve numbers in this plan.

## Scope and non-goals

### Phase 1 release scope

- ONRC companies, statuses, addresses, websites, and authorized CAEN activities.
- ONRC status/CAEN/version nomenclatures.
- Canonical `ro_companies`, `ro_industries`, `ro_company_contacts`, and `ro_company_domains`.
- Latest complete Ministry financial year across every published reporting category.
- Canonical current financial metrics and RON→USD values.
- Romania legs in domains, `company_financials_latest`, and `companies_all`.

### Explicitly deferred

- Legal representatives, family-enterprise representatives, birth dates, and beneficial owners.
- Paid ONRC documents and CAPTCHA/account-gated sources.
- VAT API enrichment until a current official endpoint is verified.
- Historical financial years until the current-year mapping and quality checks are proven.
- Treating all authorized CAEN activities as if one were officially primary.

## Planned file structure

All paths are relative to `services/dagster_v3` unless prefixed with `../../`.

```text
src/dagster_v3/defs/romania_common/
  __init__.py
  ckan.py

src/dagster_v3/defs/romania_onrc/
  __init__.py
  assets.py
  resources.py
  tables.py
  normalized.py
  industries.py
  contacts.py
  clickhouse.py
  translation.py
  docs/romania_onrc-design.md

src/dagster_v3/defs/romania_financials/
  __init__.py
  assets.py
  resources.py
  raw_store.py
  parsing.py
  mapping.py
  metrics.py
  tables.py
  clickhouse.py
  data/financial_metric_mappings.csv
  docs/romania_financials-design.md

tests/
  test_romania_ckan.py
  test_romania_onrc_resources.py
  test_romania_onrc_normalized.py
  test_romania_onrc_industries.py
  test_romania_onrc_contacts.py
  test_romania_onrc_assets.py
  test_romania_financial_resources.py
  test_romania_financial_raw_store.py
  test_romania_financial_parsing.py
  test_romania_financial_mapping.py
  test_romania_financial_metrics.py
  test_romania_financial_assets.py
  test_romania_clickhouse_migrations.py

../../clickhouse/migrations/<next>_corpscout_ro_registry.*.sql
../../clickhouse/migrations/<next>_corpscout_ro_contacts_domains.*.sql
../../clickhouse/migrations/<next>_corpscout_ro_financial_metrics.*.sql
../../clickhouse/migrations/<next>_corpscout_ro_translated_view.*.sql
../../clickhouse/migrations/<next>_corpscout_ro_financials_latest.*.sql
```

## Target asset graph

```text
romania_onrc_companies_raw_duckdb ───────────────┐
romania_onrc_statuses_raw_duckdb ────────────────┤
romania_onrc_nomenclatures_raw_duckdb ───────────┼→ romania_onrc_companies_duckdb
                                                 │      └→ romania_onrc_companies_clickhouse
romania_onrc_activities_raw_duckdb ──────────────┴→ romania_onrc_industries_duckdb
                                                        └→ romania_onrc_industries_clickhouse
romania_onrc_companies_duckdb → romania_onrc_company_contacts_duckdb
                               └→ romania_onrc_company_contacts_clickhouse
                               └→ romania_onrc_company_domains_duckdb
                                  └→ romania_onrc_company_domains_clickhouse → domains_clickhouse

romania_financial_raw_files_s3
  → romania_financial_facts_duckdb
  → romania_financial_metrics_duckdb
  → romania_financial_metrics_usd_duckdb
  → romania_financial_metrics_clickhouse
  → ro_company_financials_latest_clickhouse
  → companies_all_clickhouse
```

## Global implementation constraints

- Run all Python/Dagster commands with `uv run` from the `dagster_v3` root.
- Do not use `from __future__ import annotations` in modules defining Dagster assets.
- Every asset writing `data/romania_onrc_source.duckdb` uses pool
  `romania_onrc_duckdb`; every asset writing `data/romania_financials_source.duckdb` uses pool
  `romania_financials_duckdb`.
- DuckDB file stems must differ from dlt dataset names.
- Use `dlt.sources.helpers.requests`; large streaming downloads also need whole-download retries and
  `Content-Length` verification.
- Reject unexpected empty required inputs before replacing any nonempty table.
- Keep all ClickHouse `String` values non-null or explicitly nullable in the migration.
- Do not export raw records or `source_payload_hash` to ClickHouse.
- Use `AssetSelection.assets(...).upstream()` for scheduled chains.
- Schedules stay default-STOPPED until a live full run and row-count audit pass.
- Preserve unrelated worktree changes and commit explicit paths only.

---

### Task 0: Baseline, licence, and source-semantics gate

**Files:** no production files modified.

- [ ] Record `git status --short`; do not touch unrelated Denmark/Ansible/Sweden work.
- [ ] Run `uv run dg list defs --response-schema`, then `uv run dg list defs --json`.
- [ ] Run `uv run dg check defs` and the current focused aggregate tests:

  ```bash
  uv run pytest tests/test_domains_assets.py tests/test_company_financials_latest.py tests/test_companies_all.py -q
  ```

- [ ] Query CKAN live and save bounded metadata fixtures for:
  - newest `firme-*` ONRC package;
  - same-date `nomenclatoare-*` package;
  - newest complete Ministry financial package;
  - newest revised package for the same reporting year, if one exists.
- [ ] Confirm the exact reuse/attribution terms for the current Ministry financial package. If the
  current package licence remains blank, mark financial publishing blocked and continue only with
  ONRC plus local sample development.
- [ ] Inspect bounded samples for delimiter, BOM/encoding, quoting, malformed rows, `CUI=0`, duplicate
  registration numbers/CUIs, multi-value `WEB`, and status/CAEN code coverage.
- [ ] Confirm CAEN version→NACE revision mapping from official nomenclature documentation.
- [ ] Confirm Ministry values are plain RON, determine whether period end is always 31 December, and
  compare each 2025 companion schema against 2024 updated schemas.

**Stop conditions:** uncertain licence, missing companion schema for a required financial data file,
unmatched ONRC/nomenclature snapshot dates, or a source semantic that would change identity grain.

---

### Task 1: Correct the Romania research handoff and write source design docs

**Files:**

- Modify `../../../companies/analysis/romania/README.md`
- Modify `../../../companies/analysis/romania/investigation.md`
- Modify `../../../companies/analysis/romania/source_inventory.json`
- Modify `../../../companies/analysis/romania/source_inventory.md`
- Modify `../../../companies/analysis/romania/license_notes.md`
- Modify `../../../companies/analysis/romania/schema_notes.md`
- Create both source design docs listed above.

- [ ] Replace the old “ANAF `/bilant` primary” recommendation with Ministry bulk financial files;
  keep `/bilant` as bounded validation/fallback only.
- [ ] Document the current ONRC register+nomenclature pairing and current Ministry package.
- [ ] Record licence certainty separately for ONRC and financials.
- [ ] Record the 14 financial reporting categories and the fact that indicator codes are scoped to
  category/schema fingerprint.
- [ ] Fill every mandatory design-doc section: ingest mode, contacts, NACE, translation, currency,
  schedule, DDL deviations, and known issues.

**Verification:** review links and JSON syntax; no invented source fields.

---

### Task 2: Add the concrete Romania CKAN discovery helper

**Files:** `romania_common/ckan.py`, `test_romania_ckan.py`.

- [ ] Write failing tests from recorded CKAN fixtures for package search, `package_show`, newest-package
  selection, resource-name normalization, and ONRC/nomenclature date pairing.
- [ ] Implement concrete functions, not an interface/factory:
  - `search_packages(query, organization)`;
  - `get_package(package_id)`;
  - `latest_dated_package(prefix)`;
  - `paired_onrc_packages()`;
  - `resources_by_normalized_name(package)`.
- [ ] Use the dlt requests session with retry/backoff and an explicit User-Agent.
- [ ] Reject ambiguous duplicate normalized names and packages without required resources.

```bash
uv run pytest tests/test_romania_ckan.py -v
```

---

### Task 3: Implement resilient ONRC raw checkpoints

**Files:** `romania_onrc/resources.py`, `tables.py`, `assets.py`, resource/asset tests.

- [ ] Write tests for full-stream retry, destination re-truncation, short `Content-Length`, BOM,
  `^` delimiter, filename normalization, required resource whitelist, and empty-file refusal.
- [ ] Implement one raw-load asset per checkpoint:
  - `romania_onrc_companies_raw_duckdb` (`OD_FIRME`);
  - `romania_onrc_statuses_raw_duckdb` (`OD_STARE_FIRMA`);
  - `romania_onrc_activities_raw_duckdb` (`OD_CAEN_AUTORIZAT`);
  - `romania_onrc_nomenclatures_raw_duckdb` (three small nomenclature files).
- [ ] Download to a temporary path and load with DuckDB native `read_csv`, `all_varchar=true`.
- [ ] Store a DuckDB source catalog with package/resource ids, URL, snapshot date, ETag,
  `Last-Modified`, bytes, SHA-256, row count, and source run id.
- [ ] Emit row/byte/package metadata from every asset.

```bash
uv run pytest tests/test_romania_onrc_resources.py tests/test_romania_onrc_assets.py -v
```

---

### Task 4: Normalize ONRC companies and statuses

**Files:** `romania_onrc/normalized.py`, `tables.py`, `assets.py`, normalization tests.

- [ ] Define explicit raw/normalized/export column tuples before implementation.
- [ ] Normalize one row per `COD_INMATRICULARE`, retaining CUI, EUID, legal name/form, registration
  date, address components, website, source lineage, and raw provenance in DuckDB.
- [ ] Decode status via `N_STARE_FIRMA`; preserve source code/label and derive a small canonical
  lifecycle status (`active`, `inactive`, `dissolved`, `unknown`) with tested rules.
- [ ] Do not manufacture VAT ids: store `RO+CUI` only as a candidate/root unless VAT registration is
  independently verified.
- [ ] Add quality gates: required identity nonempty, exact output grain, duplicate counts, CUI-zero
  count, status coverage, date parse rate, and source/output row reconciliation.

```bash
uv run pytest tests/test_romania_onrc_normalized.py -v
```

---

### Task 5: Build CAEN/NACE industries and canonical website contacts/domains

**Files:** `industries.py`, `contacts.py`, `tables.py`, `assets.py`, focused tests.

- [ ] Join authorized activities to `N_CAEN` and `N_VERSIUNE_CAEN` in set-based SQL.
- [ ] Produce `ro_industries`-shaped rows with registration number, original CAEN code/description,
  CAEN version, mapped NACE revision/code, mapping method/status, and `is_primary=0`.
- [ ] Build canonical contact facts from nonempty `WEB` values. Preserve source values even when URL
  parsing fails.
- [ ] Derive registrable domains with shared URL/domain helpers; use `domain_source='website'`,
  explicit-website confidence, and shared primary election.
- [ ] Test the canonical row helpers and verify at most one primary domain per registration number.

```bash
uv run pytest tests/test_romania_onrc_industries.py tests/test_romania_onrc_contacts.py -v
```

---

### Task 6: Add ONRC ClickHouse schemas, exports, and translation

**Files:** next migrations, `clickhouse.py`, `translation.py`, asset and migration tests.

- [ ] Add migration-owned `ro_companies`, `ro_industries`, `ro_company_contacts`, and
  `ro_company_domains` tables plus a translated company view.
- [ ] Assert canonical contact/domain DDL with `tests/canonical_contact_tables.py`.
- [ ] Implement atomic exporters with explicit export columns and empty guards. Contacts/domains may
  be legitimately empty only when the source actually contains no usable values.
- [ ] Add translation loading for finite Romanian status labels; use NACE reference English labels
  for industries rather than translating CAEN free text unnecessarily.
- [ ] Pin migration/export column order and register migrations in `EXPECTED_MIGRATIONS`.

```bash
uv run pytest tests/test_romania_clickhouse_migrations.py tests/test_clickhouse_migrations.py -v
uv run pytest tests/test_romania_onrc_assets.py -v
```

---

### Task 7: Preserve and catalog the latest Ministry financial package

**Files:** `romania_financials/resources.py`, `raw_store.py`, `tables.py`, raw/resource tests.

- [ ] Define the 14 reporting-category slugs and normalization rules for inconsistent filenames.
- [ ] Pair each TXT data resource with exactly one CSV schema resource; fail on missing or ambiguous
  pairs.
- [ ] Write raw TXT and schema bytes to deterministic object keys containing year, category, package
  id/modified timestamp, and resource id. Write metadata into a durable DuckDB resource catalog,
  not an ad hoc pointer manifest.
- [ ] Preserve old objects when an annual package is revised.
- [ ] Refuse a package that has no normal-company files (`BL_BS_SL` and `UU`). Allow documented empty
  specialist categories.

```bash
uv run pytest tests/test_romania_financial_resources.py tests/test_romania_financial_raw_store.py -v
```

---

### Task 8: Parse schemas and financial rows into auditable long facts

**Files:** `parsing.py`, `tables.py`, `assets.py`, parsing/asset tests.

- [ ] Parse each companion schema as ordered `(source_indicator, original_label)` metadata and compute
  a stable SHA-256 schema fingerprint after encoding normalization.
- [ ] Load TXT with DuckDB native CSV parsing and unpivot `I*` columns into long facts in SQL.
- [ ] Store CUI, CAEN, reporting year/category, indicator code/label/value, source row number, source
  package/resource/object provenance, schema fingerprint, run id, and payload hash.
- [ ] Make replacement idempotent within `(reporting_year, reporting_category)` using a transaction:
  stage, validate, delete old scope, insert new scope.
- [ ] Add reconciliation checks between wide source rows, distinct CUIs, and long-fact counts.

```bash
uv run pytest tests/test_romania_financial_parsing.py tests/test_romania_financial_assets.py -v
```

---

### Task 9: Implement the versioned canonical indicator mapping

**Files:** `mapping.py`, `data/financial_metric_mappings.csv`, mapping fixtures/tests.

- [ ] Define canonical metric names needed by source detail and aggregate consumers: revenue, total
  revenue, total expenses, gross result, net result, fixed/current/total assets, cash, liabilities,
  provisions, equity, subscribed capital, and employees.
- [ ] Key mapping entries by reporting category + schema fingerprint + source indicator. Include
  expected original label, canonical metric, multiplier, and mapping version.
- [ ] Seed mappings from real 2025 schema fixtures for all 14 categories; do not infer mappings from
  indicator number alone.
- [ ] Fail metrics materialization on an unknown fingerprint, label mismatch, duplicate canonical
  assignment, or missing required normal-company mappings.
- [ ] Preserve unmapped specialist facts in the long fact table and report mapping coverage metadata.

```bash
uv run pytest tests/test_romania_financial_mapping.py -v
```

---

### Task 10: Build native RON metrics, then USD metrics

**Files:** `metrics.py`, `tables.py`, `assets.py`, metric tests.

- [ ] Pivot long mapped facts in set-based SQL to one row per `(CUI, reporting_year,
  reporting_category)`.
- [ ] Implement explicit derived rules, including `profit - loss` and `surplus - deficit`, and only
  derive total assets when the source components and accounting identity are documented.
- [ ] Store `mapping_version`, schema fingerprint, mapped/unmapped counts, currency `RON`, and inferred
  period-end provenance.
- [ ] Add a separate USD-conversion asset using `ExchangeRateClient`; employees remain unconverted.
- [ ] Add quality gates for duplicate grain, impossible empty normal-company output, missing FX,
  category counts, and mapped metric coverage.

```bash
uv run pytest tests/test_romania_financial_metrics.py -v
```

---

### Task 11: Add financial ClickHouse schema and export

**Files:** financial migration pair, `clickhouse.py`, `assets.py`, migration/export tests.

- [ ] Create `corpscout.ro_financial_metrics` at the canonical grain with original/USD pairs and FX,
  mapping, source, and resolution metadata.
- [ ] Keep CUI and reporting category explicit; do not collapse different reporting regimes before
  export.
- [ ] Export the entire verified accumulated DuckDB metrics table with stage + `EXCHANGE TABLES`.
- [ ] Refuse zero-row replacement and pin column order against the migration.

```bash
uv run pytest tests/test_romania_financial_metrics.py tests/test_romania_clickhouse_migrations.py -v
```

---

### Task 12: Integrate Romania into cross-country products

**Files:**

- Modify `defs/domains/{tables.py,assets.py}` and `tests/test_domains_assets.py`.
- Modify `defs/company_financials_latest/{tables.py,sql.py,assets.py}` and
  `tests/test_company_financials_latest.py`.
- Modify `defs/companies_all/{tables.py,sql.py,assets.py}` and `tests/test_companies_all.py`.
- Add the next migration pair for `ro_company_financials_latest`.

- [ ] Add `ro_company_domains` with `registry_id_type='registration_number'` to the canonical domain
  source config and add the Romanian domain-export asset dependency.
- [ ] Add `ro` to latest financials with a custom SELECT: choose the latest year/category per CUI,
  apply a deterministic category precedence only for duplicate CUI/year cases, join `ro_companies`
  on CUI, and emit registration number as the cross-product company id.
- [ ] Add `ro` to `companies_all`, using registration number as company id, `ro_industries` for
  authorized activities, and `ro_company_financials_latest` for financials.
- [ ] Preserve exact per-country row-count parity gates in `companies_all`.

```bash
uv run pytest tests/test_domains_assets.py tests/test_company_financials_latest.py tests/test_companies_all.py -v
```

---

### Task 13: Jobs, schedules, and operational controls

**Files:** both `assets.py` modules and both design docs.

- [ ] Define `romania_onrc_register_job` using the full upstream selection; rely on the assets' explicit storage pools for concurrency.
- [ ] Schedule ONRC monthly after the expected publication window, staggered from existing sources,
  default STOPPED.
- [ ] Define separate financial ingest and publish jobs so a raw/package failure cannot partially
  replace ClickHouse.
- [ ] Schedule current financial-year refresh monthly during filing/publication season and less
  frequently afterward; keep default STOPPED until publication cadence is measured.
- [ ] Document disk headroom for ~1.3 GB ONRC input plus DuckDB working space, runtime expectations,
  object-store retention, retry behavior, and recovery commands.
- [ ] Ensure every asset returns source package/year/category, row counts, byte counts, mapping
  fingerprint/version, min/max periods, and data-quality counts where applicable.

---

### Task 14: Historical financial backfill

Begin only after Tasks 0-13 pass in production for the current year.

- [ ] Add yearly partitions covering the officially available history; use
  `BackfillPolicy.multi_run(max_partitions_per_run=1)` and the financial DuckDB pool.
- [ ] Resolve the latest revision per reporting year at runtime.
- [ ] Add schema fingerprints/mappings year by year. Unknown historical schemas stop only that year;
  never weaken the fail-closed rule.
- [ ] Backfill raw files and DuckDB metrics from the Dagster UI, with ClickHouse publication disabled
  during the sweep.
- [ ] Run one atomic full-history ClickHouse publish after all requested years pass quality checks.
- [ ] Compare `/bilant` for a bounded sample of companies/years as validation evidence only.

---

### Task 15: Final verification and release gate

- [ ] Run all Romania tests and impacted aggregate tests:

  ```bash
  uv run pytest tests/test_romania_*.py -v
  uv run pytest tests/test_domains_assets.py tests/test_company_financials_latest.py tests/test_companies_all.py -v
  uv run pytest tests/test_clickhouse_migrations.py -v
  uv run dg check defs
  ```

- [ ] Run bounded live smoke tests for CKAN discovery, one small nomenclature, one ONRC sample, and
  one normal + one specialist financial category.
- [ ] Materialize the current ONRC raw→normalized chain locally and reconcile source/output counts.
- [ ] Materialize current financial raw→facts→native metrics without ClickHouse; review mapping
  coverage and category counts.
- [ ] Apply migrations in a disposable ClickHouse, publish, and verify table counts, canonical contact
  DDL, at-most-one primary domain, RON/USD plausibility, and no exported raw/hash columns.
- [ ] Verify the Romania rows in domains, `ro_company_financials_latest`, and `companies_all`.
- [ ] Turn schedules on only after a successful production-sized run and documented runtime/disk audit.

## Completion criteria

- Romania's current ONRC snapshot and latest complete financial year materialize end-to-end.
- Every source package/resource is runtime-discovered and provenance is queryable.
- Status and CAEN codes are decoded from the matching nomenclature snapshot.
- All authorized CAEN activities are preserved without inventing a primary activity.
- Canonical contact/domain schemas conform exactly and feed the domain graph.
- Financial indicator meanings are selected by category + schema fingerprint and unknown schemas fail
  closed.
- Native RON and USD metrics are separate, auditable steps.
- ClickHouse exports are migration-owned, explicit-column, empty-guarded, and atomic.
- Romania appears in latest financials and `companies_all` with registration-number identity.
- Focused tests, impacted aggregate tests, migration tests, and `uv run dg check defs` pass.
- Both design docs and the country research handoff reflect the implemented reality.

## Principal risks

- **Financial licence metadata:** current package metadata must be resolved before production reuse.
- **Identity multiplicity:** duplicate/missing CUI must not collapse distinct ONRC registrations.
- **Source revisions:** annual packages can be republished; object keys and source catalogs must retain
  revision provenance.
- **Schema drift:** source indicator numbers are unsafe without fingerprinted category mappings.
- **Encoding drift:** Romanian schema labels have shown mojibake under naive UTF-8 decoding.
- **Industry semantics:** authorized CAEN is not necessarily primary CAEN.
- **PII scope:** representative files are intentionally excluded; future ingestion requires a separate
  privacy review and plan.
- **Resource pressure:** ONRC raw inputs exceed 1 GB; DuckDB transforms require explicit disk headroom
  and serialized writers.
