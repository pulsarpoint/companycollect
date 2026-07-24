# Finland Company Signals Implementation Plan

> Source analysis:
> [`companies/analysis/finland/financial_data_gap_analysis.md`](../../../../companies/analysis/finland/financial_data_gap_analysis.md),
> [`companies/analysis/finland/source_inventory.md`](../../../../companies/analysis/finland/source_inventory.md),
> and
> [`companies/analysis/finland/search_attempts.md`](../../../../companies/analysis/finland/search_attempts.md)
>
> As-built source designs:
> `defs/finland_hilma/docs/finland_hilma-design.md`,
> `defs/ted_procurement/docs/ted_procurement-design.md`,
> `defs/esef_filings/docs/esef_filings-design.md`, and
> `defs/esma_firds/docs/esma_firds-design.md`.

**Goal:** Reuse FIRDS as the shared EU regulatory-instrument foundation, then
add evidence-backed financial-data, public-award, and current public-listing
signals to the main company list for Finland, with filters, detail evidence,
source freshness, and provenance.

**Product contract:** v1 is strictly two-state:

```text
green = confirmed positive evidence
gray  = no positive evidence, unsupported, unresolved, incomplete, or stale
```

There is no red state in v1. A stored `0` means “do not render green”; it does
not mean the company does not have financial statements, public contracts, or
a listing.

**Status legend:** checked items already exist in the current repository.
Unchecked items are still required for the Finland signal release. A checked
implementation item is not evidence that its production bootstrap, backfill,
rights review, or schedule enablement is complete.

## Architecture

```text
ESMA FIRDS full + delta + cancellations
  -> firds_instrument_events
  -> firds_instruments_current (all EU/EEA MICs and instrument types)
                                      │
EODHD symbols/prices enrichment ──────┼─> company_listings
GLEIF LEI→Finnish Y-tunnus identity ──┘          │
GLEIF ISIN→LEI fallback ─────────────────────────┘
                                                 │
PRH XBRL statutory statements ───────────────┐   │
ESEF consolidated IFRS statements ──────────>├───┤
Vero tax records (detail only, not statements)   │
                                                 │
Hilma CSV ──> fi_hilma_notice_winners ───────┐   │
TED XML ──> ted_notice_winners ──────────────┴───┤
                                                 v
                                      company signal summaries
                                                 │
                                                 v
                                          companies_all
                                                 │
                                                 v
                                  list icons, filters, facets,
                                  tooltips, and detail evidence
```

Source tables remain source-specific. Cross-source tables contain only
normalized evidence and summaries; they do not erase provenance or replace the
source tables. FIRDS, ESEF, GLEIF, EODHD, and TED remain country-neutral.
Finland is a downstream company-registry projection, not an ingestion filter on
those shared sources.

## Decisions fixed by the analysis and existing implementation

1. Keep the physical `companies_all.has_financials UInt8` column. Rename its
   UI meaning to **Financial data available**. Do not add a duplicate
   `has_financial_data` column.
2. Add physical `has_public_award UInt8` and
   `is_publicly_traded UInt8` columns to `companies_all`.
3. Use explicit UI filters:

   ```text
   financial_data = available | unknown
   public_award   = observed | unknown
   public_listing = current | unknown
   ```

4. Cross-country `country_code` values use the existing lowercase
   `companies_all` contract (`fi`, `se`, and so on). Source-specific fields
   retain their native uppercase codes (`FI`, `FIN`). FIRDS scope
   configuration also uses uppercase ISO2 (`FI`). Convert only at the
   projection boundary.
5. Initial public-listing scope is the Finnish equity venue segments:

   ```text
   XHEL  Nasdaq Helsinki
   FNFI  Nasdaq First North Finland
   SPFI  Spotlight Stock Market Finland
   ```

   `SPFI` is a valid current segment MIC even when its live equity population
   is empty. Keep it in the scope so a future listing does not require a
   product-contract change. Store the scope once in
   `esma_firds/listing_scopes.py`; do not reproduce it in ad hoc queries.
6. FIRDS is the required, reusable EU foundation for official instrument
   identity, exact venue, regulatory classification, and lifecycle state.
   Ingest all EEA records before building the Finland listing projection.
7. EODHD supplies operational symbols and prices. GLEIF supplies company
   identity resolution and an ISIN→LEI fallback where needed. Neither
   overrides fresh FIRDS lifecycle state for an EEA venue. EODHD’s
   subscription-gated ID Mapping endpoint is not a dependency.
8. The EODHD Helsinki exchange code may be used as vendor enrichment, but it
   must not be treated as proof that a symbol is on `XHEL`, `FNFI`, or `SPFI`.
   Exact segment identity comes from FIRDS.
9. No automatic name-only company matching is allowed for procurement,
   financial statements, or listings. Finland joins use an exact normalized
   Y-tunnus or a high-confidence LEI→GLEIF `registered_as`→Y-tunnus mapping.
10. Vero public corporate tax data remains a separate tax-data product. A tax
   record is not a financial statement and must not set `has_financials=1`.
   `taxable_income` must never be relabeled as accounting profit.
11. PRH XBRL is legal-entity statutory evidence. ESEF is normally consolidated
    IFRS evidence. Either may prove that financial data is available, but list
    revenue and fiscal year continue to come from
    `fi_company_financials_latest` until a statement-scope selector is designed.
12. Hilma is the national procurement source and TED is the EU-threshold
    source. Cross-source deduplication uses an exact normalized TED publication
    reference when available. Similar names, buyers, dates, or amounts are QA
    hints only and do not authorize automatic merging.
13. Hilma’s portal export is an operator-supplied manual snapshot until the AVP
    subscription key and production terms are approved. The existing external
    S3 asset remains non-materializable and no schedule pretends to refresh it.
14. Nasdaq and Spotlight web pages are validation sources only. Production
    current-listing state comes from FIRDS; vendor symbols and prices come from
    EODHD subject to its contract.

The current MIC scope must be rechecked against the official ISO 10383
registration-authority list before launch:
`https://www.iso20022.org/market-identifier-codes`.

## Global implementation constraints

- Follow `corpscout/services/dagster_v3/AGENTS.md`,
  `corpscout/services/dagster_v3/CLAUDE.md`, and
  `corpscout/services/dagster_v3/docs/data-source-guidelines.md`.
- Preserve the existing source-specific pipeline shape:

  ```text
  immutable raw object
    -> per-source DuckDB
    -> set-based normalization
    -> migration-owned ClickHouse table
  ```

- Put one shared Dagster pool on every asset that opens the same DuckDB file.
- Do not reimplement or fork the existing Finland YTJ, XBRL, Hilma, Vero, TED,
  ESEF, GLEIF, EODHD, or FIRDS ingestion pipelines for signal work.
- Refuse empty or unexpectedly short replacements.
- ClickHouse schema comes only from forward migrations. Register every new
  migration in `tests/test_clickhouse_migrations.py`.
- Publish with stage tables plus `EXCHANGE TABLES`.
- Keep raw payloads and payload hashes outside analytical ClickHouse tables
  unless they are operationally required.
- Preserve source slug, source run, source record/reference, and retrieval or
  resolution timestamps on evidence rows.
- A listing may be called “current” only while the FIRDS full baseline and
  required delta/cancellation sequence are inside their freshness SLAs.
- Historical financial-statement and award evidence does not disappear when a
  source becomes stale. Coverage metadata shows the stale refresh date.
- Do not expose unmatched Hilma/TED supplier identifiers in the UI.
- Do not add a generic identity service, registry interface, or ClickHouse
  facade. Keep Finland Y-tunnus normalization explicit and source-specific.

## Release boundaries

| Release | Outcome | FIRDS required? | Current repository baseline |
|---|---|---:|---|
| Foundation F | Reusable EU FIRDS full/delta/cancellation history and current state | Yes | Implemented; controlled live bootstrap remains |
| A | Correct financial label, green/gray renderer, and coverage model | No | PRH XBRL, ESEF, and `has_financials` exist; availability union/UI remain |
| B | Hilma + TED public-award signal, filter, and detail evidence | No | Both source pipelines and Finland detail evidence exist; summary/filter remain |
| C | FIRDS + GLEIF + EODHD reconciled Finland listing signal, filter, and detail evidence | Yes | Shared sources exist; Finland scope and reconciliation remain |
| D | Financial coverage audit, ESEF union, and Virre decision | No | Source ingestion exists; scope-safe availability projection remains |

Foundation F and the shared source pipelines are prerequisites, not Finland
forks. Releases A and B can be developed independently. Release C cannot
publish a Finland listing green signal until FIRDS is fresh and reconciled.

---

## Task 0 — Freeze Finland contracts and resolve launch gates

**Files:**

- Create or update source design documents under:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/docs/`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/docs/`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/docs/`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/esma_firds/docs/`
- Update:
  - `defs/finland_hilma/docs/finland_hilma-design.md`
  - `defs/ted_procurement/docs/ted_procurement-design.md`
  - `companies/analysis/finland/license_notes.md`

- [ ] Confirm the Finland v1 listing scope is `XHEL`, `FNFI`, and `SPFI`.
  Record operating-versus-segment MIC semantics and the observed current
  population of each venue.
- [x] Keep FIRDS country-neutral: source tables retain all EEA countries, MICs,
  and instrument classes; the Finland equity/MIC scope is downstream only.
- [ ] Confirm EODHD’s production rights for storing and showing symbol, ISIN,
  active/delisted state, and prices. Explicitly exclude the paid ID Mapping
  endpoint.
- [ ] Confirm Hilma export storage, reuse, attribution, and display rights for
  the manually downloaded search-results CSV.
- [ ] Approve the Hilma/TED supplier-ID policy:
  - raw snapshots remain in source storage;
  - only a valid normalized Y-tunnus that exactly matches
    `fi_companies.business_id` may create company evidence;
  - unmatched identifiers remain QA counts and are never returned by the
    backoffice;
  - names alone never create or merge company evidence.
- [ ] Confirm the Hilma/TED attribution text: source, source reference,
  snapshot/publication date, covered period, and Corpscout as processor.
- [ ] Confirm that Vero tax rows do not set `has_financials`.
- [ ] Confirm the statement-scope labels:

  ```text
  PRH XBRL: legal_entity_statutory
  ESEF:     group_consolidated_ifrs
  ```

- [ ] Record freshness SLAs:

  | Source | Expected cadence | Proposed stale threshold |
  |---|---|---:|
  | PRH XBRL incremental | daily | informational only for existing evidence |
  | ESEF filings | weekly | informational only for existing evidence |
  | Hilma manual export | operator supplied | informational; show snapshot date |
  | TED | monthly in current pipeline | 45 days |
  | GLEIF Golden Copy / ISIN→LEI | daily | 3 days |
  | EODHD reference symbols | weekly | 9 days |
  | FIRDS full/cancellation | weekly | 9 days |
  | FIRDS delta | daily | 3 days |

**Exit criterion:** privacy, identity, licensing, attribution, scope,
statement semantics, and freshness are written down. No later implementation
task needs to infer them.

---

## Task 1 — Add cross-country evidence and coverage schemas

This task is shared with the Sweden plan. Implement it once; do not create
Finland-specific duplicates.

**Files:**

- Create the next unused ClickHouse migration pair under
  `corpscout/clickhouse/migrations/`.
- Modify:
  `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`.
- Create:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/`.
- Test:
  `corpscout/services/dagster_v3/tests/test_company_signals.py`.

Create these cross-country tables:

```text
company_public_procurement_summary
  country_code             LowCardinality(String)
  company_id               String
  public_award_count       UInt32
  public_award_last_date   Nullable(Date)
  source_slugs             Array(String)
  source_updated_at        DateTime64(3, 'UTC')
  resolved_at              DateTime64(3, 'UTC')
  ORDER BY (country_code, company_id)

company_listing_summary
  country_code             LowCardinality(String)
  company_id               String
  listing_venue_count      UInt16
  listing_markets          Array(String)
  source_updated_at        DateTime64(3, 'UTC')
  resolved_at              DateTime64(3, 'UTC')
  ORDER BY (country_code, company_id)

company_signal_coverage
  country_code             LowCardinality(String)
  signal_name              LowCardinality(String)
  coverage_status          LowCardinality(String)
  coverage_from            Nullable(Date)
  coverage_to              Nullable(Date)
  source_slugs             Array(String)
  source_updated_at        Nullable(DateTime64(3, 'UTC'))
  resolved_at              DateTime64(3, 'UTC')
  caveat                   String
  ORDER BY (country_code, signal_name)
```

`coverage_status` is metadata only:

```text
unavailable | partial | complete
```

It does not change the v1 two-state storage contract.

All three tables use lowercase `country_code` values matching
`companies_all.country_code`. Source queries translate `FI`/`FIN` to `fi`
when publishing the summary or coverage row.

- [ ] Add `tables.py` constants and exact export-column tuples.
- [ ] Add migration contract tests covering every column and `ORDER BY`.
- [ ] Add only the stage/replace helpers not already supplied by the concrete
  ClickHouse utilities.
- [ ] Do not add a service interface or repository abstraction around
  ClickHouse.

**Exit criterion:** empty summary/coverage tables can be migrated safely
before any signal asset or UI change is deployed.

---

## Task 2 — Reuse and production-validate the EU FIRDS foundation

Foundation F is shared with Sweden and later EEA countries. Finland must add no
predicate to the ingestion path.

**Existing files:**

- `corpscout/services/dagster_v3/src/dagster_v3/defs/esma_firds/`
- migration `000164_corpscout_esma_firds`
- `tests/test_esma_firds_*.py`

### Existing asset graph

```text
esma_firds_full_raw_files_s3          weekly
esma_firds_delta_raw_files_s3         daily
esma_firds_cancellations_raw_files_s3 daily
  -> esma_firds_instrument_events_duckdb
  -> esma_firds_instruments_current_duckdb
  -> esma_firds_clickhouse
```

- [x] Resolve ESMA file URLs through the register/machine interface and store
  per-file manifests.
- [x] Preserve file kind, publication date, sequence, checksum, retrieval
  time, and raw-object key.
- [x] Parse decompressed XML with a namespace-aware streaming parser and
  bounded batches.
- [x] Preserve `NewRcrd`, `ModfdRcrd`, `TermntdRcrd`, and `CancRcrd` event
  semantics before deriving current state.
- [x] Apply events deterministically by source publication/sequence metadata.
- [x] Retain all EEA countries, MICs, and instrument classes.
- [x] Preserve ISIN, issuer LEI, venue MICs, CFI, competent-authority country,
  lifecycle dates/status, and source identity.
- [x] Cover new, modified, terminated, cancelled, late, multi-MIC,
  multi-country, and non-equity fixtures.
- [x] Put assets that open the FIRDS DuckDB file in one dedicated pool.
- [x] Define weekly full/cancellation and daily delta jobs/schedules.
- [x] Emit country, MIC, CFI, issuer-LEI, event-type, and source-date
  aggregates.
- [ ] Complete the controlled live bootstrap and validate production row
  thresholds, runtime, storage, and recovery behavior.
- [ ] Enable the stopped schedules only after the live baseline and first
  delta/cancellation application pass.
- [ ] Re-run the multi-country acceptance query after adding Finland’s scope
  and prove the scope changed no source-table row counts.

### Foundation acceptance checks

- [x] The same source/current tables contain multiple EEA countries.
- [x] Replaying the same baseline and event sequence is idempotent.
- [x] Modifications replace current attributes while history remains.
- [x] Termination/cancellation removes only the exact `(ISIN, MIC)` current
  relation.
- [x] Missing baseline/sequence input fails rather than publishing partial
  current state.
- [ ] Production freshness metadata is sufficient to suppress Finland
  current-listing green when full or delta input is stale.

**Exit criterion:** the shared source can answer current and historical exact
`(ISIN, MIC)` state for Finland’s venues without any Finland filter in
ingestion.

---

## Task 3 — Harden the existing Hilma award evidence

**Existing files:**

- `defs/finland_hilma/`
- migration `000147_corpscout_fi_hilma_notices`
- `tests/test_finland_hilma_parsing.py`
- `scripts/upload_hilma_export.py`

### Existing asset graph

```text
finland_hilma_export_s3 (manual external asset)
  -> finland_hilma_notices_duckdb
  -> finland_hilma_notices_usd_duckdb
  -> finland_hilma_clickhouse
```

- [x] Model the manually uploaded portal CSV as a non-materializable external
  asset.
- [x] Store immutable uploaded CSV objects and metadata under the Hilma source
  bucket.
- [x] Validate the exact 58-column export shape.
- [x] Transcode `cp1252` safely and parse semicolon-delimited quoted multiline
  rows.
- [x] Use DuckDB’s native CSV path and set-based transforms.
- [x] Deduplicate notices by `(notice_number, lot_id)` across accumulated
  exports.
- [x] Split all `//` winner entries and preserve winner order.
- [x] Normalize the trailing Finnish Y-tunnus when present without fabricating
  one from the winner name.
- [x] Publish migration-owned `fi_hilma_notices` and
  `fi_hilma_notice_winners` tables with stage/exchange replacement.
- [x] Preserve Hilma/TED reference, notice/lot identity, buyer, winner, CPV,
  publication time, values, currencies, and source run.
- [x] Convert monetary values through the shared exchange-rate boundary.
- [x] Keep the job manual and unscheduled.
- [ ] Add source materialization metadata for:

  ```text
  winner rows
  winners with syntactically valid Y-tunnus
  winners matched to fi_companies
  unmatched ids
  missing ids
  non-award rows
  duplicate notice/lot rows
  source export dates
  newest upload date
  ```

- [ ] Add a contract test that summary-eligible rows satisfy:

  ```text
  is_award = 1
  AND winner_business_id is a valid canonical Y-tunnus
  AND winner_business_id exactly matches fi_companies.business_id
  ```

- [ ] Refuse a replacement that is unexpectedly short relative to the newest
  previously accepted export set, not only one that is empty.
- [ ] Complete one controlled live refresh after the rights/attribution gate
  and record the resulting counts in the design doc.

**Exit criterion:** Hilma source evidence remains queryable independently,
only exact company matches become signal evidence, and unmatched identifiers
cannot leak through a backoffice query.

---

## Task 4 — Make the existing Finland TED pipeline country-safe and automatic

**Existing files:**

- `defs/ted_procurement/`
- migration `000148_corpscout_ted_procurement`
- `tests/test_ted_procurement_parser.py`
- `tests/test_ted_procurement_publish.py`

### Already implemented

- [x] Configure:

  ```python
  TedCountry(place_code="FIN", country_iso2="FI")
  ```

- [x] Normalize both canonical Y-tunnus and `FI` VAT-form identifiers:

  ```text
  2856390-5 -> 2856390-5
  FI28563905 -> 2856390-5
  ```

- [x] Partition the search by publication month from `2024-01-01`.
- [x] Download each eForms XML document once, preserve it in S3, and parse the
  buyer/winner linkage.
- [x] Preserve multi-lot, multi-tender, and multi-winner evidence.
- [x] Publish Finland notices and winners to ClickHouse.
- [x] Provide a Finland detail query that unions Hilma and TED and excludes
  exact Hilma/TED duplicates.
- [x] Verify a live Finland partition and company join rate in the source
  design.

### Shared work required before adding another country

The current `ORDER BY` and DuckDB dedupe are not country-safe when the same TED
publication is returned for more than one configured place country.

- [ ] Add a forward migration:

  ```text
  ted_notices:
    ORDER BY (country_iso2, publication_number)

  ted_notice_winners:
    ORDER BY (
      country_iso2,
      winner_national_id,
      publication_number,
      lot_id,
      tender_id,
      winner_ordinal
    )
  ```

- [ ] Deduplicate listing rows by `(country_iso2, publication_number)`.
- [ ] Keep XML organizations and winner links globally deduplicated by
  publication/lot/tender, then join them to country-scoped notice rows.
- [ ] Keep one listing row per matching country while downloading/parsing one
  XML document per publication and partition.
- [ ] Update the Finland detail query to join notices on both
  `country_iso2` and `publication_number`, with an explicit `FI` filter.
- [ ] Add a fixture where one publication appears in FIN and another
  configured country. Assert both country rows survive without cross-join
  duplication or ReplacingMergeTree collapse.
- [ ] Add a success sensor that monitors `ted_procurement_job` and requests
  `ted_publish_job` with an idempotent upstream-run key.
- [ ] Keep the sensor stopped until the Finland backfill and first automatic
  publish are validated.
- [ ] Backfill FIN from `2024-01-01` through the current month and record
  notice, winner, exact-ID, and company-match counts by month.
- [ ] Add a reconciliation check: every stored partition’s winner rows are
  present in the published ClickHouse table after the publish job.

**Exit criterion:** Finland TED winner evidence is complete for the declared
eForms window, publisher execution is automatic after successful parsing, and
adding another country cannot collapse or duplicate Finland rows.

---

## Task 5 — Build the Finland procurement summary and coverage row

**Files:**

- Add:
  `defs/company_signals/procurement.py`.
- Extend:
  `tests/test_company_signals.py`.

### Summary inputs

```text
fi_hilma_notice_winners JOIN fi_hilma_notices
  WHERE country_iso2 = 'FI' AND is_award = 1

UNION ALL

ted_notice_winners JOIN ted_notices
  WHERE country_iso2 = 'FI'
```

- [ ] Join each source to `fi_companies` by exact canonical Y-tunnus before it
  enters the summary.
- [ ] Keep source rows separate through normalization.
- [ ] Deduplicate cross-source rows only when Hilma carries an exact TED
  publication reference that normalizes to the TED publication number.
- [ ] Do not merge evidence on winner/buyer names, dates, titles, or amounts.
  Emit possible-duplicate QA counts instead.
- [ ] Define `public_award_count` as deduplicated observed award evidence, not
  total contracts, call-offs, revenue, or spend.
- [ ] Select `public_award_last_date` from the evidence publication/award date
  contract documented for each source.
- [ ] Materialize one row per `(country_code, company_id)` into
  `company_public_procurement_summary`.
- [ ] Materialize Finland’s `public_award` coverage row:

  ```text
  status: partial
  sources: finland_hilma, ted_procurement
  coverage: Hilma 2018–latest manual export; TED eForms 2024–current
  caveat: Hilma freshness depends on manual export; excludes awards absent
          from published award notices and evidence without a resolvable
          company Y-tunnus
  ```

- [ ] Depend on `finland_hilma_clickhouse` and
  `ted_publish_clickhouse`, not on their raw/intermediate assets.
- [ ] Refuse to replace the Finland summary when both sources are
  unexpectedly empty.
- [ ] Allow one source to be absent only with an explicit materialization
  reason and a partial/stale coverage row.
- [ ] Test that every summary company exists in `fi_companies`, counts are
  deterministic, and no company is duplicated.

**Exit criterion:** Finland companies can be filtered by observed public
award without a negative claim, and every green row links to at least one
source evidence row.

---

## Task 6 — Add the shared daily GLEIF ISIN-to-LEI bridge

This task is shared with the Sweden plan and every future listing projection.
Implement it once.

**Files:**

- Extend:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/`.
- Create a migration pair for `corpscout.gleif_isin_lei`.
- Extend:
  - `tests/test_gleif_source.py`
  - `tests/test_gleif_tables.py`
  - `tests/test_gleif_assets.py`

### Asset graph

```text
gleif_isin_lei_raw_file
  -> gleif_isin_lei_duckdb
  -> gleif_isin_lei_clickhouse
```

### Table

```text
gleif_isin_lei
  isin
  lei
  mapping_file_date
  source_run_id
  source_record_id
  source_retrieved_at
  resolved_at
  ORDER BY (isin, lei)
```

- [x] The existing GLEIF Golden Copy pipeline already maintains current LEI
  records, including `registered_as`, country, entity status, and source dates.
- [ ] Resolve the current ISIN→LEI file URL from the GLEIF download page; do
  not hardcode a date-bearing ZIP.
- [ ] Download daily, preserve the ZIP and manifest, and load with a set-based
  DuckDB reader.
- [ ] Uppercase and validate ISIN/LEI syntax without silently correcting
  malformed source values.
- [ ] Preserve multiple mappings per ISIN. Resolve ambiguity downstream.
- [ ] Emit row, distinct-ISIN, distinct-LEI, malformed, and duplicate counts.
- [ ] Extend the existing daily GLEIF job/schedule or add a separately
  staggered daily job.
- [ ] Use the existing GLEIF DuckDB pool only if the bridge opens the same
  file; otherwise give the bridge its own DuckDB file and pool.

**Exit criterion:** an ISIN resolves to zero, one, or multiple LEIs with
explicit provenance and no dependency on EODHD ID Mapping.

---

## Task 7 — Reconcile FIRDS and EODHD instruments to Finland companies

**Files:**

- Extend shared module:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/`.
- Extend or create migration tables:
  - `corpscout.eodhd_company_listings`
  - `corpscout.company_listings`
- Modify:
  `defs/esma_firds/listing_scopes.py`.
- Extend:
  `tests/test_company_listings.py` and
  `tests/test_esma_firds_listing_scopes.py`.

### Finland scope

Add:

```python
"FI": CountryListingScope(
    country_code="FI",
    mic_codes=frozenset({"XHEL", "FNFI", "SPFI"}),
    cfi_categories=frozenset({"E"}),
)
```

The exact code style may follow the implemented module, but the values and
single-source-of-truth contract are fixed.

### Source-specific EODHD resolution

Build the shared EODHD resolution with dependencies on:

```text
eodhd_symbols
eodhd_symbol_mics
esma_firds_clickhouse
gleif_isin_lei_clickhouse
gleif_reference_clickhouse
finland_ytj_resolved_clickhouse
```

Mapping:

```text
eodhd_symbols.isin
  -> current FIRDS (ISIN, MIC).issuer_lei
  -> gleif_lei_records.registered_as
  -> normalized Finnish Y-tunnus
  -> fi_companies.business_id

fallback only when FIRDS has no usable issuer LEI:
eodhd_symbols.isin
  -> gleif_isin_lei.lei
  -> gleif_lei_records.registered_as
  -> normalized Finnish Y-tunnus
  -> fi_companies.business_id
```

Finnish identity normalization must accept only documented forms:

```text
1234567-8 -> 1234567-8
12345678  -> 1234567-8
FI12345678 -> 1234567-8
```

Validate the Y-tunnus format/check digit. Reject rather than repair any other
shape.

- [ ] Filter vendor candidates to the agreed EODHD equity types
  (`Common Stock`, `Preferred Stock`, `Stock`) and emit rejected-type counts.
- [ ] Keep delisted vendor rows with `is_delisted=1`.
- [ ] Produce one resolution row per relevant vendor symbol, including
  unresolved rows:

  ```text
  eodhd_symbol_key
  country_code                 lowercase `fi`
  company_id
  isin
  issuer_lei
  mic
  instrument_type
  is_delisted
  identity_match_method
  identity_match_confidence
  unresolved_reason
  source_run_id
  source_retrieved_at
  resolved_at
  ```

- [ ] Use explicit unresolved reasons:

  ```text
  missing_isin
  isin_not_in_firds
  firds_issuer_lei_missing
  ambiguous_firds_venue
  isin_not_in_gleif
  ambiguous_isin_lei
  lei_not_finland
  invalid_registered_as
  invalid_business_id
  company_not_found
  ambiguous_company
  ```

- [ ] Never fall back to ticker, symbol, or company-name matching.
- [ ] Keep EODHD-only and delisted observations for QA/detail provenance, but
  do not let them create the current EEA listing green signal.
- [ ] Prefer an issued/active GLEIF record when duplicate LEI records exist,
  without hiding a genuinely ambiguous ISIN→LEI relation.

### Canonical company listings

Build `company_listings` from exact FIRDS `(ISIN, MIC)` current/history rows,
resolve the issuer LEI to `fi_companies`, then left-enrich with EODHD
ticker/name/price keys.

```text
country_code
company_id
issuer_lei
isin
ticker
instrument_name
instrument_type
venue_name
mic
market_type
segment
admission_date
termination_date
trading_status
is_current
identity_match_method
identity_match_confidence
listing_status_source
status_conflict
source_slug
source_record_id
source_retrieved_at
resolved_at
```

For Finland:

```text
is_current =
  exact (ISIN, MIC) is current in FIRDS
  AND FIRDS CFI is an allowed equity class
  AND MIC is in {XHEL, FNFI, SPFI}
  AND issuer LEI resolves exactly to fi_companies.business_id
  AND FIRDS weekly full snapshot is <= 9 days old
  AND required FIRDS delta/cancellation sequence is <= 3 days old
```

EODHD’s Helsinki exchange mapping may enrich a row, but it does not prove a
First North or Spotlight segment.

- [ ] Apply source precedence:

  ```text
  identity:
    FIRDS issuer LEI
    > GLEIF ISIN→LEI fallback

  in-scope EEA venue status:
    FIRDS exact (ISIN, MIC) lifecycle
    > EODHD active/delisted vendor state

  ticker and price:
    EODHD
  ```

- [ ] When FIRDS terminates/cancels an exact relation while EODHD still says
  active, retain both observations and set `status_conflict=1`; canonical
  status follows FIRDS.
- [ ] Build `company_listing_summary` only from `is_current=1`.
- [ ] Count distinct MICs and store a stable sorted `listing_markets` array.
- [ ] Materialize Finland’s `public_listing` coverage row with exact MIC/CFI
  scope and FIRDS, GLEIF, and EODHD dates.
- [ ] Reconcile after successful FIRDS current-state publication and the daily
  GLEIF bridge. EODHD may refresh weekly without overriding FIRDS state.
- [ ] Add historical “was traded on date T” tests based on FIRDS events.
- [ ] Establish a live Finland baseline from FIRDS and EODHD. Account for
  every candidate as matched or with an explicit unresolved reason.
- [ ] Do not use the approximately 298 active `Oyj` companies as an expected
  listing count. Legal form and current exchange listing are different facts.
- [ ] Keep foreign issuers in source QA but do not attach them to a Finland
  company by name.
- [ ] Assert no canonical listing row has an empty company ID, ISIN, or issuer
  LEI.
- [ ] Assert multi-class issuers produce multiple evidence rows but one company
  summary.
- [ ] Assert stale/sequence-incomplete FIRDS state produces zero Finland
  current-listing summaries.

**Exit criterion:** Finland’s current equity-listing signal is derived from
fresh exact FIRDS venue state and high-confidence registry identity, with EODHD
used only for operational enrichment.

---

## Task 8 — Extend `companies_all`

**Files:**

- Create a forward migration for the new signal columns.
- Modify:
  - `defs/companies_all/tables.py`
  - `defs/companies_all/sql.py`
  - `defs/companies_all/assets.py`
  - `tests/test_companies_all.py`
  - `backoffice/tests/companies-all-parity.test.ts`

### Physical columns

Keep:

```text
has_financials UInt8
fiscal_year Nullable(Int32)
```

Add:

```text
has_public_award          UInt8
public_award_count        Nullable(UInt32)
public_award_last_date    Nullable(Date)
is_publicly_traded        UInt8
listing_venue_count       Nullable(UInt16)
listing_markets           Array(String)
signals_resolved_at       DateTime64(3, 'UTC')
```

- [ ] Append columns to `COMPANIES_ALL_COLUMNS` in migration order.
- [ ] Join the two cross-country summary tables by
  `(country_code, company_id)` in every country leg.
- [ ] Derive:

  ```text
  has_public_award   = procurement summary row exists
  is_publicly_traded = listing summary row exists
  ```

- [ ] A missing row yields `0`, NULL counts/dates, and an empty market array.
- [ ] Add only the two summary asset keys to `companies_all_clickhouse`
  dependencies. Do not add Hilma, TED, FIRDS, EODHD, or GLEIF as direct
  dependencies.
- [ ] Preserve exact per-country row-count equality.
- [ ] Extend parity tests:
  - Finland green procurement rows exist in the procurement summary;
  - Finland green listing rows exist in the listing summary;
  - countries without summaries remain gray;
  - total and per-country row counts do not change.
- [ ] Keep Finland list revenue/fiscal year sourced from
  `fi_company_financials_latest`, even when an ESEF-only row makes
  `has_financials=1`.

**Exit criterion:** the unified table exposes Finland’s two new signals
without changing company grain or conflating statutory and consolidated
financial values.

---

## Task 9 — Add list icons, filters, coverage tooltips, and detail evidence

**Files:**

- Modify:
  - `backoffice/app/lib/filters.ts`
  - `backoffice/app/lib/unified.server.ts`
  - `backoffice/app/lib/company-list.server.ts`
  - `backoffice/app/components/data-table/unified-columns.tsx`
  - `backoffice/app/components/data-table/filter-sidebar.tsx`
  - `backoffice/app/components/companies/company-list-page.tsx`
  - `backoffice/app/routes/facet-options.ts`
  - `backoffice/app/lib/countries.ts`
  - `backoffice/app/lib/queries.server.ts`
  - `backoffice/app/routes/country-company-detail.tsx`
  - `backoffice/app/components/detail/public-contracts-section.tsx`
- Create:
  - `backoffice/app/components/companies/company-signal-icons.tsx`
  - `backoffice/app/components/detail/company-listings-section.tsx`
- Extend relevant backoffice tests.

### Unified row and filter contract

Return:

```text
has_financials
has_public_award
public_award_count
public_award_last_date
is_publicly_traded
listing_venue_count
listing_markets
signals_resolved_at
```

Replace the positive-only financial toggle with:

```text
financial_data:
  available -> has_financials = 1
  unknown   -> has_financials = 0

public_award:
  observed -> has_public_award = 1
  unknown  -> has_public_award = 0

public_listing:
  current -> is_publicly_traded = 1
  unknown -> is_publicly_traded = 0
```

- [ ] Whitelist all values in `parseUnifiedFilters`; never interpolate URL
  keys or values into SQL.
- [ ] Map old `f_has_financials=true` links to
  `financial_data=available` for one release.
- [ ] If both values of a facet are selected, omit its predicate.
- [ ] Return green and gray counts with `countIf`.
- [ ] For a Finland-locked list, compute counts from
  `companies_all WHERE country_code = 'fi'`.

### Main-list presentation

- [ ] Add one compact **Signals** column with accessible financial, award, and
  listing icons.
- [ ] Green and gray differ through text/accessible labels, not color alone.
- [ ] Use:

  ```text
  Financial data available
  Public award observed
  Currently equity-traded
  ```

- [ ] Never render a red/destructive state for `0`.
- [ ] Green icons link to:

  ```text
  #financial-data
  #public-awards
  #public-listings
  ```

- [ ] Gray icons are not evidence links.

### Coverage metadata

- [ ] Fetch `company_signal_coverage` once for countries present on the page.
- [ ] Pass a country/signal coverage map to the icon component; do not join
  caveat text onto every company row.
- [ ] Tooltips include sources, scope, latest refresh, status, and caveat.
- [ ] Gray copy says “No positive evidence in covered sources,” never “No.”
- [ ] Finland’s procurement tooltip shows the Hilma manual snapshot date and
  TED covered-through month separately.

### Finland detail queries

- [x] A Finland `publicContractsQuery` already unions Hilma and TED.
- [x] The existing public-contract component renders the canonical source,
  reference, date, buyer, title, amount, and currency shape.
- [ ] Make the existing query country-safe after the TED migration.
- [ ] Keep source labels and references on every row.
- [ ] Add a query against `company_listings` ordered by current state, venue,
  ticker, and ISIN.
- [ ] Render current and historical listing evidence separately with source,
  MIC, ticker, ISIN, issuer LEI, lifecycle dates, and retrieval date.
- [ ] Add `#financial-data` and show statutory versus consolidated scope.
- [ ] Keep Vero tax records in the existing tax section, not under financial
  statements and not as financial-green evidence.
- [ ] Keep Wikidata listings in the Wikidata enrichment section; do not use
  them to set the main signal.

### UI tests

- [ ] Type tests cover the expanded `UnifiedRow`.
- [ ] Query tests cover all six filter values and composed Finland filters.
- [ ] Facet tests assert green + gray equals the applicable company count.
- [ ] Rendering tests assert `0` is neutral and uses unknown wording.
- [ ] Rendering tests assert accessible labels and evidence links for green.
- [ ] Detail tests assert Finland Hilma/TED and canonical listing rows have no
  cross-country leakage.
- [ ] Run `pnpm typecheck` and the full backoffice test suite.
- [ ] Browser-smoke-test the global and Finland-locked lists, filters,
  tooltips, keyboard navigation, and detail anchors.

**Exit criterion:** users can see and filter all three Finland signals without
the UI making a negative claim or confusing tax, statutory, and consolidated
financial data.

---

## Task 10 — Build scope-safe Finland financial availability

**Existing files:**

- `defs/finland_xbrl/`
- `defs/esef_filings/`
- `defs/finland_verotax/`
- `defs/company_financials_latest/`

**Add:**

- A migration-owned `corpscout.fi_company_financial_availability` table.
- A concrete Finland asset under `defs/company_signals/financials.py`.
- Tests in `tests/test_company_signals.py`.

### Existing foundation

- [x] PRH XBRL listing and XML retrieval are implemented.
- [x] PRH statement documents, contexts, units, raw facts, taxonomy codes, and
  normalized financial metrics are published.
- [x] Daily PRH incremental ingestion and a complete publish chain exist.
- [x] `fi_company_financials_latest` produces one latest statutory metrics row
  per matched Finland company.
- [x] ESEF filings, facts, metrics, and LEI→registry mapping are implemented as
  a cross-country source.
- [x] The existing ESEF mapping normalizes Finnish `registered_as` values to
  Y-tunnus form.
- [x] Vero tax records are published separately and already have a dedicated
  Finland detail section.

### Availability table

Create:

```text
fi_company_financial_availability
  business_id
  has_prh_statutory        UInt8
  has_esef_consolidated    UInt8
  latest_statutory_period  Nullable(Date)
  latest_esef_period       Nullable(Date)
  source_slugs             Array(String)
  source_updated_at        DateTime64(3, 'UTC')
  resolved_at              DateTime64(3, 'UTC')
  ORDER BY (business_id)
```

- [ ] Add a reproducible audit:

  ```text
  fi_financial_statements companies
    -> fi_financial_metrics companies
    -> fi_company_financials_latest companies
    -> fi_company_financial_availability statutory rows
    -> companies_all has_financials

  esef_filings FI LEIs
    -> esef_financial_metrics usable filings
    -> esef_entity_registry_map FI registry ids
    -> fi_companies
    -> fi_company_financial_availability consolidated rows
    -> companies_all has_financials
  ```

- [ ] Classify losses as parser/mapping, taxonomy/metric availability,
  sentinel date, LEI/registry identity, company join, latest-row selection, or
  expected source-scope loss.
- [ ] Define “usable statement” explicitly. A source index row or empty metrics
  row alone must not set green.
- [ ] Preserve all valid ESEF filing versions in evidence. For availability,
  choose the latest usable version deterministically by the documented
  `fxo_id` version rule.
- [ ] Build one availability row per exact `fi_companies.business_id` from:
  - usable `fi_company_financials_latest` statutory evidence;
  - usable Finnish `esef_financial_metrics` joined through
    `esef_entity_registry_map`.
- [ ] Keep scope flags on the availability row; do not flatten statutory and
  consolidated statements into one numeric record.
- [ ] Derive Finland `has_financials=1` from availability-row existence.
- [ ] Continue sourcing `revenue_usd` and `fiscal_year` only from
  `fi_company_financials_latest`.
- [ ] An ESEF-only green company may have no list revenue/fiscal year. Do not
  substitute consolidated group revenue silently.
- [ ] Exclude `fi_tax_records` from the availability query and add a regression
  test proving a tax-only company remains financial-gray.
- [ ] Materialize Finland’s `financial_data` coverage row:

  ```text
  status: partial
  sources: finland_prh_xbrl, esef_filings
  caveat: PRH covers digitally filed statutory statements; ESEF covers
          regulated listed-issuer reports and is usually consolidated;
          paper/Virre-only statements are outside v1
  ```

- [ ] Complete controlled ESEF bootstrap/backfill and enable its stopped
  schedules only after counts, mapping, and rights checks pass.
- [ ] Treat Virre as a separate decision gate: confirm access, per-document
  cost, redistribution rights, and target cohort before opening a PDF/OCR
  implementation task.
- [ ] Re-run the documented legal-form coverage audit after the PRH+ESEF union.
  Do not treat every `Oyj` as listed or every listed issuer as PRH-XBRL
  eligible.

**Exit criterion:** Finland’s financial icon means Corpscout has a usable
statement, statutory and consolidated scopes remain visible, tax data cannot
create a false green, and projection loss is measured.

---

## Task 11 — Operational rollout and acceptance

### Deployment order

1. Resolve rights, attribution, scope, privacy, and freshness gates.
2. Apply shared summary/coverage and listing migrations.
3. Complete the FIRDS full baseline and validate multi-country/MIC/CFI/LEI
   coverage.
4. Apply FIRDS deltas/cancellations through the current sequence.
5. Materialize the GLEIF ISIN→LEI bridge and refresh GLEIF records.
6. Refresh EODHD reference symbols.
7. Build Finland EODHD resolution, canonical listings, listing summary, and
   coverage.
8. Refresh Hilma from an approved manual export and complete the Finland TED
   backfill/publish.
9. Build Finland procurement summary and coverage.
10. Build Finland financial availability and coverage.
11. Rebuild `companies_all`.
12. Run data-quality and parity checks.
13. Deploy the backoffice UI.
14. Enable stopped schedules/sensors only after successful controlled runs.

### Required validation commands

From `corpscout/services/dagster_v3`:

```text
uv run pytest <new and affected tests> -q
uv run dg check defs
uv run dg list defs --json
uv run python scripts/dagster-health-check.py
```

From `corpscout/services/backoffice`:

```text
pnpm typecheck
pnpm test
```

Use the repository migration mechanism and verify every new/changed table with
`system.columns` and `SHOW CREATE TABLE`.

### Data acceptance

- [ ] `companies_all` total and per-country row counts are unchanged.
- [ ] Every signal flag is exactly `0` or `1`.
- [ ] Every Finland financial green resolves to usable PRH or ESEF evidence.
- [ ] No tax-only company becomes financial green.
- [ ] Every Finland procurement green resolves to Hilma or TED evidence.
- [ ] Every Finland listing green resolves to a fresh current canonical
  listing in `XHEL`, `FNFI`, or `SPFI`.
- [ ] Every listing candidate is matched or has an explicit unresolved reason.
- [ ] Summary joins create no duplicate companies.
- [ ] Green + gray facet counts equal the applicable company count.
- [ ] Coverage rows exist for all three Finland signals.
- [ ] No unmatched supplier identifier is returned by a backoffice query.
- [ ] No name-only identity match exists in any signal table.
- [ ] UI/accessibility tests confirm there is no red/negative presentation.

### Observability

Every source, reconciliation, summary, and availability materialization should
report:

```text
raw rows
normalized rows
matched companies
unmatched rows by reason
duplicate/ambiguous rows
source publication/retrieval date
freshness status
output rows
duration
```

Alert or fail on:

- empty replacement;
- large unexplained row-count regression;
- stale or sequence-incomplete FIRDS;
- all Finland procurement/listing matches dropping to zero;
- Hilma export older than the declared coverage metadata;
- TED parsed partitions not reaching the publisher;
- PRH/ESEF availability collapsing unexpectedly;
- tax-only rows entering financial availability;
- summary/company join duplication;
- migration/export column drift;
- schedule/sensor failure or leaked Dagster pool slots.

## Suggested pull-request sequence

1. `feat(data): company signal and coverage schemas`
2. `fix(data): make TED publication grains country safe`
3. `feat(data): automate TED publish and backfill Finland`
4. `feat(data): harden Finland Hilma company evidence`
5. `feat(data): build Finland public-award summary`
6. `feat(data): ingest GLEIF ISIN to LEI mapping`
7. `feat(data): add Finland FIRDS listing scope`
8. `feat(data): reconcile FIRDS and EODHD listings to Finland companies`
9. `feat(data): build Finland financial availability across PRH and ESEF`
10. `feat(data): project company signals into companies_all`
11. `feat(backoffice): company signal icons filters and Finland evidence`
12. `chore(ops): validate and enable signal schedules`

Do not combine source hardening, identity reconciliation, the
`companies_all` migration, and the UI into one pull request. Evidence tables
and quality checks must be reviewable before their booleans become
user-visible.
