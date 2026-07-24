# Sweden Company Signals Implementation Plan

> Source analysis:
> [`companies/analysis/sweden/company-signals-analysis.md`](../../../../companies/analysis/sweden/company-signals-analysis.md)

**Goal:** Build FIRDS once as the shared EU regulatory-instrument foundation,
then add evidence-backed financial-data, public-award, and current
public-listing signals to the main company list for Sweden, with filters,
detail evidence, source freshness, and provenance.

**Product contract:** v1 is strictly two-state:

```text
green = confirmed positive evidence
gray  = no positive evidence, unsupported, unresolved, incomplete, or stale
```

There is no red state in v1. A stored `0` means “do not render green”; it does
not mean the company does not have financial statements, public contracts, or
a listing.

## Architecture

```text
ESMA FIRDS full + delta + cancellations
  -> firds_instrument_events
  -> firds_instruments_current (all EU/EEA MICs and instrument types)
                                      │
EODHD symbols/prices enrichment ──────┼─> company_listings
GLEIF LEI→country registry identity ──┘          │
GLEIF ISIN→LEI fallback ─────────────────────────┘
                                                 │
Bolagsverket/ESEF ──> financial evidence ────────┤
UHM CSV ──> se_uhm_procurement_awards ─────┐     │
TED XML ──> ted_notice_winners ────────────┴─────┤
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

The source tables remain source-specific. Cross-source tables contain only
normalized evidence and summaries; they do not erase provenance or replace
the source tables. FIRDS ingestion is country-neutral: Sweden is the first
company-registry projection, not a filter on the underlying FIRDS data.

## Decisions fixed by the analysis

1. Keep the physical `companies_all.has_financials UInt8` column. Rename its
   UI meaning to **Financial data available**. Do not add a duplicate
   `has_financial_data` column.
2. Add physical `has_government_contract UInt8` and
   `is_publicly_traded UInt8` columns to `companies_all`.
3. Use explicit UI filters:

   ```text
   financial_data = available | unknown
   government_contract = observed | unknown
   public_listing       = current | unknown
   ```

4. Initial public-listing scope is Swedish equity venues:

   ```text
   XSTO  Nasdaq Stockholm
   FNSE  Nasdaq First North Sweden
   XNGM  NGM Main Regulated
   NSME  NGM Nordic SME
   XSAT  Spotlight Stock Market
   ```

5. FIRDS is the required, reusable EU foundation for official instrument
   identity, exact venue, regulatory classification, and lifecycle state.
   Ingest all EEA records before building any country-specific listing
   projection; do not build a Sweden-only FIRDS pipeline.
6. EODHD supplies operational symbols and prices. GLEIF supplies company
   identity resolution and an ISIN→LEI fallback where needed. Neither
   overrides fresh FIRDS lifecycle state for an EEA venue, and the
   subscription-gated EODHD ID Mapping endpoint is not a dependency.
7. No automatic name-only company matching is allowed for procurement or
   listings.
8. Nasdaq, NGM, and Spotlight website endpoints are validation sources only
   until production storage/display rights are confirmed.

## Global implementation constraints

- Follow `corpscout/services/dagster_v3/AGENTS.md` and
  `docs/data-source-guidelines.md`.
- Every new bulk source uses:

  ```text
  immutable raw object
    -> per-source DuckDB
    -> set-based SQL normalization
    -> migration-owned ClickHouse table
  ```

- Put one shared Dagster pool on every asset that opens the same DuckDB file.
- Use DuckDB’s native CSV/XML processing path where possible; do not process
  the 115 MB UHM CSV row-by-row in Python.
- Refuse empty or unexpectedly short replacements.
- ClickHouse schema comes only from forward migrations. Register every new
  migration in `tests/test_clickhouse_migrations.py`.
- Publish with stage tables plus `EXCHANGE TABLES`.
- Keep raw payloads and payload hashes outside analytical ClickHouse tables
  unless they are operationally required.
- Preserve `source_slug`, `source_run_id`, `source_record_id`, and retrieval
  timestamps on evidence rows.
- A listing may be called “current” only while its status source is inside its
  freshness SLA.
- Historical financial-statement and award evidence does not disappear when a
  source becomes stale. The coverage tooltip shows the stale refresh date.
- Do not expose unmatched supplier identifiers in the UI.

## Release boundaries

| Release | Outcome | FIRDS required? |
|---|---|---:|
| Foundation F | Reusable EU FIRDS full/delta/cancellation history and current state | Yes |
| A | Correct financial label, green/gray renderer, coverage model | No |
| B | UHM + TED public-award signal, filter, and detail evidence | No |
| C | FIRDS + GLEIF + EODHD reconciled Sweden listing signal, filter, and detail evidence | Yes |
| D | Financial coverage audit, ESEF scope, paper-document decision | No |

Foundation F is the first data-source implementation. Releases A and B can be
developed independently, but Release C cannot publish a Sweden listing green
signal until the FIRDS foundation is fresh and reconciled.

---

## Task 0 — Freeze contracts and resolve launch gates

**Files:**

- Create or update source design documents under:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_uhm_procurement/docs/`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/docs/`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/docs/`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/esma_firds/docs/`
- Update:
  `companies/analysis/sweden/license_notes.md` if a gate is resolved.

- [ ] Confirm that the Sweden v1 listing scope is the five MICs above. Store
  the scope in one code/table contract, not separately in every query.
- [ ] Confirm the country-neutral FIRDS contract: source tables retain all
  EEA countries, MICs, and instrument classes; per-country listing products
  apply their own equity/MIC scope only in downstream projections.
- [ ] Confirm EODHD’s production rights for storing and showing symbol,
  ISIN, active/delisted status, and prices. The paid ID Mapping endpoint is
  explicitly excluded.
- [ ] Approve the UHM personal-data policy before production ingestion:
  - raw snapshots live in a restricted source bucket;
  - only exact ten-digit identifiers that match a ten-digit
    `se_companies.company_id` are published as company evidence;
  - unmatched identifiers are never returned by the backoffice;
  - 12-digit person-keyed `se_companies` rows are not attached to UHM
    evidence in v1.
- [ ] Confirm the UHM attribution text: source, snapshot date, covered period,
  and Corpscout as processor.
- [ ] Record Nasdaq/NGM/Spotlight as validation-only until rights are cleared.
- [ ] Record freshness SLAs:

  | Source | Expected cadence | Proposed stale threshold |
  |---|---|---:|
  | Bolagsverket financial bulk | weekly/current | informational only for existing evidence |
  | UHM awards | annual | informational only for existing awards |
  | TED | monthly in current pipeline | 45 days |
  | GLEIF ISIN→LEI | daily | 3 days |
  | EODHD reference symbols | weekly | 9 days |
| FIRDS full/cancellation | weekly (current observed register cadence) | 9 days |
| FIRDS delta | daily | 3 days |

**Exit criterion:** decisions are written into the source design documents;
no implementation task needs to infer privacy, scope, licensing, or freshness
semantics.

---

## Task 1 — Add cross-country evidence and coverage schemas

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

- [ ] Add `tables.py` constants and exact export-column tuples.
- [ ] Add migration contract tests covering every column and `ORDER BY`.
- [ ] Add small stage/replace helpers only where the existing ClickHouse
  replacement helpers do not already cover the operation.
- [ ] Do not add a generic service or interface around ClickHouse; the source
  asset should call the existing concrete helpers directly.

**Exit criterion:** empty tables can be migrated safely before any source
pipeline or UI change is deployed.

---

## Task 2 — Build the reusable EU FIRDS regulatory foundation

This is Foundation F and the first data-source workstream. It is not
Sweden-specific: ingest the complete EEA source once, then let Sweden and
later EU country projections select their configured MIC and instrument
scope.

**Files:**

- Create module:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/esma_firds/`.
- Create migrations for:
  - `firds_instrument_events`
  - `firds_instruments_current`
- Add fixtures and tests for discovery, streaming XML normalization, event
  application, current-state derivation, and replacement safety.

### Raw and normalized assets

```text
esma_firds_full_raw_files_s3          weekly
esma_firds_delta_raw_files_s3         daily
esma_firds_cancellations_raw_files_s3 daily
  -> esma_firds_instrument_events_duckdb
  -> esma_firds_instruments_current_duckdb
  -> esma_firds_clickhouse
```

- [x] Resolve all file URLs from ESMA’s register/machine interface and store a
  manifest for every ZIP/XML file.
- [x] Preserve file type, publication date, sequence, checksum, retrieval
  time, and raw-object key. Never treat a weekly full file alone as complete
  history.
- [x] Parse decompressed XML with a namespace-aware streaming parser
  (`lxml.iterparse` or the existing repository equivalent), clearing elements
  as they are emitted. Write bounded batches into DuckDB/Parquet; do not load
  a whole FIRDS XML file or build one Python object per complete file in
  memory.
- [x] Parse and preserve the event semantics:

  ```text
  NewRcrd
  ModfdRcrd
  TermntdRcrd
  CancRcrd
  ```

- [x] Preserve immutable event history before deriving current state. Apply
  events deterministically by ESMA publication/sequence metadata, not by
  download completion order.
- [x] Retain all EEA countries, MICs, and instrument classes in the FIRDS
  source tables. Do not filter the source ingestion to Sweden or equities.
- [x] Include at least ISIN, issuer LEI, full and relevant venue MICs, CFI,
  competent-authority country, admission/first-trade date, termination date,
  lifecycle event/status, source file identity, and source timestamps when
  present.
- [x] Add one configuration-backed country listing-scope mapping. The first
  projection is Sweden’s equity CFIs and five MICs; later EU countries add
  configuration and registry identity resolution without downloading FIRDS
  again.
- [x] Add fixtures for new, modified, terminated, cancelled, late-reported,
  multi-MIC, multi-country, and non-equity instruments.
- [x] Put assets that open the FIRDS DuckDB file in one dedicated Dagster
  pool. Refuse empty or unexpectedly short full-state replacements.
  Raw XML remains in immutable object storage; DuckDB stores parsed fields,
  source coordinates, and the per-record hash instead of duplicating the XML.
- [x] Define a weekly full/cancellation rebuild and daily delta application.
  Schedule daily work only after the latest successful full baseline and make
  missing sequence input a hard failure.
- [x] Publish coverage metadata per source file/type and enough country/MIC
  aggregates to detect an accidental country or venue collapse.

### Foundation acceptance checks

- [x] The same raw/current tables contain records for multiple EEA countries;
  no Sweden predicate exists in source ingestion.
- [x] Replaying the same full plus event sequence is idempotent.
- [x] A modification replaces current attributes but retains the earlier
  event.
- [x] A termination/cancellation removes the exact `(ISIN, MIC)` relationship
  from current status without deleting history.
- [x] A delta with a missing/unknown baseline or sequence fails rather than
  publishing a partial current table.
- [x] Country, MIC, CFI, issuer-LEI, event-type, and source-date counts are
  emitted as materialization metadata.
- [ ] Complete the first controlled live bootstrap, validate production row
  thresholds/runtime/storage, then enable the stopped schedules.

**Exit criterion:** Corpscout has one fresh, history-preserving FIRDS
instrument foundation reusable by every EEA company registry. It can answer
official current and historical `(ISIN, MIC)` state before any EODHD or
country-company reconciliation is attempted.

---

## Task 3 — Ingest Upphandlingsmyndigheten awards

**Files:**

- Create module:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_uhm_procurement/`
  with:
  - `__init__.py`
  - `assets.py`
  - `resources.py`
  - `normalize.py`
  - `tables.py`
  - `docs/sweden_uhm_procurement-design.md`
- Create a ClickHouse migration pair for
  `corpscout.se_uhm_procurement_awards`.
- Add fixtures under
  `corpscout/services/dagster_v3/tests/fixtures/sweden_uhm_procurement/`.
- Add tests:
  - `test_sweden_uhm_procurement_source.py`
  - `test_sweden_uhm_procurement_normalize.py`
  - `test_sweden_uhm_procurement_assets.py`

### Asset graph

```text
sweden_uhm_procurement_raw_snapshot_s3
  -> sweden_uhm_procurement_raw_duckdb
  -> sweden_uhm_procurement_awards_duckdb
  -> sweden_uhm_procurement_awards_clickhouse
```

Use one non-partitioned full refresh because the source publishes one complete
CSV snapshot.

### Raw download

- [ ] Download
  `https://catalog.upphandlingsmyndigheten.se/store/12/resource/239`
  with the dlt retry-capable HTTP session and whole-stream retry.
- [ ] Verify `Content-Length` when supplied and calculate SHA-256.
- [ ] Store the immutable object under a key containing retrieval date and
  digest. Write a manifest containing URL, ETag/Last-Modified, length, digest,
  retrieval time, and source run.
- [ ] Reuse an existing object when the digest has not changed.
- [ ] Refuse zero-byte, header-only, or materially truncated snapshots.

### DuckDB normalization

- [ ] Load with DuckDB `read_csv(..., all_varchar=true)` and the detected
  delimiter/encoding. Preserve original Swedish columns in the raw table.
- [ ] Normalize dates and the following source fields with set-based SQL:
  procurement ID, lot/tender-area ID, publication date, title, agreement
  type, contracted flag, buyer, supplier, CPV, and advertising database.
- [ ] Normalize a candidate supplier identity by stripping punctuation.
- [ ] Publish a company match only when:

  ```text
  contracted row
  AND normalized supplier id is exactly 10 digits
  AND it exactly matches se_companies.company_id
  AND the matched se_companies company_id is also 10 digits
  ```

- [ ] Retain an aggregate count for every unresolved reason in materialization
  metadata. Store unresolved source observations in the protected source
  table for aggregate market analysis, but never return person-keyed/private
  identifiers from company-facing queries.

### ClickHouse grain

One row per source award/supplier observation:

```text
se_uhm_procurement_awards
  company_id
  company_match_status
  match_eligibility
  source_slug
  source_run_id
  source_record_id
  source_line_number
  source_procurement_id
  source_lot_id
  publication_date
  title
  agreement_type
  contracted
  buyer_name
  buyer_id_normalized
  supplier_name
  supplier_id_normalized
  cpv_code
  advertising_database
  source_object_key
  source_retrieved_at
  resolved_at
```

Publish every normalized supplier observation. `company_id` is empty for an
unresolved observation, while `company_match_status` distinguishes `exact`,
`unmatched_company`, and the ineligible/private reasons. Only `exact` rows may
feed company evidence or `has_government_contract`.

Use a deterministic `source_record_id` from stable source fields plus source
row position. Do not claim it is a unique contract ID.

### Tests and checks

- [ ] Cover quoted delimiters, empty optional fields, malformed dates,
  multiple suppliers, duplicate evidence, corporate IDs with punctuation,
  12-digit person IDs, and unmatched IDs.
- [ ] Assert every normalized row is exported and only exact matches have a
  non-empty `company_id`.
- [ ] Assert the production full refresh is non-empty before exchange.
- [ ] After materialization, compare with the investigation baseline:
  approximately 102,785 source rows and approximately 18,564 distinct matched
  Swedish companies. Treat large deviations as review failures, not as fixed
  exact test constants.
- [ ] Add a stopped-by-default cadence-matched schedule. Monthly polling is
  sufficient for the current annual source release.

**Exit criterion:** all UHM supplier observations are queryable for protected
market analysis, exact evidence is queryable by Sweden company ID, provenance
is retained, and no person-keyed/unmatched supplier identifier is exposed by
company-facing backoffice queries.

---

## Task 4 — Enable and automate Swedish TED awards

**Files:**

- Modify:
  - `defs/ted_procurement/tables.py`
  - `defs/ted_procurement/assets.py`
  - `defs/ted_procurement/publish.py`
  - `defs/ted_procurement/docs/ted_procurement-design.md`
  - `backoffice/app/lib/countries.ts` for the existing Finland TED detail join
  - `tests/test_ted_procurement_publish.py`
  - `tests/test_ted_procurement_parser.py` if fixture coverage changes
- Create a forward ClickHouse migration that makes the TED table grains
  country-safe.

- [ ] Add:

  ```python
  TedCountry(place_code="SWE", country_iso2="SE")
  ```

  to `COUNTRIES`.

- [ ] Add Swedish national-ID normalization for both `SWE` and `SE`:

  ```text
  556533-8133   -> 5565338133
  5565338133    -> 5565338133
  165565338133  -> 5565338133
  ```

  A 12-digit identifier with a `19` or `20` person-century prefix must not be
  reduced to a ten-digit company ID.

- [ ] Keep the existing Swedish multi-winner XML fixtures and add a direct
  assertion that normalized winners join the digit-only Sweden company ID.
- [ ] Make the multi-country grain explicit before enabling Sweden. One TED
  notice can match more than one place-of-performance search, so the current
  `ORDER BY (publication_number)` notice key and country-free DuckDB dedupe
  are not safe for two countries:

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

- [ ] In `publish.py`, deduplicate notice listing rows by
  `(country_iso2, publication_number)`. Keep parsed XML organizations and
  winner links globally deduplicated by publication/lot/tender because the XML
  document is the same; joining them to the country-scoped notice rows then
  produces the intended country-scoped winner evidence.
- [ ] In the snapshot asset, keep one listing row per matched country but
  download/parse each publication XML once per partition.
- [ ] Update Finland’s existing detail query to join TED notices on both
  `country_iso2` and `publication_number` and to filter the Finland coverage
  row. Add the same explicit country join/filter to the Sweden query.
- [ ] Add a fixture where one publication appears in both FIN and SWE search
  results. Assert that both country-scoped notice/winner rows survive without
  cross-join duplication or ReplacingMergeTree collapse.
- [ ] Automate `ted_publish_job`. The current monthly schedule materializes
  the partitioned snapshot/parser assets but does not launch the unpartitioned
  ClickHouse publisher.
- [ ] Preferred orchestration: a success sensor monitoring
  `ted_procurement_job` and requesting `ted_publish_job` with the upstream run
  ID as `run_key`. Keep it stopped until the first Swedish backfill and publish
  are validated.
- [ ] Backfill Sweden from `2024-01-01` through the current month using the
  existing one-partition-per-run policy.

**Exit criterion:** Swedish TED winners are present in
`ted_notice_winners`, normalize to Sweden company IDs, and the publisher runs
after successful monthly parsing without a manual step.

---

## Task 5 — Build the procurement summary and coverage rows

**Files:**

- Add:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/procurement.py`.
- Extend:
  `test_company_signals.py`.

### Summary inputs

```text
se_uhm_procurement_awards
UNION ALL
ted_notice_winners JOIN ted_notices
  WHERE country_iso2 = 'SE'
```

- [ ] Keep source rows separate until the summary query.
- [ ] Before counting, deduplicate:
  1. exact TED publication number where the UHM row carries one;
  2. otherwise a conservative composite of company, buyer, source
     procurement/lot identifier, publication date, and normalized title;
  3. preserve a conflict flag in the DuckDB/source QA output when two rows
     look related but cannot be deterministically merged.
- [ ] `public_award_count` means deduplicated observed award evidence, not
  total public-sector contracts and not total framework call-offs.
- [ ] Materialize one row per `(country_code, company_id)` into
  `company_public_procurement_summary`.
- [ ] Materialize Sweden’s `public_award` coverage row:

  ```text
  status: partial
  sources: sweden_uhm_procurement, ted_procurement
  coverage: UHM 2021–latest release; TED eForms 2024–current
  caveat: excludes direct/non-advertised procurement and missing after-notices
  ```

- [ ] Add Dagster dependencies on both ClickHouse source assets.
- [ ] Refuse to replace the summary when both source inputs are unexpectedly
  empty. One source may be empty only when explicitly allowed and explained in
  materialization metadata.

**Exit criterion:** every summary row points to a real Sweden company, counts
are deterministic, and coverage/freshness is queryable independently of
millions of company rows.

---

## Task 6 — Add the daily GLEIF ISIN-to-LEI bridge

**Files:**

- Extend the existing:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/` module.
- Create a migration pair for `corpscout.gleif_isin_lei`.
- Add a bounded real-file fixture and extend:
  - `tests/test_gleif_source.py`
  - `tests/test_gleif_tables.py`
  - `tests/test_gleif_assets.py`

### Asset graph

```text
gleif_isin_lei_raw_file
  -> gleif_isin_lei_duckdb
  -> gleif_isin_lei_clickhouse
```

The bridge is independent of GLEIF Golden Copy LEI records, but the listing
mapping depends on both outputs.

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

- [ ] Resolve the current file URL from the GLEIF download page; do not
  hardcode a date-bearing ZIP URL.
- [ ] Download once daily, preserve the ZIP plus manifest, and load with a
  set-based DuckDB reader.
- [ ] Uppercase and validate ISIN/LEI syntax without silently correcting
  malformed source values.
- [ ] Preserve multiple mappings if the source publishes them. Ambiguity is
  resolved downstream, not discarded during ingestion.
- [ ] Add row-count, distinct-ISIN, distinct-LEI, malformed, and duplicate
  metadata.
- [ ] Extend the existing daily GLEIF job/schedule or add a separately
  staggered daily job. Use the existing GLEIF pool if the same DuckDB file is
  opened; otherwise use a distinct file and pool.

**Exit criterion:** an ISIN can be deterministically looked up to zero, one,
or multiple LEIs, with file date and provenance.

---

## Task 7 — Reconcile FIRDS and EODHD instruments to Sweden companies

**Files:**

- Create module:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/`.
- Create migration tables:
  - `corpscout.eodhd_company_listings`
  - `corpscout.company_listings`
- Add:
  `corpscout/services/dagster_v3/tests/test_company_listings.py`.

### Source-specific EODHD resolution

Create `eodhd_company_listings_clickhouse` with dependencies on:

```text
eodhd_symbols
eodhd_symbol_mics
esma_firds_clickhouse
gleif_isin_lei_clickhouse
gleif_reference_clickhouse
sweden_company_companies_clickhouse
```

Mapping:

```text
eodhd_symbols.isin
  -> current FIRDS (ISIN, MIC).issuer_lei
  -> gleif_lei_records.registered_as
  -> digits-only Swedish organisation number
  -> se_companies.company_id

fallback only when FIRDS has no usable issuer LEI:
eodhd_symbols.isin
  -> gleif_isin_lei.lei
  -> gleif_lei_records.registered_as
  -> digits-only Swedish organisation number
  -> se_companies.company_id
```

- [ ] Filter candidate instruments to EODHD equity types agreed in the
  analysis (`Common Stock`, `Preferred Stock`, `Stock`) while preserving
  rejected-type counts in QA metadata.
- [ ] Do not discard delisted rows; store them with `is_delisted=1`.
- [ ] Create one resolution row for every relevant EODHD symbol, including
  unresolved rows:

  ```text
  eodhd_symbol_key
  company_id                  empty when unresolved
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
  lei_not_sweden
  invalid_registered_as
  company_not_found
  ambiguous_company
  ```

- [ ] Never fall back to symbol/name matching.
- [ ] Preserve EODHD-only and delisted observations for QA and detail
  provenance, but do not let an EODHD-only active row create the EEA
  current-listing green signal.
- [ ] Prefer an issued/active GLEIF record when duplicate LEI records exist,
  but do not hide a genuinely ambiguous ISIN→LEI relationship.

### Canonical company listings

Create `company_listings_clickhouse` by starting from exact FIRDS
`(ISIN, MIC)` current/history rows, resolving issuer LEIs to the Sweden
registry, and left-enriching matching rows with EODHD ticker/name/price keys.
The table grain is one canonical instrument-and-venue relationship:

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

For the first Sweden listing release:

```text
is_current =
  exact (ISIN, MIC) is current in FIRDS
  AND FIRDS CFI is an allowed equity class
  AND MIC is in the configured Sweden scope
  AND company match is high-confidence
  AND FIRDS weekly full snapshot is <= 9 days old
  AND the required FIRDS delta/cancellation sequence is <= 3 days old
```

The EODHD `ST -> XSTO` mapping may be stored as vendor venue evidence, but it
must not be described as a verified First North/NGM/Spotlight segment.

- [ ] Apply source precedence explicitly:

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

- [ ] When FIRDS terminates/cancels an exact `(ISIN, MIC)` and EODHD still
  reports active, retain both source observations and set `status_conflict=1`.
  The canonical in-scope venue status follows FIRDS.
- [ ] Build `company_listing_summary` only from `is_current=1`.
- [ ] Count distinct MICs for `listing_venue_count`; use a stable sorted array
  for `listing_markets`.
- [ ] Add Sweden’s `public_listing` coverage row with the exact configured
  FIRDS MIC/CFI scope plus FIRDS, GLEIF, and EODHD source dates.
- [ ] Schedule reconciliation after successful FIRDS current-state
  publication and the daily GLEIF bridge. EODHD enrichment may refresh
  weekly without changing FIRDS-authoritative lifecycle state.
- [ ] Add historical “was traded on date T” tests based on FIRDS events.
- [ ] Keep the UI two-state. Do not implement red until a separate product
  decision defines a scope-complete negative contract.

### Acceptance checks

- [ ] Every current green listing resolves to a fresh exact FIRDS
  `(ISIN, MIC)` equity record in the configured Sweden scope.
- [ ] All 946 active Stockholm common-stock baseline rows are accounted for
  as matched or with an explicit unresolved reason.
- [ ] The approximately 209 EODHD missing-ISIN rows may be recovered through
  FIRDS issuer/company evidence, but unresolved vendor rows remain explicit.
- [ ] No canonical listing row has an empty company ID, ISIN, or issuer LEI.
- [ ] Multi-class issuers produce multiple detail rows but one company summary.
- [ ] Foreign issuers remain in source resolution QA but do not attach to a
  Sweden company by name.
- [ ] A stale or sequence-incomplete FIRDS state produces zero current green
  listing summaries; stale EODHD removes ticker/price freshness but does not
  reverse a fresh FIRDS lifecycle result.

**Exit criterion:** the current-listing green signal can be built without
EODHD’s paid ID Mapping endpoint, while exact venue/classification/lifecycle
comes from the reusable EU FIRDS foundation and EODHD remains enrichment.

---

## Task 8 — Extend `companies_all`

**Files:**

- Create a forward migration that adds the new signal columns to
  `corpscout.companies_all`.
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
has_government_contract   UInt8
public_award_count        Nullable(UInt32)
public_award_last_date    Nullable(Date)
is_publicly_traded        UInt8
listing_venue_count       Nullable(UInt16)
listing_markets           Array(String)
signals_resolved_at       DateTime64(3, 'UTC')
```

- [ ] Append the columns to `COMPANIES_ALL_COLUMNS` in migration order.
- [ ] Join the two summary tables by `(country_code, company_id)` in every
  country leg.
- [ ] Derive flags from summary-row existence:

  ```text
  has_government_contract = procurement summary row exists
  is_publicly_traded      = listing summary row exists
  ```

- [ ] A missing summary row yields `0`, NULL counts/dates, and an empty market
  array. That `0` is a gray UI state.
- [ ] Add the summary asset keys to `companies_all_clickhouse` dependencies.
  Do not add the raw UHM/FIRDS/EODHD/GLEIF assets as direct dependencies.
- [ ] Keep the exact per-country row-count equality guard. Summary joins must
  never duplicate a company.
- [ ] Extend parity tests:
  - Sweden green procurement rows exist in the procurement summary;
  - Sweden green listing rows exist in the listing summary;
  - all other countries default to gray until their own summaries exist;
  - total and per-country `companies_all` row counts remain unchanged.

**Exit criterion:** the unified table exposes both new signals without
changing the company grain or the meaning/type of `has_financials`.

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
- Extend relevant backoffice unit/live tests.

### Unified row and query

Return:

```text
has_financials
has_government_contract
public_award_count
public_award_last_date
is_publicly_traded
listing_venue_count
listing_markets
signals_resolved_at
```

Do not calculate signals in TypeScript from revenue, company names, Wikidata,
or detail-page evidence.

### Filter contract

Replace the positive-only synthetic financial toggle with three two-option
signal facets:

```text
financial_data:
  available -> has_financials = 1
  unknown   -> has_financials = 0

government_contract:
  observed -> has_government_contract = 1
  unknown  -> has_government_contract = 0

public_listing:
  current -> is_publicly_traded = 1
  unknown -> is_publicly_traded = 0
```

- [ ] Whitelist the values in `parseUnifiedFilters`; never interpolate a
  value or column from the URL into SQL.
- [ ] Support old `f_has_financials=true` links for one release by mapping
  them to `financial_data=available`, but emit only the new key in newly
  generated links.
- [ ] If both values of one two-option facet are selected, omit its SQL
  predicate because it means “all.”
- [ ] Return both facet counts with `countIf(flag = 1)` and
  `countIf(flag = 0)`.
- [ ] When the company list is country-locked, compute signal facet counts
  from `companies_all WHERE country_code = ...`; do not fall back to the
  country registry table, which lacks these columns.

### Main-list presentation

- [ ] Add one compact **Signals** column containing three accessible icons:
  financial data, government contract, and public listing.
- [ ] Green and gray must differ by icon text/accessible label, not color
  alone.
- [ ] Use these labels:

  ```text
  Financial data available
  Government contract observed
  Currently equity-traded
  ```

- [ ] Never render a red/destructive variant for `0`.
- [ ] Green icons link to detail anchors:

  ```text
  #financial-data
  #government-contracts
  #public-listings
  ```

- [ ] Gray icons are not evidence links.

### Coverage metadata

- [ ] Add one server query that fetches `company_signal_coverage` for the
  countries present on the returned page.
- [ ] Pass a country/signal coverage map to the signal icon component rather
  than joining caveat text onto every company row.
- [ ] Tooltip content includes source names, declared scope, latest source
  refresh, and the caveat. Gray copy says “No positive evidence in covered
  sources,” never “No.”

### Sweden detail queries

- [ ] Add a Sweden `publicContractsQuery` that unions:
  - `se_uhm_procurement_awards`;
  - Swedish `ted_notice_winners` joined to `ted_notices`.
- [ ] Keep source labels and source references in every row.
- [ ] Add a listings query against `company_listings` ordered by
  `is_current`, venue, ticker, and ISIN.
- [ ] Render current and historical listing evidence separately. Show source,
  MIC, ticker, ISIN, issuer LEI, admission/termination dates when present, and
  retrieval date.
- [ ] Keep Wikidata listings in the Wikidata enrichment section; do not use
  them to set the main-list flag.

### UI tests

- [ ] Type-level tests cover the expanded `UnifiedRow`.
- [ ] Query tests cover all six filter values and composed country filters.
- [ ] Facet tests assert green + gray counts add to the applicable company
  total.
- [ ] Rendering tests assert `0` has neutral styling and unknown wording.
- [ ] Rendering tests assert accessible labels and evidence links for green.
- [ ] Detail query tests assert Swedish UHM/TED rows and canonical listings
  are returned without cross-country leakage.
- [ ] Run `pnpm typecheck` and the full backoffice test suite.
- [ ] Perform a browser smoke test on global and Sweden-locked company lists,
  filters, tooltips, keyboard navigation, and detail anchors.

**Exit criterion:** users can see and filter all three signals on the main
page without the UI making a negative claim.

---

## Task 10 — Improve financial availability without mixing statement scopes

**Files:**

- Extend Sweden financial QA/tests.
- Extend the existing ESEF and company-financial-summary modules only after
  the projection gap is explained.
- Update financial detail components to show statement scope.

- [ ] Add a reproducible audit for:

  ```text
  se_financial_reports companies
  -> se_financial_metrics companies
  -> se_company_financials_latest companies
  -> companies_all has_financials
  ```

- [ ] Classify each loss as parser/mapping, company-registry join,
  latest-row selection, or expected source-scope loss.
- [ ] Add quality checks for the approximately 1,602 report companies without
  mapped metrics and the larger metrics→`companies_all` gap.
- [ ] Keep Bolagsverket metrics as:

  ```text
  statement_scope = legal_entity_statutory
  ```

- [ ] Expose existing Swedish ESEF metrics separately as:

  ```text
  statement_scope = group_consolidated_ifrs
  ```

- [ ] Build a one-row-per-company Sweden financial-availability projection
  from the union of:
  - `se_company_financials_latest` statutory evidence;
  - `esef_entity_registry_map` joined to usable Swedish
    `esef_financial_metrics`.
- [ ] For Sweden’s main green flag, a usable statement in either scope may
  count as financial data available. Keep the current
  `se_company_financials_latest` join as the source of list revenue and fiscal
  year. An ESEF-only green company may therefore have no list revenue until
  the product explicitly adds a statement-scope selector; do not silently
  substitute consolidated group revenue for statutory legal-entity revenue.
- [ ] Update only the Sweden `companies_all` leg to test the availability
  projection in addition to the statutory row. Other countries keep their
  current financial-summary semantics until equivalent country-specific
  evidence is implemented.
- [ ] Add the `#financial-data` detail anchor and show scope/source/year.
- [ ] Treat scanned-paper ingestion as a decision gate:
  confirm access, price, redistribution rights, and completeness before any
  OCR/PDF implementation task is opened.

**Exit criterion:** the financial icon describes Corpscout data availability,
the projection loss is measured, and statutory versus consolidated figures
cannot be confused.

---

## Task 11 — Operational rollout and acceptance

### Deployment order

1. Apply forward migrations.
2. Materialize the complete FIRDS weekly baseline and verify multi-country,
   MIC, CFI, and issuer-LEI coverage.
3. Apply FIRDS deltas/cancellations through the latest available sequence and
   publish `firds_instruments_current`.
4. Materialize GLEIF ISIN→LEI and refresh GLEIF LEI records.
5. Materialize EODHD resolution and the FIRDS-authoritative Sweden canonical
   listing projection.
6. Materialize UHM and Swedish TED evidence.
7. Materialize procurement/listing summaries and coverage.
8. Rebuild `companies_all`.
9. Run data-quality and parity checks.
10. Deploy the backoffice UI.
11. Enable source schedules/sensors only after successful manual runs.

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

Use the repository migration mechanism and verify every new table with
`system.columns`/`SHOW CREATE TABLE` before materialization.

### Data acceptance

- [ ] `companies_all` total and per-country row counts are unchanged.
- [ ] Every new flag is exactly `0` or `1`.
- [ ] Every green procurement row resolves to at least one UHM/TED evidence
  row.
- [ ] Every green listing row resolves to at least one fresh current canonical
  listing row.
- [ ] Every listing source candidate is either matched or has an explicit
  unresolved reason.
- [ ] Summary joins produce no duplicate companies.
- [ ] Facet green + gray counts equal the applicable company count.
- [ ] Coverage rows exist for all three Sweden signals.
- [ ] No unmatched supplier identifier is returned by a backoffice query.
- [ ] UI and accessibility tests confirm no red/negative presentation exists.

### Observability

Every source/summary materialization should report:

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
- listing source beyond freshness SLA;
- all Sweden procurement/listing matches dropping to zero;
- summary/company join duplication;
- migration/export column drift;
- schedule/sensor failures or leaked Dagster pool slots.

## Suggested pull-request sequence

1. `feat(data): company signal and coverage schemas`
2. `feat(data): build reusable EU ESMA FIRDS foundation`
3. `feat(data): ingest GLEIF ISIN to LEI mapping`
4. `feat(data): reconcile FIRDS and EODHD listings to Sweden companies`
5. `feat(data): ingest Sweden UHM procurement awards`
6. `feat(data): enable Sweden TED awards and publish automation`
7. `feat(data): build Sweden public-award summary`
8. `feat(data): project company signals into companies_all`
9. `feat(backoffice): company signal icons filters and evidence`
10. `fix(data): Sweden financial availability projection and ESEF scope`

Do not combine source ingestion, the `companies_all` migration, and the entire
UI into one pull request. The evidence tables and quality checks must be
reviewable before their booleans become user-visible.

---

## Follow-on — Active procurement opportunities

This is a separate product from the historical **Government contract** signal.
An award row proves an observed past win; it does not prove that a supplier can
submit an offer now.

- Add TED Competition notices (`cn-standard`, `cn-social`,
  `pin-cfc-standard`, and `pin-cfc-social`) alongside the current Result
  notices.
- Store procedure ID, notice/version chain, current status, publication date,
  submission deadline, buyer, CPV, place of performance, estimated value,
  procedure type, submission/document URLs, and source update time.
- Resolve change/cancellation notices before setting `is_open = 1`; a deadline
  alone is not sufficient.
- Keep the full normalized opportunity corpus. Use company NACE↔CPV matching
  only as a recommendation score, never as a claim that the company is
  legally eligible.
- TED covers EU-threshold opportunities. Sweden has no single national live
  announcement database; below-threshold coverage requires licensed/API
  integrations with the registered private announcement databases. Do not
  label TED-only results as complete Sweden coverage.
- The UHM statistical collection is useful for market history, but its
  reported/annual open datasets are not a real-time tender-submission feed.
- Deep-link to the original announcement database where the supplier
  registers, retrieves documents, asks questions, and submits the offer.
  Corpscout must not imply that an offer can be submitted directly from an
  analytical snapshot.
