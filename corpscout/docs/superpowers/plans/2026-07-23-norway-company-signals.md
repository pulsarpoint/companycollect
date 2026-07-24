# Norway Company Signals Implementation Plan

> Source analysis:
> [`companies/analysis/norway/README.md`](../../../../companies/analysis/norway/README.md),
> [`investigation.md`](../../../../companies/analysis/norway/investigation.md),
> [`license_notes.md`](../../../../companies/analysis/norway/license_notes.md),
> and [`source_inventory.md`](../../../../companies/analysis/norway/source_inventory.md)

**Goal:** Extend the existing Norway Brønnøysund registry and financial
pipelines with evidence-backed public-award and current public-listing
signals, while making financial availability scope explicit. Reuse the shared
EU FIRDS, GLEIF, TED, and signal-summary foundations rather than creating
Norway-only copies.

**Product contract:** v1 is strictly two-state:

```text
green = confirmed positive evidence
gray  = no positive evidence, unsupported, unresolved, incomplete, or stale
```

There is no red state in v1. A stored `0` means “do not render green”; it does
not mean the company has no financial statements, public contracts, or
publicly traded instruments.

## Architecture

```text
ESMA FIRDS full + delta + cancellations
  -> firds_instrument_events
  -> firds_instruments_current (all EU/EEA MICs and instrument types)
                                      │
EODHD symbols/prices enrichment ──────┼─> company_listings
GLEIF LEI registry identity ──────────┘          │
GLEIF ISIN→LEI fallback ─────────────────────────┘
                                                 │
Brreg key figures ────────────────┐               │
ESEF consolidated statements ────┼─> financial availability
Brreg report-copy OCR (gated) ────┘               │
                                                 │
Doffin eForms ─> no_doffin_notice_winners ─┐     │
TED eForms ───> ted_notice_winners ────────┴─────┤
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

The source tables remain source-specific. Cross-source tables contain
normalized evidence and summaries; they do not erase provenance or replace
the source tables. Shared EU ingestion remains country-neutral. Norway is a
downstream projection identified by exact organization number, LEI, ISIN,
MIC, and publication identifiers.

## Decisions fixed by the analysis

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

4. Initial Norway public-listing scope is equity instruments on:

   ```text
   XOSL  Oslo Børs
   XOAS  Euronext Expand Oslo
   MERK  Euronext Growth Oslo
   ```

   The scope follows active ISO 10383 MIC records and exact MIC values
   observed in FIRDS. Exclude Oslo bond, derivatives, auction, off-book, and
   dark-pool segments. `XOAX` observed in some Euronext material is not a
   substitute for authoritative `XOAS` without explicit source reconciliation.
5. FIRDS is the official foundation for instrument identity, venue,
   classification, and lifecycle state. Ingest all EEA records before any
   Norway projection; do not build a Norway-only FIRDS pipeline.
6. Resolve a Norwegian issuer LEI only through GLEIF registration authorities
   `RA000472` (Foretaksregisteret) or `RA000473` (Enhetsregisteret), then
   validate the exact nine-digit `registered_as` value against
   `no_companies.org_number`.
7. EODHD supplies operational symbols and prices only. It does not override
   fresh FIRDS lifecycle or venue state, and the subscription-gated EODHD ID
   Mapping endpoint is not a dependency.
8. Procurement evidence requires an exact, checksum-valid Norwegian
   organization number. Never attach Doffin or TED evidence by supplier name.
9. Doffin is the primary Norway-specific award source and TED supplements
   EEA publication evidence. Deduplicate the same publication only through an
   exact Doffin `tedId`/TED publication identifier, never fuzzy title, buyer,
   supplier, or amount matching.
10. The current Doffin web-client search and notice-detail APIs expose
    structured eForms data, including organization numbers, winner roles, and
    lot links. They are an observed implementation surface, not yet an
    approved durable ingestion contract. Production ingestion is blocked
    until access, reuse, storage, pagination, rate-limit, and compatibility
    terms are recorded.
11. Brreg’s open key-figure API proves only the latest approved ordinary
    accounts available through that endpoint. It does not provide a complete
    historical feed and excludes important layouts such as banks, insurers,
    and some group accounts.
12. ESEF evidence is a separate
    `group_consolidated_ifrs` scope. It may make the availability signal green
    but must not silently replace legal-entity revenue in the company list.
13. The existing annual-report PDF/OCR pipeline stays out of the production
    green flag until document retention/reuse rights and extraction-quality
    gates are approved. The paid XML feed remains a separate commercial
    decision.

## Global implementation constraints

- Follow `corpscout/services/dagster_v3/AGENTS.md`,
  `corpscout/services/dagster_v3/CLAUDE.md`, and
  `corpscout/services/dagster_v3/docs/data-source-guidelines.md`.
- Prefer direct source-specific APIs and asset wiring. Do not add a registry,
  facade, repository interface, or service layer merely to make tests
  mockable.
- Every new bulk source uses:

  ```text
  immutable raw object
    -> per-source DuckDB
    -> set-based SQL normalization
    -> migration-owned ClickHouse table
  ```

- Put one shared Dagster pool on every asset that opens the same DuckDB file.
- Use `dlt` retry/session helpers for HTTP sources and preserve response
  metadata needed to prove completeness.
- Refuse empty, incomplete, unexpectedly short, or silently truncated
  replacements.
- ClickHouse schema comes only from forward migrations. Register every new
  migration in `tests/test_clickhouse_migrations.py`.
- Allocate migration numbers when implementation starts; do not reuse numbers
  already reserved by the in-flight FIRDS work.
- Publish with stage tables plus `EXCHANGE TABLES`.
- Keep raw payloads and payload hashes outside analytical ClickHouse tables
  unless they are operationally required.
- Preserve `source_slug`, `source_run_id`, `source_record_id`, retrieval time,
  and source publication/version identifiers on evidence rows.
- A listing may be called “current” only while its status source is inside its
  freshness SLA.
- Historical statement and award evidence does not disappear when a source
  becomes stale. Coverage metadata shows the stale refresh date.
- Do not expose unmatched supplier identifiers or source payloads in the UI.

## Release boundaries

| Release | Outcome | Shared foundation required? |
|---|---|---:|
| Foundation F | Reusable EU FIRDS full/delta/cancellation state | FIRDS |
| A | Correct financial label, green/gray renderer, coverage model | Signals schema |
| B | Doffin + Norwegian TED public-award signal and evidence | TED country safety |
| C | FIRDS + GLEIF + EODHD reconciled Norway listing signal | FIRDS + GLEIF |
| D | Financial coverage audit and scope-safe Brreg/ESEF availability | GLEIF identity |

Foundation F and the existing Norway Brreg registry pipeline are baselines,
not Norway-specific rebuilds. Releases A and B can be developed independently,
but Release C cannot publish green listing state until FIRDS is fresh and
reconciled. Doffin ingestion cannot enter production until its Task 0 gate is
cleared.

---

## Task 0 — Freeze contracts and resolve launch gates

**Files:**

- Create:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/docs/source_design.md`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/docs/norway.md`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/docs/norway.md`
- Update:
  - `companies/analysis/norway/license_notes.md`
  - shared GLEIF, TED, and FIRDS source-design documents when a cross-country
    contract changes.

- [ ] Confirm the Norway v1 listing scope is exactly `XOSL`, `XOAS`, and
  `MERK`, with CFI category `E`. Store this once in the shared country listing
  scope contract.
- [ ] Confirm FIRDS remains all-EEA and country-neutral. Norway filtering
  belongs only in downstream listing resolution.
- [ ] Confirm EODHD production rights for storing and showing symbol, ISIN,
  active/delisted metadata, and price enrichment. Explicitly exclude the paid
  ID Mapping endpoint.
- [ ] Confirm Doffin’s supported machine-readable access path. The observed
  web-client endpoints are:

  ```text
  POST https://api.doffin.no/webclient/api/v2/search-api/search
  GET  https://api.doffin.no/webclient/api/v2/notices-api/notices/{id}
  ```

  Prefer a documented raw eForms/export endpoint if the source owner provides
  one. Do not bypass authentication, access controls, or anti-automation
  measures.
- [ ] Record Doffin terms for access, raw-response retention, derived-table
  storage, redistribution, attribution, commercial reuse, pagination, rate
  limits, and backward compatibility. The historical data.norge CSV’s
  CC BY 4.0 metadata is evidence for that dataset only; do not automatically
  apply it to the current web-client API.
- [ ] Confirm Doffin’s authoritative result-notice type codes and the earliest
  complete eForms date. The proposed v1 lower bound is `2024-01-01`.
- [ ] Approve storage of supplier organization numbers as company identifiers.
  Unmatched IDs stay in restricted source evidence and are not returned by
  public/backoffice company queries.
- [ ] Record that Doffin/TED result coverage is incomplete by design:
  unadvertised and below-threshold purchases are excluded, and published
  competitions do not always receive a result notice.
- [ ] Resolve Brreg report-copy terms before using retained PDFs, OCR output,
  or derived historical metrics in the product flag. Record retention,
  processing, display, and redistribution separately.
- [ ] Keep the paid Brreg XML/SFTP accounts feed out of v1 until price,
  historical completeness, update semantics, and redistribution rights are
  approved.
- [ ] Record freshness SLAs:

  | Source | Expected cadence | Proposed stale threshold |
  |---|---|---:|
  | Brreg entity registry updates | continuous/daily | 3 days |
  | Brreg latest key figures | on-demand/current | informational for existing evidence |
  | Doffin result notices | daily | 3 days |
  | TED eForms | monthly in current pipeline | 45 days |
  | GLEIF Golden Copy | daily | 3 days |
  | GLEIF ISIN→LEI | daily | 3 days |
  | EODHD reference symbols | weekly | 9 days |
  | FIRDS full/cancellation | weekly | 9 days |
  | FIRDS delta | daily | 3 days |

**Exit criterion:** no implementation task needs to infer Doffin access
rights, identifier policy, financial-document rights, listing scope, or
freshness semantics.

---

## Task 1 — Add shared signal evidence and coverage schemas

**Files:**

- Create the next available migration pair under:
  `corpscout/clickhouse/migrations/`
- Modify:
  `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`
- Create or extend:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/`
- Add:
  `corpscout/services/dagster_v3/tests/test_company_signals_tables.py`

### Tables

Create source-neutral summary and coverage tables:

```text
company_procurement_summary
  country_code LowCardinality(String)
  company_id String
  award_count UInt32
  first_award_date Nullable(Date)
  last_award_date Nullable(Date)
  source_slugs Array(String)
  source_updated_at DateTime64(3, 'UTC')
  resolved_at DateTime64(3, 'UTC')
  ORDER BY (country_code, company_id)

company_signal_coverage
  country_code LowCardinality(String)
  signal LowCardinality(String)
  status Enum8('unsupported' = 0, 'partial' = 1, 'supported' = 2)
  source_slugs Array(String)
  coverage_start Nullable(Date)
  coverage_end Nullable(Date)
  latest_source_refresh Nullable(DateTime64(3, 'UTC'))
  stale_after Nullable(DateTime64(3, 'UTC'))
  caveat String
  resolved_at DateTime64(3, 'UTC')
  ORDER BY (country_code, signal)
```

`company_listings` and `company_listing_summary` are created in Task 7 because
their row contract depends on the shared FIRDS/GLEIF reconciliation.

- [ ] Store `country_code` in lowercase registry form (`no`, `se`, `fi`).
  Keep source-native `NO` and TED `NOR` only at source boundaries.
- [ ] Make all company joins `(country_code, company_id)` safe.
- [ ] Use summary-row existence as positive evidence; never manufacture a
  negative evidence row.
- [ ] Constrain `signal` to:

  ```text
  financial_data
  public_award
  public_listing
  ```

- [ ] Seed coverage only after a source projection publishes successfully.
- [ ] Document that `partial` means positive observations are usable but
  absence is inconclusive.
- [ ] Add migration apply/down and exact-column-order tests.

**Exit criterion:** source-specific pipelines have a stable, country-safe
target for positive-evidence summaries and coverage caveats.

---

## Task 2 — Reuse and validate the shared FIRDS foundation

**Files:**

- Reuse:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/esma_firds/`
- Extend only where Norway reveals a shared defect:
  - FIRDS source, parser, state, and listing-scope tests;
  - FIRDS docs and freshness checks.

The current workspace already contains the shared FIRDS full/delta/
cancellation implementation, assets, jobs, schedules, fixtures, and
migration work. Norway must consume that foundation, not duplicate it.

- [x] Keep raw FIRDS ingestion country-neutral and all-EEA.
- [x] Preserve instrument events separately from current state.
- [x] Reject empty or incomplete upstream register/file replacements.
- [x] Provide weekly full/cancellation and daily delta automation.
- [ ] Bootstrap the full register in the deployment environment and prove
  successful publication of current state.
- [ ] Verify at least one current Norwegian equity example for each in-scope
  MIC present in live FIRDS, or record the venue as temporarily empty.
- [ ] Verify cancellations and terminated records cannot survive as current.
- [ ] Verify current-state freshness checks are usable by the listing
  projection.
- [ ] Add Norway-specific scope fixtures without filtering the shared source
  tables.

**Exit criterion:** the deployed FIRDS foundation contains fresh,
country-neutral current state and can support an exact Norway equity
projection.

---

## Task 3 — Ingest Norway Doffin result and award evidence

**Files:**

- Create:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/__init__.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/assets.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/config.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/normalize.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/source.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/tables.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/jobs.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_doffin_procurement/schedules.py`
- Create the next available ClickHouse migration pair.
- Add fixtures and tests:
  - `corpscout/services/dagster_v3/tests/fixtures/norway_doffin_procurement/`
  - `corpscout/services/dagster_v3/tests/test_norway_doffin_source.py`
  - `corpscout/services/dagster_v3/tests/test_norway_doffin_parser.py`
  - `corpscout/services/dagster_v3/tests/test_norway_doffin_assets.py`

### Partition and asset graph

Use monthly partitions from the approved eForms lower bound:

```text
MonthlyPartitionsDefinition(start_date="2024-01-01", end_offset=1)
```

Use one partition per run:

```text
BackfillPolicy.multi_run(max_partitions_per_run=1)
```

Build:

```text
norway_doffin_monthly_snapshot_s3
  -> norway_doffin_monthly_duckdb
  -> norway_doffin_clickhouse
```

- [ ] Block production implementation until the Task 0 Doffin contract is
  approved. Fixtures may be developed from already observed public examples,
  but production automation must remain stopped.
- [ ] Query only result/award notice types in the approved contract. Preserve
  the exact request, result pages, notice details, retrieval time, response
  metadata, and a manifest of notice IDs and versions in immutable storage.
- [ ] For each month, assert:

  ```text
  fetched result IDs == declared accessible result IDs
  every result ID has a successful detail response or an explicit retry/error
  no page was truncated
  no notice silently crossed the requested date boundary
  ```

- [ ] Treat `numHitsTotal > numHitsAccessible` as incomplete. Split the date
  window if supported; otherwise fail the partition and record the source
  limitation.
- [ ] Use deterministic pagination and an explicit stable sort. Do not depend
  on UI defaults.
- [ ] Deduplicate notice versions by Doffin notice ID, eForm ID, and explicit
  version/publication fields. Retain enough provenance to reconstruct which
  source version produced a normalized row.
- [ ] Parse structured eForms block IDs, not localized visible labels. The
  currently observed shape places organizations below `block06`, organization
  number at `block060104`, and winner-lot roles below the organization block;
  pin these IDs in fixtures and fail loudly on incompatible structural drift.
- [ ] Extract:
  - Doffin notice ID and eForm ID;
  - exact TED publication identifier when present;
  - notice/result type and publication date;
  - procedure, lot, tender, and contract identifiers;
  - buyer identity;
  - winner name and organization number;
  - winner-to-lot/contract role;
  - award/contract date;
  - amount and currency when explicitly present;
  - CPV and title fields useful for detail evidence.
- [ ] Normalize Norwegian organization IDs only from explicitly identified
  organization-number fields:

  ```text
  923609016           -> 923609016
  923 609 016         -> 923609016
  NO923609016         -> 923609016, only if documented in source fixtures
  NO 923 609 016 MVA  -> 923609016, only if documented in source fixtures
  ```

- [ ] Validate exactly nine digits with the Norwegian modulo-11 organization
  number checksum. Reject personal identifiers, foreign IDs, arbitrary digit
  extraction, and invalid check digits.
- [ ] Never fall back to winner name, address, email domain, or buyer name.

### ClickHouse tables

Create:

```text
no_doffin_notices
  source_notice_id String
  eform_id String
  ted_publication_number Nullable(String)
  publication_date Date
  notice_type String
  procedure_id Nullable(String)
  buyer_name Nullable(String)
  title Nullable(String)
  source_run_id String
  retrieved_at DateTime64(3, 'UTC')
  ORDER BY (publication_date, source_notice_id)

no_doffin_notice_winners
  source_notice_id String
  eform_id String
  ted_publication_number Nullable(String)
  lot_id String
  tender_id String
  contract_id Nullable(String)
  winner_ordinal UInt16
  winner_name String
  winner_org_number Nullable(String)
  award_date Nullable(Date)
  contract_date Nullable(Date)
  amount Nullable(Decimal(20, 2))
  currency Nullable(FixedString(3))
  source_record_id String
  source_run_id String
  retrieved_at DateTime64(3, 'UTC')
  ORDER BY
    (winner_org_number, publication_date, source_notice_id, lot_id,
     tender_id, winner_ordinal)
```

Add any fields required for deterministic versioning and provenance, but do
not store unrestricted raw JSON in the analytical tables.

- [ ] Publish both tables atomically through stage tables and
  `EXCHANGE TABLES`.
- [ ] Assert winner rows reference an included notice version.
- [ ] Assert the normalized winner key is unique at its declared grain.
- [ ] Assert a Norway company match uses exact
  `winner_org_number = no_companies.org_number`.
- [ ] Emit checks for missing winner IDs, invalid IDs, unresolved company
  IDs, structural drift, duplicate versions, and incomplete partitions.
- [ ] Add a daily incremental job only after a historical backfill succeeds.
  Use run-status sensor automation only if it is needed to launch a separate
  downstream job; prefer direct asset dependencies within one job.

**Exit criterion:** approved Doffin result notices produce reproducible,
versioned winner evidence, and only checksum-valid exact organization numbers
can attach to Norway companies.

---

## Task 4 — Add Norway to the shared TED procurement pipeline

**Files:**

- Modify:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/ted_procurement/tables.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/ted_procurement/normalize.py`
  - shared TED assets/jobs/schedules as required for automatic publication
- Create a country-safe TED migration before loading a second country.
- Extend:
  - `corpscout/services/dagster_v3/tests/test_ted_procurement_parser.py`
  - `corpscout/services/dagster_v3/tests/test_ted_procurement_assets.py`
  - TED fixtures with Norwegian eForms examples.

### Country-safe table order

Before the first Norway load, rebuild shared TED tables so country is part of
the sorting and uniqueness contract:

```text
ted_notices
  ORDER BY (country_iso2, publication_number)

ted_notice_winners
  ORDER BY
    (country_iso2, winner_national_id, publication_number, lot_id,
     tender_id, winner_ordinal)
```

- [ ] Register Norway:

  ```python
  TedCountry(place_code="NOR", country_iso2="NO")
  ```

- [ ] Ensure snapshots, DuckDB paths, manifests, checks, and replacement SQL
  are country-partitioned. A Norway run must not replace Finland rows.
- [ ] Parse winner `CompanyID` only from the structured eForms organization
  field.
- [ ] Normalize and checksum-validate the same approved nine-digit Norwegian
  ID forms as Task 3.
- [ ] Reject missing, foreign, invalid, or ambiguous identifiers without name
  matching.
- [ ] Preserve publication number, notice/version ID, lot, tender, winner
  ordinal, publication date, award/contract dates, amount, currency, and
  retrieval provenance.
- [ ] Add a fixture from a Norwegian TED result notice whose winner
  `CompanyID` is a plain nine-digit organization number.
- [ ] Add a cross-country fixture where equal-looking source IDs cannot
  collide or replace each other.
- [ ] Backfill `NOR` from `2024-01` through the latest complete partition.
- [ ] Make current-month publication automatic. Start the publisher
  schedule/sensor in `STOPPED` state, validate one manual production run, then
  enable it deliberately.
- [ ] Add checks for fresh partition coverage, exact company-match rate,
  missing winner IDs, and unexpected ID forms.

**Exit criterion:** Norwegian TED winner evidence is loaded automatically
without cross-country overwrite risk and matches companies only by an exact
validated organization number.

---

## Task 5 — Build the Norway procurement summary and coverage contract

**Files:**

- Create or extend:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/procurement.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_signals/coverage.py`
- Add:
  - `corpscout/services/dagster_v3/tests/test_norway_procurement_summary.py`
  - `corpscout/services/dagster_v3/tests/test_norway_signal_coverage.py`

### Evidence union

Build one canonical Norway award-evidence query from:

```text
no_doffin_notice_winners
  UNION DISTINCT-BY-EXACT-SOURCE-IDENTITY
ted_notice_winners WHERE country_iso2 = 'NO'
```

- [ ] Attach Doffin rows through exact
  `winner_org_number = no_companies.org_number`.
- [ ] Attach TED rows through exact
  `winner_national_id = no_companies.org_number`.
- [ ] Deduplicate a Doffin/TED duplicate only when Doffin carries the exact
  normalized TED publication number and the winner/lot identity agrees.
- [ ] If exact cross-source identifiers disagree, preserve both source rows
  for QA and do not invent a fuzzy merge.
- [ ] Count canonical awarded notice/lot/tender evidence, not raw parser rows.
  Document the exact summary grain.
- [ ] Publish one `company_procurement_summary` row per matched Norway company
  with:

  ```text
  country_code = 'no'
  company_id = org_number
  award_count
  first_award_date
  last_award_date
  source_slugs
  source_updated_at
  resolved_at
  ```

- [ ] Emit one `company_signal_coverage` row:

  ```text
  country_code = no
  signal = public_award
  status = partial
  sources = [norway_doffin, ted_procurement]
  coverage = approved Doffin/TED eForms interval
  caveat = excludes unadvertised/below-threshold procurement,
           unpublished result notices, ID-less winners, and source gaps
  ```

- [ ] Do not publish a green procurement summary from Doffin until its launch
  gate is approved. TED-only evidence may publish with a TED-only partial
  coverage row if product explicitly accepts that intermediate release.
- [ ] Add parity checks from source rows to valid IDs, registry matches,
  canonical evidence, and summary rows, with classified loss reasons.

**Exit criterion:** a green public-award signal is traceable to exact Doffin
or TED evidence, while gray remains an inconclusive state.

---

## Task 6 — Build the shared GLEIF ISIN-to-LEI identity bridge

**Files:**

- Reuse the existing GLEIF Golden Copy definitions.
- Create or extend:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/isin_lei.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/jobs.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/gleif/schedules.py`
- Create the next available migration pair for the shared bridge if it does
  not already exist.
- Add:
  - `corpscout/services/dagster_v3/tests/test_gleif_isin_lei.py`
  - `corpscout/services/dagster_v3/tests/test_gleif_norway_identity.py`

- [x] Reuse GLEIF Golden Copy LEI and registration-authority data.
- [ ] Ingest the official GLEIF ISIN-to-LEI relationship file as a shared,
  country-neutral source.
- [ ] Preserve all ISIN-to-LEI candidates and relationship provenance.
- [ ] Use the bridge only when FIRDS does not provide an issuer LEI. An
  ambiguous ISIN-to-LEI relationship does not resolve a listing.
- [ ] Normalize Norway GLEIF registry identity:

  ```text
  registration_authority_id IN ('RA000472', 'RA000473')
  registered_as -> remove permitted presentation spacing
  registered_as -> exactly 9 digits
  registered_as -> Norwegian modulo-11 checksum valid
  registered_as = no_companies.org_number
  ```

- [ ] Reject an LEI with a foreign registration authority, invalid
  `registered_as`, multiple conflicting Norwegian IDs, or no exact company
  row.
- [ ] Never use legal name, trading name, ticker, address, or domain as a
  fallback.
- [ ] Add fixtures for both Norwegian registration authorities, foreign
  issuers listed in Oslo, missing `registered_as`, invalid checksums, and
  conflicting ISIN relationships.
- [ ] Run daily after GLEIF Golden Copy and expose freshness metadata.

**Exit criterion:** an issuer LEI or ISIN resolves to a Norway organization
number only through an exact, auditable GLEIF authority mapping.

---

## Task 7 — Reconcile current Norway equity listings

**Files:**

- Create or extend:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/__init__.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/assets.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/listing_scopes.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/reconcile.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/tables.py`
- Create the next available migration pair.
- Add:
  - `corpscout/services/dagster_v3/tests/test_norway_listing_scope.py`
  - `corpscout/services/dagster_v3/tests/test_norway_company_listings.py`

### Scope

Add:

```python
"NO": CountryListingScope(
    country_code="NO",
    mic_codes=frozenset({"XOSL", "XOAS", "MERK"}),
    cfi_categories=frozenset({"E"}),
)
```

Do not add bond, derivatives, auction, off-book, or dark-pool MICs.

### Canonical tables

Use the shared tables:

```text
company_listings
  country_code String
  company_id String
  isin String
  mic String
  ticker Nullable(String)
  issuer_lei String
  instrument_name Nullable(String)
  cfi_code Nullable(String)
  admission_date Nullable(Date)
  termination_date Nullable(Date)
  is_current UInt8
  resolution_method LowCardinality(String)
  lifecycle_source LowCardinality(String)
  source_slugs Array(String)
  source_updated_at DateTime64(3, 'UTC')
  resolved_at DateTime64(3, 'UTC')
  ORDER BY (country_code, company_id, mic, isin)

company_listing_summary
  country_code String
  company_id String
  current_instrument_count UInt32
  current_venue_count UInt16
  markets Array(String)
  source_updated_at DateTime64(3, 'UTC')
  resolved_at DateTime64(3, 'UTC')
  ORDER BY (country_code, company_id)
```

### Reconciliation

- [ ] Start from fresh `firds_instruments_current` rows where:

  ```text
  mic IN ('XOSL', 'XOAS', 'MERK')
  AND cfi_category = 'E'
  AND lifecycle state is current
  ```

- [ ] Resolve issuer LEI in this order:
  1. issuer LEI present in FIRDS;
  2. unique GLEIF ISIN-to-LEI fallback if FIRDS has no issuer LEI.
- [ ] Resolve company through GLEIF authority `RA000472` or `RA000473` and
  exact checksum-valid organization number.
- [ ] Preserve foreign Oslo-listed issuers as unresolved/foreign QA evidence;
  do not attach them to a Norwegian company by name.
- [ ] Use EODHD only for symbol, exchange-code, price, and provider-status
  enrichment after official identity resolution. A provider exchange code is
  not exact MIC evidence.
- [ ] Set `is_current = 1` only when:
  - FIRDS current state is inside its SLA;
  - exact MIC is in Norway scope;
  - the instrument is equity-classified;
  - the issuer resolves exactly to one Norway company;
  - no cancellation/termination evidence makes the instrument non-current.
- [ ] If FIRDS is stale, do not publish a replacement that falsely preserves
  current green state. Fail the run or publish an explicitly stale coverage
  row according to the shared rollout contract.
- [ ] Record unresolved reasons:

  ```text
  missing_isin
  isin_not_in_firds
  firds_issuer_lei_missing
  ambiguous_firds_venue
  isin_not_in_gleif
  ambiguous_isin_lei
  lei_not_norway
  unsupported_registration_authority
  invalid_registered_as
  invalid_org_number
  company_not_found
  ambiguous_company
  ```

- [ ] Publish `company_listing_summary` only from `is_current = 1`.
- [ ] Publish Norway listing coverage with exact MIC scope, source refresh,
  and the caveat that foreign issuers, non-equity instruments, unresolved
  identity, and out-of-scope venues do not produce a green company signal.
- [ ] Use Euronext and Finanstilsynet venue pages for validation only unless
  separate production reuse rights are approved.

**Exit criterion:** every green Norway listing signal has a fresh FIRDS equity
instrument, exact in-scope MIC, issuer LEI, and exact GLEIF-to-Brreg company
identity chain.

---

## Task 8 — Extend `companies_all` without changing company grain

**Files:**

- Create the next available migration pair under:
  `corpscout/clickhouse/migrations/`
- Modify:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/sql.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/assets.py`
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/tables.py`
- Extend:
  - `corpscout/services/dagster_v3/tests/test_companies_all.py`
  - `corpscout/services/dagster_v3/tests/test_companies_all_schema.py`

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
- [ ] Join procurement and listing summaries by
  `(country_code, company_id)` in every country leg.
- [ ] Derive flags only from summary-row existence:

  ```text
  has_public_award   = procurement summary row exists
  is_publicly_traded = listing summary row exists
  ```

- [ ] A missing summary row yields `0`, NULL counts/dates, and an empty market
  array. `0` is a gray UI state.
- [ ] Add summary asset keys to `companies_all_clickhouse` dependencies. Do
  not add Doffin, TED, FIRDS, EODHD, or GLEIF raw assets directly.
- [ ] Keep exact per-country input/output row-count equality. Summary joins
  must never duplicate a company.
- [ ] Add Norway parity tests:
  - each green award row exists in the procurement summary;
  - each green listing row exists in the listing summary;
  - exact counts/dates/markets agree;
  - unresolved source evidence stays gray;
  - total and per-country company counts remain unchanged.
- [ ] Verify countries without implemented summaries still default to gray.

**Exit criterion:** the unified table exposes both new signals for Norway
without changing the company grain or overloading `has_financials`.

---

## Task 9 — Add list icons, filters, tooltips, and Norway detail evidence

**Files:**

- Modify under `corpscout/services/backoffice/`:
  - `app/lib/filters.ts`
  - `app/lib/unified.server.ts`
  - `app/lib/company-list.server.ts`
  - `app/components/data-table/unified-columns.tsx`
  - `app/components/data-table/filter-sidebar.tsx`
  - `app/components/companies/company-list-page.tsx`
  - `app/routes/facet-options.ts`
  - `app/lib/countries.ts`
  - `app/lib/queries.server.ts`
  - `app/routes/country-company-detail.tsx`
  - `app/components/detail/public-contracts-section.tsx`
- Create if not already added by the shared signals release:
  - `app/components/companies/company-signal-icons.tsx`
  - `app/components/detail/company-listings-section.tsx`
- Extend relevant unit, query, rendering, and browser tests.

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

Use:

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

- [ ] Whitelist all values in `parseUnifiedFilters`; never interpolate a URL
  value or column into SQL.
- [ ] Support old `f_has_financials=true` links for one release by mapping
  them to `financial_data=available`, but emit only the new key.
- [ ] If both values of a facet are selected, omit its predicate.
- [ ] Compute green and gray facet counts from `companies_all`, including on a
  Norway-locked list.
- [ ] Do not derive signals in TypeScript from revenue, names, Wikidata, or
  detail rows.

### Presentation and coverage

- [ ] Add one compact **Signals** column with accessible financial, award, and
  listing icons.
- [ ] Green and gray differ by text/accessible label, not color alone.
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

- [ ] Fetch `company_signal_coverage` once for countries present on the page
  and pass a coverage map to the icons.
- [ ] Tooltip copy includes sources, declared interval/scope, latest refresh,
  stale state, and caveat. Gray says “No positive evidence in covered
  sources,” never “No.”

### Norway detail evidence

- [ ] Add a Norway public-contract query that returns canonical Doffin and TED
  evidence with source label, source notice reference, dates, lot/contract,
  amount/currency, and retrieval date.
- [ ] Do not return unmatched organization identifiers.
- [ ] Add a listings query against `company_listings`, ordered by current
  state, MIC, ticker, and ISIN.
- [ ] Render current and historical listings separately with source, venue,
  ticker, ISIN, issuer LEI, admission/termination dates, and retrieval date.
- [ ] Add `#financial-data` evidence with statement source, scope, period, and
  availability caveat.
- [ ] Keep Wikidata or other enrichment separate and unable to set the main
  signal flags.

### Tests

- [ ] Cover the expanded unified row type and all six filter values.
- [ ] Assert green + gray facet counts equal the applicable company total.
- [ ] Assert neutral styling and unknown wording for `0`.
- [ ] Assert accessible labels and detail links for green icons.
- [ ] Assert Norway detail queries return only the requested company and
  country.
- [ ] Run `pnpm typecheck` and the full backoffice test suite.
- [ ] Browser-smoke-test global and Norway-locked lists, filters, tooltips,
  keyboard navigation, and detail anchors.

**Exit criterion:** users can see, filter, and inspect all three Norway signals
without the UI making unsupported negative claims.

---

## Task 10 — Make Norway financial availability scope-safe

**Files:**

- Create the next available migration pair for:
  `no_company_financial_availability`
- Create or extend:
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/norway_brreg_financial/availability.py`
  - shared ESEF-to-registry mapping assets
  - `corpscout/services/dagster_v3/src/dagster_v3/defs/companies_all/sql.py`
- Add:
  - `corpscout/services/dagster_v3/tests/test_norway_financial_availability.py`
  - Norway financial parity/audit tests.

### Availability projection

Create one row per Norway company with positive evidence:

```text
no_company_financial_availability
  org_number String
  has_brreg_key_figures UInt8
  has_esef_consolidated UInt8
  has_report_copy_metrics UInt8
  latest_key_figures_period Nullable(Date)
  latest_esef_period Nullable(Date)
  latest_report_copy_period Nullable(Date)
  source_slugs Array(String)
  source_updated_at DateTime64(3, 'UTC')
  resolved_at DateTime64(3, 'UTC')
  ORDER BY org_number
```

- [x] Reuse the existing open Brreg key-figure ingestion and
  `no_company_financials_latest` projection.
- [x] Reuse the existing Brreg report-copy/PDF/OCR tables as experimental
  source infrastructure; do not rebuild them.
- [ ] Count a usable `no_company_financials_latest` row backed by a valid
  `no_financial_statements` row as:

  ```text
  statement_scope = legal_entity_key_figures
  has_brreg_key_figures = 1
  ```

- [ ] Reject malformed or quality-failed statement rows from availability.
- [ ] Resolve a Norway ESEF entity only through exact GLEIF LEI identity with
  `RA000472` or `RA000473` and a checksum-valid organization number.
- [ ] Count usable ESEF metrics separately as:

  ```text
  statement_scope = group_consolidated_ifrs
  has_esef_consolidated = 1
  ```

- [ ] Keep `has_report_copy_metrics = 0` in the production projection until:
  - report-copy access and retention/reuse terms are approved;
  - OCR/extraction provenance is complete;
  - production validation thresholds are defined;
  - acceptable rows are distinguishable from partial or failed extraction.
- [ ] If the report-copy gate is later cleared, count only validated
  `no_financial_metrics` rows with a documented quality status and source
  report link.
- [ ] Set `has_financials = 1` for Norway when the availability row has either
  usable Brreg key figures or usable ESEF consolidated evidence. Report-copy
  evidence joins only after its gate clears.
- [ ] Continue sourcing list revenue and fiscal year from
  `no_company_financials_latest`. An ESEF-only green company may have NULL
  list revenue; do not substitute consolidated group revenue for
  legal-entity key figures.
- [ ] Publish Norway financial coverage as `partial`, with caveats covering:
  - latest-only open key figures;
  - ordinary-layout limitations;
  - exclusions such as banks, insurers, and some group accounts;
  - ESEF’s regulated-issuer/consolidated scope;
  - gated historical report-copy extraction.
- [ ] Build a reproducible audit:

  ```text
  Brreg fetch outcomes
    -> no_financial_statements
    -> no_company_financials_latest
    -> no_company_financial_availability
    -> companies_all.has_financials

  ESEF filings
    -> LEI registry identity
    -> usable ESEF metrics
    -> no_company_financial_availability
    -> companies_all.has_financials
  ```

- [ ] Classify losses as not eligible/not found, source-layout exclusion,
  malformed response, quality rejection, stale/latest selection, LEI/registry
  mapping failure, company join failure, or expected scope loss.
- [ ] Keep the paid XML feed as a separate future source-design decision; do
  not make v1 depend on it.

**Exit criterion:** Norway’s financial icon means Corpscout has usable data in
an explicitly displayed statement scope; it never implies complete filing
coverage or mixes consolidated revenue into legal-entity list metrics.

---

## Task 11 — Operational rollout and acceptance

### Deployment order

1. Apply shared signal schema and country-safe TED migrations.
2. Bootstrap/verify deployed FIRDS current state.
3. Load shared GLEIF ISIN-to-LEI and validate Norway authority mappings.
4. Backfill Norwegian TED from the approved lower bound.
5. After the Doffin gate clears, backfill Doffin result notices and validate
   source completeness.
6. Publish procurement summaries and coverage.
7. Reconcile Norway company listings and publish listing summaries/coverage.
8. Publish the Norway financial-availability projection and audit.
9. Rebuild `companies_all`.
10. Deploy the backoffice icons, filters, tooltips, and detail evidence.
11. Enable schedules only after one successful manual production cycle and
    monitoring review.

### Data acceptance checks

- [ ] `no_companies` row count is unchanged by signal work.
- [ ] `companies_all` total and per-country row counts equal their registry
  inputs.
- [ ] Every green award has at least one canonical Doffin/TED evidence row.
- [ ] Every green listing has at least one fresh current FIRDS equity row with
  an in-scope MIC and exact issuer identity.
- [ ] Every green financial signal has at least one usable availability
  source/scope row.
- [ ] No source or summary join duplicates a company.
- [ ] No invalid, personal, foreign, or unmatched supplier ID appears in a
  company response.
- [ ] Doffin/TED exact cross-source duplicates count once.
- [ ] Cross-source disagreements remain visible in QA.
- [ ] Coverage rows exist and show the actual source refresh and scope.
- [ ] Stale FIRDS cannot silently preserve a current listing green.
- [ ] Historical awards and financial evidence remain visible when their
  source refresh is late, with stale coverage copy.

### Operational checks

- [ ] Source jobs have checks for zero rows, unexpected shrinkage, structural
  drift, pagination completeness, duplicate keys, invalid IDs, and freshness.
- [ ] Per-source DuckDB assets share exactly one source-specific Dagster pool.
- [ ] Schedules have explicit owners, cadence, timezone, and alert routing.
- [ ] Backfills are partition-bounded and resumable.
- [ ] Raw object manifests can reproduce normalized rows.
- [ ] Boundary jobs/sensors log one structured error with safe context;
  lower layers wrap and return errors without duplicate logging.
- [ ] No logs contain raw payload bodies, tokens, cookies, or sensitive
  identifiers beyond approved operational keys.
- [ ] Rollback is forward-only: disable schedules, restore the last good
  published tables, and apply a corrective migration.

### Product acceptance

- [ ] Norway list and detail pages use green/gray only.
- [ ] Signal labels describe positive evidence, not legal or factual absence.
- [ ] Filters compose with country, industry, status, and other facets.
- [ ] Tooltips expose source, scope, freshness, and caveats.
- [ ] Detail evidence is source-attributed and linked where permitted.
- [ ] An ESEF-only company can be financially green with NULL legal-entity
  revenue.
- [ ] Foreign issuers listed in Oslo do not attach to Norwegian companies by
  name.
- [ ] Browser smoke tests pass on desktop and narrow layouts.

**Exit criterion:** Norway company signals are reproducible, monitored,
source-attributed, scope-safe, and reversible without making false negative
claims.

---

## Suggested PR sequence

1. **Shared signal schemas and coverage contract**
2. **Shared FIRDS deployment validation and Norway scope fixtures**
3. **Shared GLEIF ISIN-to-LEI and Norway registry identity**
4. **Country-safe TED migration and Norway TED projection**
5. **Norway Doffin source** — only after the access/reuse gate clears
6. **Norway procurement summary and coverage**
7. **Norway listing reconciliation and coverage**
8. **Norway financial-availability projection and audit**
9. **`companies_all` signal columns and joins**
10. **Backoffice icons, filters, tooltips, and detail evidence**
11. **Schedules, monitors, backfill report, and production acceptance**

Each PR must leave migrations, assets, and tests internally consistent. Do
not merge a UI green state before its evidence projection and coverage row can
be produced in the target environment.
