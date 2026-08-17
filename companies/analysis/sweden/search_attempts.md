# Sweden — search attempts log

## Attempt 1

- Date/time: 2026-06-13
- Search engine or source: Web search
- Query: `Bolagsverket Sweden company register open data API bulk download näringslivsregistret`
- Language: English/Swedish
- Why: Identify the official company-register source and whether bulk access exists.
- Top relevant URLs:
  - `https://bolagsverket.se/apierochoppnadata.2531.html`
  - `https://bolagsverket.se/apierochoppnadata/hamtaforetagsinformation/vardefulladatamangder.5294.html`
- Result: Earlier notes focused on the authenticated Värdefulla datamängder API.
- Decision: Keep API as fallback, but verify downloadable-file path separately.

## Attempt 2

- Date/time: 2026-06-13
- Search engine or source: Web search
- Query: `Sweden SCB statistical business register företagsregister open data download companies`
- Language: English/Swedish
- Why: SCB/FDB is the statistical business-register source.
- Top relevant URLs:
  - `https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/`
- Result: SCB business-register data is part of the open/high-value data setup.
- Decision: Use SCB data as company-universe/statistical complement.

## Attempt 3

- Date/time: 2026-06-13
- Search engine or source: Web search
- Query: `Sweden company financial statements annual accounts årsredovisning API Bolagsverket open data`
- Language: English/Swedish
- Why: Need financial statements.
- Top relevant URLs:
  - `https://bolagsverket.se/apierochoppnadata/hamtaforetagsinformation/vardefulladatamangder.5294.html`
  - `https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/`
- Result: Annual reports are available as public downloadable ZIP archives under `arsredovisningar/`.
- Decision: Use public annual-report archives as the primary financial source.

## Attempt 4 — public downloadable files

- Date/time: 2026-07-02
- Source: User-provided official Bolagsverket page and direct URLs
- URLs:
  - `https://bolagsverket.se/apierochoppnadata/hamtaforetagsinformation/nedladdningsbarafiler.2517.html`
  - `https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip`
  - `https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip`
  - `https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/`
- Result:
  - Main Bolagsverket HTML page can present JavaScript/anti-bot verification to automated clients.
  - Direct ZIP URLs are publicly reachable.
  - `scb_bulkfil.zip` returned HTTP 200, `content-type: application/zip`,
    `last-modified: Mon, 29 Jun 2026 13:04:12 GMT`.
  - `bolagsverket_bulkfil.zip` returned HTTP 200, `content-type: application/zip`,
    `last-modified: Mon, 29 Jun 2026 01:27:14 GMT`.
- Decision: Documentation updated to bulk-first ingestion.

## Attempt 5 — local file inspection

- Date/time: 2026-07-02
- Source: Local files in `companycollect/companies/analysis/sweden/data_model/`
- Files:
  - `bolagsverket_bulkfil.txt`
  - `scb_bulkfil_JE_20260629T055245_80.txt`
  - `01_1.zip`
- Result:
  - `bolagsverket_bulkfil.txt`: UTF-8 semicolon CSV, 11 columns, ~2.96M data lines.
  - `scb_bulkfil_JE_20260629T055245_80.txt`: Latin-1 tab-separated text, 35 columns, ~1.82M data lines.
  - `01_1.zip`: annual-report sample with 1,512 nested company ZIPs; nested ZIPs contain XHTML/iXBRL.
- Decision: Update schema and ingestion notes using observed fields, encodings, and archive structure.

## Attempt 6 — authenticated API reassessment

- Date/time: 2026-07-02
- Source: Prior API notes plus user clarification
- Result: API access requires authentication with EU identity documentation/eID-style process.
- Decision: Do not build first ingestion around API. Use it only for future targeted enrichment if
  credentials are available.

## Attempt 7 — current repository and live-data inventory

- Date/time: 2026-07-23
- Source: Local Dagster/backoffice code and read-only ClickHouse queries
- Why: Separate missing-source coverage from parser/UI gaps.
- Result:
  - `companies_all` has 3,407,809 Sweden rows, 1,774,084 active rows,
    and 560,208 rows with the current financial flag.
  - Active limited-company forms total 817,643; 512,180 have financial
    data (62.6%).
  - `se_financial_reports` covers 572,074 companies and
    `se_financial_metrics` covers 570,472; only 1,602 report companies
    lack mapped metrics.
  - Main-list schema/filtering has no procurement or listing signal.
  - Wikidata listing coverage maps only 56 current Swedish listing
    entities to registry identifiers.
- Decision: Treat financial missingness mainly as source/eligibility, not
  parser loss; add tri-state signals to the shared list model.

## Attempt 8 — Upphandlingsmyndigheten national award data

- Date/time: 2026-07-23
- Source:
  - `https://www.upphandlingsmyndigheten.se/om-oss/var-oppna-data/`
  - `https://catalog.upphandlingsmyndigheten.se/rowstore/dataset/582c2145-af7d-4eb5-a02d-dffd60585ff0`
  - `https://catalog.upphandlingsmyndigheten.se/store/12/resource/239`
- Why: Find a Sweden-wide supplier/winner source, including procurement
  below EU thresholds.
- Result:
  - Public 115,068,644-byte CSV plus row API.
  - 102,785 rows covering 2021–2024, 40,245 procurement IDs, and
    19,983 distinct digits-only supplier IDs.
  - 18,564 supplier IDs matched the live Sweden company universe (92.9%).
  - Direct procurement is excluded and after-notice compliance is
    incomplete.
- Decision: Use UHM as the national primary source. Positive winner evidence
  is strong; a missing row is only “not observed in covered data.”

## Attempt 9 — TED Sweden feasibility

- Date/time: 2026-07-23
- Source: Existing `defs/ted_procurement` module and official TED
  Search API/eForms documentation.
- Why: Determine whether TED should be used in addition to UHM.
- Result:
  - Existing parser is already tested with Swedish multi-winner eForms.
  - The module is country-parameterized; Sweden needs `SWE`/`SE` added.
  - TED supplies current EU-threshold award XML and award values where
    reported, but the current module intentionally starts at 2024.
- Decision: Enable TED as the EU-threshold/current complement, not as a
  replacement for UHM.

## Attempt 10 — Nasdaq + GLEIF listing identity test

- Date/time: 2026-07-23
- Sources:
  - `https://api.nasdaq.com/api/nordic/screener/shares`
  - `https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files`
  - existing `gleif_lei_records`
- Why: Test whether current venue instruments can map deterministically to
  Swedish registry companies.
- Result:
  - 414 Stockholm Main instruments and 334 Stockholm First North
    instruments.
  - 725 of 748 ISINs matched the current GLEIF ISIN-to-LEI file.
  - The union resolved to 655 unique Swedish organisation numbers; all
    655 matched the live Sweden registry.
- Decision: ISIN -> LEI -> `registered_as` is the correct listing identity
  chain. Do not use names or Wikidata as the primary join.

## Attempt 11 — Spotlight and NGM venue sources

- Date/time: 2026-07-23
- Sources:
  - `https://www.spotlightstockmarket.com/en/market-overview/our-companies/`
  - `https://www.ngm.se/en/our-companies-eng`
  - NGM market-data documentation
- Why: Cover Swedish growth/MTF venues outside Nasdaq.
- Result:
  - Spotlight exposed 135 current company pages, all with organisation
    numbers, 113 with LEIs, and 125 matching Swedish registry rows.
  - NGM's public company page is current but primarily name/website based.
    Its Data API has the needed instrument/ISIN/status/segment fields, with
    market-data licensing.
- Decision: Use Spotlight for validation after a terms review. Use ESMA
  FIRDS for NGM boolean coverage or license the NGM Data API; do not
  auto-match NGM names.

## Attempt 12 — ESMA FIRDS and listing scope

- Date/time: 2026-07-23
- Sources:
  - `https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files`
  - current ESMA MiFIR reporting documentation
- Why: Find a reusable regulatory source covering regulated markets and
  MTFs with issuer LEIs.
- Result: Public weekly full and daily delta reference files expose ISIN,
  issuer LEI, MIC, classification, admission date, and termination date.
- Decision: Use FIRDS as the listing spine, filtered to current equities and
  declared venue MICs. Add venue feeds only for enrichment/validation.

## Attempt 13 — financial paper/digital distinction

- Date/time: 2026-07-23
- Sources:
  - Bolagsverket annual-report guidance
  - Bolagsverket annual-report statistics API documentation
  - Bolagsverket document-product documentation
- Why: Explain the missing financial population and identify an upgrade path.
- Result:
  - Every limited company must file within seven months after financial
    year end.
  - Bolagsverket still accepts paper reports.
  - The official statistics API explicitly reports paper and digital
    submissions separately.
  - Older document-delivery documentation describes scanned annual-report
    delivery, but access, cost, and current reuse terms require confirmation.
- Decision: “No digital report” is not “no annual report.” Keep the free
  iXBRL source, add ESEF for listed consolidated data, and investigate
  scanned-paper access before considering OCR.

## Attempt 14 — existing EODHD listing pipeline and live Sweden coverage

- Date/time: 2026-07-23
- Sources:
  - existing `defs/eodhd` Dagster source
  - live `eodhd_exchanges`, `eodhd_symbols`, `eodhd_symbol_mics`, and
    `eodhd_eod_prices` ClickHouse tables
  - `https://eodhd.com/financial-apis/covered-tickers-eodhd`
- Why: Determine whether direct Nasdaq Nordic ingestion is required before a
  company-level public-listing signal can be built.
- Result:
  - EODHD already collects global active and delisted symbol snapshots, MIC
    candidates, and historical/daily prices.
  - Stockholm (`ST`) resolves to `XSTO`.
  - There are 946 active and 670 delisted Stockholm common-stock symbols.
  - 737 active common stocks have ISIN and 209 do not.
  - Current tables have no LEI or Swedish `company_id`, and no downstream
    EODHD-to-company mapping exists.
- Decision: Use EODHD as the operational listing/price source, but add a
  deterministic identifier bridge before exposing the company flag. Keep
  absence gray because the provider defines active symbols through recent
  activity rather than official admission status.

## Attempt 15 — EODHD ID Mapping entitlement and GLEIF fallback

- Date/time: 2026-07-23
- Sources:
  - `https://eodhd.com/api/id-mapping`
  - `https://eodhd.com/financial-apis/id-mapping-api-cusip-isin-figi-lei-cik-%E2%86%94-symbol`
  - `https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files`
  - live `gleif_lei_records`
- Why: Choose the first implementation for symbol/ISIN/LEI/company mapping.
- Result:
  - EODHD documents symbol, ISIN, FIGI, and LEI mappings with exchange-scoped
    pagination.
  - A bounded `filter[ex]=ST` request returned HTTP 402 Payment Required under
    the configured subscription; no payload was saved.
  - GLEIF/ANNA publishes open daily ISIN-to-LEI relationship files.
  - Existing GLEIF data contains 117,831 Swedish LEIs with `registered_as`;
    117,478 normalize to ten digits and 114,995 match `se_companies`.
- Decision: First ingest the open GLEIF ISIN-to-LEI file and join existing
  EODHD ISINs through `gleif_lei_records.registered_as`. Treat the paid EODHD
  ID Mapping endpoint as an optional later gap-filler after a subscription
  decision.

## Attempt 16 — FIRDS value beyond EODHD

- Date/time: 2026-07-23
- Sources:
  - `https://www.esma.europa.eu/document/firds-instructions-access-and-download-full-and-delta-reference-data-files`
  - current ESMA FIRDS/MiFIR documentation
- Why: Explain whether EODHD makes FIRDS redundant.
- Result:
  - FIRDS adds exact `(ISIN, MIC)` regulatory records, issuer LEI, CFI
    classification, admission/first-trade and termination dates, and
    new/modified/terminated/cancelled lifecycle files.
  - It supports point-in-time status and exact EEA venue scope, while EODHD
    supplies operational global tickers and prices.
- Decision: Keep both roles. EODHD is the operational vendor layer; FIRDS is
  the official EEA identity, classification, venue, lifecycle, and
  completeness layer required before a trustworthy scoped red state.

## Attempt 17 — authenticated Bolagsverket Värdefulla datamängder API

- Date/time: 2026-08-17T08:10:09Z
- Source:
  - supplied `sweden-api/data.txt` connection note
  - supplied two-line OAuth client credential files (values not recorded)
  - Bolagsverket public developer portal and OpenAPI v1 specification
  - production gateway `https://gw.api.bolagsverket.se/vardefulla-datamangder/v1`
- Why: Test the supplied access and identify API fields not represented in the
  existing Sweden company and annual-report pipelines.
- Calls:
  - OAuth client credentials using HTTP Basic and form client authentication
  - `GET /isalive`
  - `POST /organisationer` for `5562434182`
  - `POST /dokumentlista` for `5562434182`
  - data-plane Basic and `ApiKey` header probes
- Result:
  - Public API metadata and the 74 KB OpenAPI specification returned HTTP 200.
  - The original client returned HTTP 401 `invalid_client`; the replacement
    client returned HTTP 200 and a one-hour token with read and ping scopes.
  - Data-plane calls without a valid bearer token returned HTTP 401, WSO2 code
    `900902`.
  - Authenticated `GET /isalive`, `POST /organisationer`, and both bounded
    `POST /dokumentlista` calls returned HTTP 200.
  - `5562434182` returned one active organisation and zero digital annual
    reports. Its organisation, SCB-introduction, and name dates were
    `1984-05-03`, `1984-08-19`, and `1984-11-08`, respectively.
  - Positive-control `5560187493` returned one digital report with an official
    document ID and registration date `2025-12-29`.
  - The contract documents four endpoints and 31 component schemas.
  - It confirms the compound registration identity role of
    `namnskyddslopnummer`, separate organisation/SCB dates, field-level
    producer/error provenance, and annual-report document ID/registration date.
- Decision:
  - Keep bulk ingestion primary.
  - Move the working credential to secret storage before integration.
  - Fix compound registration identity and SCB date semantics independently of
    API access.
  - Use the API for bounded targeted refresh and document discovery.
