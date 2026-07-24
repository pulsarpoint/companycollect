# Sweden company signals — financials, public procurement, and public listing

Analysis date: **2026-07-23**

Implementation plan:
[`corpscout/docs/superpowers/plans/2026-07-23-sweden-company-signals.md`](../../../corpscout/docs/superpowers/plans/2026-07-23-sweden-company-signals.md)

## Summary

**Decision (2026-07-23): v1 uses a two-state display contract — yes/unknown
only.** Almost no source in scope can prove an explicit "no," so the product
never renders a negative state. The most important product rule is:

```text
green = positive evidence
gray  = no positive evidence (unknown, unsupported, unmatchable, or stale)
```

A red/negative state is deliberately out of scope for v1. Because gray never
claims anything about the company, no source coverage gap, stale snapshot, or
missing paper filing can produce a false negative.

Sweden can support all three signals on the company list:

| Signal | Positive result | Could Sweden ever prove a negative? | v1 list behavior |
|---|---|---|---|
| Financial data available | We have at least one usable financial statement | Not for the company itself: paper filings are absent from the digital bulk source | Green when available; gray otherwise |
| Government contract observed | The company is a named winner in UHM or TED | No: direct purchases are excluded and after-notice compliance is incomplete | Green for an observed contract award; gray otherwise |
| Currently publicly traded | A current equity instrument maps to the company on an in-scope venue | FIRDS provides the required scoped foundation, but a red/negative UI remains a future product option | Green for a current listing; gray otherwise |

The evidence tables below stay rich enough (per-award rows, per-instrument
listing rows, provenance) that a scope-complete negative state — for example a
FIRDS-based "not on any Swedish venue" — can be added later without
re-ingestion. Only the display contract is simplified, not the storage.

### Recommended Sweden source stack

```text
financial availability
  Bolagsverket digital iXBRL bulk
  + ESEF consolidated IFRS filings for listed issuers
  + later paid/authenticated scanned-paper source if complete statutory coverage is required

public procurement
  Upphandlingsmyndigheten national award/supplier open data
  + TED eForms awards from 2024 onward

public listing
  ESMA FIRDS as the shared EU venue, classification, and lifecycle foundation
  + GLEIF LEI -> Swedish organisation number (and ISIN -> LEI fallback)
  + EODHD symbols and prices as operational enrichment
  + venue sources only for ticker/segment validation when needed
```

## What we have today

### Registry and company universe

The Sweden company pipeline is already a strong base:

- `se_companies`: about **3.41 million** company/entity rows.
- `se_company_addresses`: about **4.40 million** rows.
- `se_industries`: about **2.45 million** rows.
- Identity key: normalized Swedish organisation number.
- Available registry fields include legal name, legal form, status, status
  reason, incorporation/dissolution dates, activity description, address, and
  SNI/NACE-compatible industry codes.
- `companies_all` already exposes Sweden in the main cross-country company
  list.

The 3.41 million denominator is not “3.41 million active limited companies.”
It contains inactive entities, sole traders/person identifiers, partnerships,
associations, housing cooperatives, and other legal forms. Coverage claims
must use the eligible legal-form denominator for each signal.

### Financials

Live ClickHouse state on 2026-07-23:

| Measure | Count |
|---|---:|
| All Sweden rows in `companies_all` | 3,407,809 |
| Active Sweden rows | 1,774,084 |
| Main-list rows with `has_financials = 1` | 560,208 |
| Active main-list rows with financials | 512,184 |
| Main-list rows with a non-null revenue | 513,527 |
| Companies in `se_financial_reports` | 572,074 |
| Companies in `se_financial_metrics` | 570,472 |
| Report companies without mapped metrics | 1,602 |

The source-to-metrics loss is only about **0.28%** of report companies
(`1,602 / 572,074`). The missing-financial problem is therefore primarily an
upstream coverage/eligibility problem, not an iXBRL parser problem.

For active limited-company forms (`AB-ORGFO` plus the SCB fallback code `49`):

| Measure | Count |
|---|---:|
| Active limited companies | 817,643 |
| With financial data in the main list | 512,180 |
| Without financial data in the main list | 305,463 |
| Coverage | 62.6% |

Among the missing active limited companies, 28,310 were incorporated in 2026
and 40,015 in 2025. Many recent companies will not yet have reached their first
filing deadline. Incorporation date alone cannot determine the exact deadline
because the financial year end is needed.

What is already available from the filings is substantially richer than the
main-list icon:

- annual report periods and fiscal year;
- revenue, operating result, profit/loss, assets, equity, liabilities, cash,
  current assets/liabilities, personnel costs, wages, and employees;
- full long-form iXBRL facts;
- comparative-year financial history;
- annual-report signers/officers;
- audit firm/opinion data and modified-opinion signal;
- source archive and exact source-document provenance.

### Public procurement

There is already a generic `ted_procurement` module and a reusable
`public-contracts-section.tsx`, but:

- TED is enabled only for Finland;
- Sweden has no public-contract query wired into its detail-page country
  configuration;
- the main list has no procurement indicator or filter.

The existing TED parser is already tested against Swedish eForms cases,
including multi-winner framework awards. Enabling Sweden is a small
configuration change:

```text
place_code = SWE
country_iso2 = SE
```

No new Swedish organisation-number parser is required for ordinary 10-digit
winner identifiers.

### Public listing

The current listing signal is not suitable for the main list:

- Wikidata listing data is available only in the detail query.
- Only **56** Wikidata Swedish items with a current listing currently map to a
  Swedish registry number.
- A missing Wikidata row is not evidence that a company is unlisted.
- The main list and its filter model have no listing fields.

Existing ESEF and GLEIF data are useful building blocks:

- `esef_entity_registry_map`: **404** Swedish LEIs mapped to Swedish registry
  identifiers.
- `esef_filings`: **1,415** Swedish-country filings from **417** issuers.
- ESEF is strong evidence for regulated-market issuers, but it is historical,
  filing-based, and excludes many MTF/growth-market issuers. It cannot be the
  sole current-listing source.

The existing EODHD pipeline is a much stronger operational starting point than
Wikidata:

| Live EODHD measure on 2026-07-23 | Count |
|---|---:|
| Global exchanges | 65 |
| Global active + delisted symbols | 222,845 |
| Stockholm active common-stock symbols | 946 |
| Stockholm delisted common-stock symbols | 670 |
| Stockholm active common stocks with ISIN | 737 |
| Stockholm active common stocks without ISIN | 209 |

The current `eodhd_symbols` table already stores symbol, name, exchange,
instrument type, ISIN, and `is_delisted`; `eodhd_symbol_mics` resolves the
provider exchange to MIC candidates; `eodhd_eod_prices` stores prices. However:

- the current EODHD tables have no LEI or Swedish `company_id`;
- no downstream asset currently links EODHD listings to `se_companies`;
- the provider's `ST` exchange is resolved to `XSTO`, which is too coarse to
  distinguish every Swedish regulated/growth venue and segment;
- EODHD documents its active symbol list as symbols active in the past month,
  which is an activity-based vendor definition rather than a legal admission
  status.

The GLEIF company side is already strong. Live `gleif_lei_records` contains
117,831 Swedish LEIs with `registered_as`; 117,478 normalize to ten digits and
114,995 match `se_companies`. The missing piece is the instrument-to-LEI
crosswalk, not the LEI-to-company join.

## Financial information

### Why so many companies appear to be missing financials

There are four separate causes:

1. **Wrong denominator.** Sole traders, partnerships, associations, and other
   entities do not have the same public annual-report obligation as limited
   companies.
2. **The bulk source is digital-only.** The Bolagsverket high-value archive is
   made from digitally submitted iXBRL reports. Bolagsverket still accepts
   paper annual reports.
3. **New companies may not be due yet.** A Swedish limited company files
   within seven months after its financial year end, not seven months after
   incorporation.
4. **A small technical tail remains.** There are 1,602 report companies
   without mapped metrics, plus a roughly 10,000-company difference between
   metric-table coverage and the `companies_all` projection that should be
   audited separately.

Bolagsverket states that every limited company, including dormant companies
and companies in liquidation, must file annually. Therefore “no iXBRL found”
does **not** mean “the company has no financial statements.”

### Best free financial source

Continue using:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
```

It is the best free, structured, batch source:

- native iXBRL;
- direct organisation-number matching;
- rich facts rather than PDF-only documents;
- weekly/current ingestion is already implemented;
- source provenance is already retained.

### How to improve coverage

#### 1. Fix eligibility and availability semantics first

Expose two different concepts:

```text
financial_filing_obligation
financial_data_available
```

`financial_filing_obligation` is derived from normalized legal form and, when
necessary, size/status rules. `financial_data_available` is a property of our
data, not a claim about the company.

The main-list icon should be labelled **Financial data available**:

- green: usable statement exists;
- gray: no usable statement in Corpscout, no country source/coverage, or the
  entity cannot be evaluated.

Gray makes no claim about the company, so the missing paper filings cannot
produce a false negative.

#### 2. Add ESEF as a separate consolidated-financial layer

For listed Swedish issuers, ESEF provides IFRS consolidated filings. Do not
overwrite the legal-entity statutory numbers from Bolagsverket:

```text
statement_scope = legal_entity_statutory | group_consolidated_ifrs
```

This improves the quality and breadth of financial data for public companies,
but not the long tail of private limited companies.

#### 3. Determine whether complete paper-document access is worth paying for

The public statistics API explicitly distinguishes paper and digital
submissions, confirming that the free iXBRL archive is not the complete filing
population. To ingest paper reports, the practical path is likely:

```text
Bolagsverket document-list/document-delivery product
-> scanned annual-report PDF
-> document classification/OCR/table extraction
-> normalized metrics with lower confidence
```

The older Bolagsverket XML documentation describes delivery of the latest
scanned annual report. Access, price, redistribution rights, and whether the
current Värdefulla datamängder API includes scanned paper reports must be
confirmed with Bolagsverket before implementation.

Do not build PDF OCR until this access question is answered. It is a much more
expensive and lower-confidence path than the existing iXBRL pipeline.

#### 4. Audit the final projection

Measure and explain:

```text
se_financial_reports companies       572,074
se_financial_metrics companies       570,472
companies_all has_financials         560,208
```

The first gap is mapping/parser coverage; the second is projection/registry
join/latest-row eligibility. They should have separate quality checks.

## Public procurement

### Primary source: Upphandlingsmyndigheten

The best Sweden-specific source is the official open-data dataset:

```text
catalog:
https://www.upphandlingsmyndigheten.se/om-oss/var-oppna-data/

row API:
https://catalog.upphandlingsmyndigheten.se/rowstore/dataset/582c2145-af7d-4eb5-a02d-dffd60585ff0

CSV:
https://catalog.upphandlingsmyndigheten.se/store/12/resource/239
```

The dataset is row-level supplier/award evidence, not merely an aggregate.
Observed fields include:

- year;
- procurement ID and lot/tender-area ID;
- publication date and title;
- buyer name and organisation number;
- supplier name and organisation number;
- contracted flag;
- contract versus framework-agreement type;
- CPV classifications;
- buyer/supplier legal form, sector, SNI, and supplier size;
- source advertising database.

Observed snapshot, retrieved 2026-07-23:

| Measure | Count |
|---|---:|
| Rows | 102,785 |
| Covered years | 2021–2024 |
| Distinct procurement IDs | 40,245 |
| Buyers | 1,241 |
| Distinct non-empty supplier IDs before normalization | 20,049 |
| Distinct digits-only supplier IDs | 19,983 |
| Digits-only IDs matching `companies_all` Sweden | 18,564 |
| Match rate | 92.9% |
| Matched active companies | 17,783 |
| Matched companies with financials | 9,628 |

Rows by year:

| Year | Rows | Supplier IDs | Procurements |
|---:|---:|---:|---:|
| 2021 | 26,027 | 9,237 | 10,772 |
| 2022 | 24,658 | 8,831 | 10,615 |
| 2023 | 24,930 | 8,185 | 9,697 |
| 2024 | 27,170 | 8,032 | 9,161 |

Agreement types:

| Type | Rows | Supplier IDs |
|---|---:|---:|
| Framework agreement | 58,856 | 13,714 |
| Contract | 43,887 | 12,151 |
| Missing type | 42 | 34 |

This source should be the primary Sweden procurement layer because it covers
advertised Swedish procurement both below and above EU thresholds.

### Complementary source: TED

TED must also be enabled for Sweden.

It adds:

- EU-threshold award notices;
- current 2024+ eForms coverage while the UHM open-data release is annual and
  currently stops at 2024;
- structured lot-result/winner links;
- award and estimated values with currencies where reported;
- buyer, winner, CPV, NUTS, notice type, and publication dates;
- direct source XML and EU notice identifiers.

The existing module is already country-parameterized and already solves:

- per-notice eForms XML retrieval;
- national winner IDs from `CompanyID`;
- multi-winner framework awards;
- monthly partitions;
- raw XML/manifests in object storage;
- normalized `ted_notices` and `ted_notice_winners`;
- currency conversion.

The recommended source union is:

```text
UHM 2021 onward advertised Swedish awards
UNION
TED 2024 onward EU-threshold eForms awards
```

For the boolean signal, duplicate rows do not affect the result. Before showing
award counts or values, deduplicate overlapping UHM/TED evidence using the TED
publication number when present, otherwise a conservative composite of buyer,
supplier, lot/procurement identifier, publication date, and title.

### Procurement limitations

The official 2024 statistics make the negative-result problem explicit:

- direct procurement is excluded;
- call-offs under framework agreements are not necessarily present;
- only 63.7% of 2024 advertised procurements had an after-notice as of the
  official cutoff;
- contracted value was present for only about 52% of 2024 advertised
  procurements;
- manually entered values can be zero, unit prices, or otherwise implausible.

Therefore the source can prove:

```text
company X was observed winning at least one covered public award
```

It cannot prove:

```text
company X has never contracted with a public institution
```

Recommended UI language:

- green: **Government contract observed**
- gray: **No award observed in covered sources / unknown**

The tooltip should show:

```text
Sources: Upphandlingsmyndigheten + TED
Coverage: UHM 2021–2024; TED eForms 2024–current
Excludes: direct/non-advertised purchases and missing after-notices
```

### Procurement ingestion design

```text
UHM CSV/row API
  -> immutable raw CSV + metadata/checksum
  -> DuckDB source tables
  -> normalized award + winner tables
  -> exact org-number match to se_companies

TED monthly search
  -> raw XML + manifest
  -> existing generic TED parser
  -> ted_notices + ted_notice_winners

both
  -> company_public_procurement_summary
  -> companies_all signal projection
```

Keep source-specific rows. Do not collapse UHM and TED into a single lossy
record before deduplication and provenance are available.

## Public listing

### Define the signal before collecting it

Do not infer a listing from:

- `AB (publ)` in the legal name;
- a public-company legal form;
- an old prospectus;
- an old ESEF filing;
- a Wikidata statement without current venue validation.

In Sweden, `publ` means a public limited company; it does not by itself mean
that the shares are admitted to trading.

Recommended v1 definition:

```text
The company currently has an equity instrument admitted to trading on:
  Nasdaq Stockholm Main Market
  Nasdaq First North Growth Market Stockholm
  NGM Main Regulated
  NGM Nordic SME
  Spotlight Stock Market
```

Include suspended/observation-status instruments as listed, but store the
instrument status. Exclude bonds, funds, ETPs, warrants, and other non-equity
instruments from the company-level equity-listing flag.

A broader “listed anywhere in the EEA” flag can be derived later from the same
country-neutral FIRDS foundation.
Truly worldwide listing coverage requires additional non-EEA venue/reference
data and should not be implied by the Swedish-venue v1 flag.

### First operational step: a reusable EU FIRDS foundation

FIRDS should be implemented before the Sweden-specific listing projection.
The ingestion must retain all EEA countries, MICs, and instrument classes,
including full, delta, termination, invalid, and cancellation history.
Sweden is only the first downstream country configuration.

```text
ESMA FIRDS weekly full + daily delta/cancellation files
  -> immutable instrument lifecycle events
  -> current exact (ISIN, MIC) state for all EEA venues
  -> per-country equity/MIC projection
  -> issuer LEI
  -> GLEIF registered_as
  -> normalized country registry identifier
```

For Sweden, resolve the configured equity venues as:

```text
FIRDS current (ISIN, MIC, CFI, issuer LEI)
  -> GLEIF LEI record
  -> primary_country_iso2 = SE
  -> registered_as
  -> digits-only 10-character Swedish organisation number
  -> se_companies.company_id
```

Then enrich the FIRDS-authoritative row with EODHD ticker, symbol metadata,
and prices by ISIN. Where a source row lacks a usable issuer LEI, the open
GLEIF/ANNA ISIN-to-LEI relationship can act as an identity fallback.

The current EODHD plan also advertises a separately entitled ID Mapping API.
A bounded probe on 2026-07-23 returned HTTP **402 Payment Required** with the
configured subscription. It is therefore not a dependency. It can be added
later to enrich/cross-check symbols, but it must not replace FIRDS lifecycle
state or the open GLEIF identity bridge.

For the first green signal, require:

```text
exact (ISIN, MIC) is current in fresh, sequence-complete FIRDS state
AND CFI is an allowed equity class
AND MIC is in the configured Sweden scope
AND issuer LEI maps deterministically to a Swedish company_id
```

EODHD-only active rows remain useful QA/enrichment evidence but do not create
the Sweden green signal. Absence still stays gray in v1; ingesting FIRDS now
does not itself authorize a red/negative display.

### Why FIRDS comes before EODHD enrichment

EODHD and FIRDS serve different roles:

| Requirement | EODHD | ESMA FIRDS |
|---|---|---|
| Operational ticker universe | Strong global vendor coverage | EEA regulatory instruments |
| End-of-day prices | Yes | No |
| Coarse active/delisted flag | Yes | Lifecycle records and dates |
| ISIN | Often; 209/946 active Stockholm common stocks currently missing | Instrument identifier is fundamental to the record |
| Issuer LEI | Available through a separately entitled endpoint; not ingested | Reported directly for applicable instruments |
| Instrument classification | Provider type such as `Common Stock` | Regulatory CFI classification |
| Exact venue | Current Sweden mapping collapses `ST` to `XSTO` | One record per ISIN/MIC relationship |
| Admission/termination history | Not present in our current symbol schema | Admission/first-trade and termination dates plus deltas |
| Regulatory provenance | Commercial vendor | Trading-venue/SI reports under MiFIR/MAR |

FIRDS provides four product capabilities that EODHD alone cannot currently
provide (the fourth is a future option, not a v1 requirement):

1. **Authoritative venue scope.** FIRDS identifies the exact trading venue by
   MIC, allowing Stockholm Main, First North, NGM, Nordic SME, and Spotlight
   to be evaluated separately instead of treating every Swedish symbol as
   `XSTO`.
2. **Reliable instrument filtering.** CFI-based classification prevents bonds,
   funds, ETFs, warrants, and derivatives from accidentally producing the
   company-level equity flag.
3. **Lifecycle and point-in-time status.** Full, delta, invalid, and
   cancellation files distinguish new, modified, terminated, and cancelled
   `(ISIN, MIC)` records. Admission/first-trade and termination dates support
   both “currently traded” and historical “was traded on date T” queries.
4. **A future negative state.** A fresh, complete FIRDS snapshot filtered to
   declared EEA venues is the only realistic basis for a defensible “not found
   in the complete in-scope venue set.” V1 deliberately does not render a
   negative; this option is preserved for later. EODHD absence remains
   vendor-coverage uncertainty and stays gray regardless.

FIRDS therefore does not replace EODHD. FIRDS is built first as the shared
official EEA identity, venue, classification, and lifecycle layer; EODHD
remains the operational global ticker/price enrichment source.

### Additional information available from FIRDS

ESMA FIRDS is the best reusable regulatory spine:

- full files weekly and delta files daily;
- instrument ISIN;
- issuer/operator LEI;
- trading venue MIC;
- admission/first-trade date;
- termination date;
- instrument classification.

Depending on the instrument class, FIRDS also exposes:

- request-for-admission and issuer-approval indicators;
- relevant competent authority country;
- notional currency;
- maturity/expiry dates and price multiplier;
- underlying ISIN, basket, or index identifiers;
- option type, strike price/currency, and exercise style;
- delivery type;
- interest-rate/index reference fields for debt instruments;
- commodity/emission-allowance classification fields.

These fields enable more than the main-list boolean:

```text
current equity listings by company and venue
historical listings as of a selected date
first admission / last termination timeline
multi-class and multi-venue issuer views
listed debt versus listed equity distinction
underlying-instrument relationships
official venue and competent-authority evidence
```

FIRDS does **not** provide prices, volume, market capitalization, free float,
company financial statements, or Swedish registry status. Those remain EODHD,
Bolagsverket, and derived-data responsibilities.

Filter current equity instruments by the in-scope MICs:

| Venue | MIC |
|---|---|
| Nasdaq Stockholm | `XSTO` |
| Nasdaq First North Sweden | `FNSE` |
| NGM Main Regulated | `XNGM` |
| NGM Nordic SME | `NSME` |
| Spotlight Stock Market | `XSAT` |

Then resolve:

```text
FIRDS instrument ISIN
-> issuer LEI
-> GLEIF registered_as
-> normalized 10-digit Swedish organisation number
-> se_companies.company_id
```

Where a venue source provides ISIN but not issuer LEI, use GLEIF's open daily
ISIN-to-LEI relationship file. The mapping is open and updated daily, though
GLEIF notes that participating national numbering agencies determine coverage
and that historical/pre-existing ISIN coverage is still being expanded.

### Empirical Nasdaq test

The public Nasdaq Nordic screener endpoint exposes current Stockholm
instruments:

```text
https://api.nasdaq.com/api/nordic/screener/shares
```

Snapshot tested on 2026-07-23:

| Segment | Instruments | ISIN -> LEI mapped | Unique Swedish org numbers | Registry matches |
|---|---:|---:|---:|---:|
| Stockholm Main Market | 414 | 403 | 350 | 350 |
| Stockholm First North | 334 | 322 | 305 | 305 |
| Union | 748 | 725 mapping rows | 655 | 655 |

The difference between instruments and companies is expected because a company
can have multiple share classes and some Stockholm-traded issuers are foreign.

This is much better than the current 56 registry-matched Wikidata listings.
However, Nasdaq sells licensed Nordic reference-data products and publishes
data-use policies. The public website API is useful for validation/prototyping,
but production storage/redistribution rights must be confirmed before it is
used as the primary feed.

### Empirical Spotlight test

Spotlight's public “Our Companies” page currently exposes 135 company pages.
Each tested contact page provides:

- organisation number;
- LEI when available;
- instrument ID;
- legal/company name and contact fields.

Observed on 2026-07-23:

| Measure | Count |
|---|---:|
| Current Spotlight company/instrument pages | 135 |
| Pages with organisation number | 135 |
| Pages with LEI | 113 |
| Organisation numbers matching Sweden registry | 125 |

The ten non-matches are consistent with foreign issuers. Matching quality is
excellent because the source exposes the organisation number directly.

Production scraping/republication rights still need a terms review. Spotlight
is best used as venue validation/enrichment unless written reuse permission or
a licensed feed is obtained.

### NGM

NGM's official public company page supplies the current company universe, but
primarily as names and issuer websites. Its official Data API/reference data
offers instrument name, symbol, ISIN, status, listing, segment, and product
type, but NGM's market-data policy governs non-display use and redistribution.

Options in order:

1. use ESMA FIRDS for the current boolean and entity linkage;
2. license the NGM Data API for exact venue/ticker/status enrichment;
3. use the public company page only for validation, not name-only automatic
   entity matching.

### Listing status model

Keep one row per instrument:

```text
company_listings
  country_code
  company_id
  issuer_lei
  isin
  ticker
  instrument_name
  instrument_type
  venue_name
  mic
  market_type             regulated_market | mtf | growth_market
  segment
  admission_date
  termination_date
  trading_status
  is_current
  identity_match_method
  identity_match_confidence
  listing_status_source
  source_slug
  source_record_id
  source_retrieved_at
```

The company summary is:

```text
is_publicly_traded = any current equity listing in the declared scope
```

The detailed table supports multi-class and multi-venue issuers without
duplicating companies on the main list.

## Cross-country product and schema

### Use a two-state signal that never fakes a negative

For the main search projection:

```text
has_financial_data       UInt8
has_government_contract  UInt8
is_publicly_traded       UInt8
```

Meaning:

| Stored value | UI | Meaning |
|---:|---|---|
| `1` | green | confirmed positive |
| `0` | gray | no positive evidence (unknown/unsupported/unmatchable/stale) |

`0` must never be rendered as a negative claim about the company. This keeps
the existing non-null `has_financials UInt8` storage contract — only the label
and rendering change, and no cross-country migration is required. If a
scope-complete negative is ever added (for example FIRDS-backed listing
absence), that is the point to widen the type; the underlying evidence tables
already retain enough provenance to backfill it without re-ingestion.

Add useful summary fields:

```text
financial_latest_year       Nullable(Int32)
public_award_count           Nullable(UInt32)
public_award_last_date       Nullable(Date)
listing_venue_count          Nullable(UInt16)
listing_markets              Array(String)
signals_resolved_at          DateTime64
```

Store country-level coverage separately:

```text
company_signal_coverage
  country_code
  signal_name
  coverage_status            unavailable | partial | complete
  coverage_from
  coverage_to
  source_slugs
  source_updated_at
  resolved_at
  caveat
```

This prevents repeating the same source caveat on millions of company rows and
gives the UI enough information for tooltips and gray states.

### Main-list filters

Use explicit filter values:

```text
financial_data      = available | unknown
government_contract = observed | unknown
public_listing       = current | unknown
```

The `unknown` option doubles as the practical “no data here” filter — nothing
is lost functionally; only the claim that absence means absence is removed.

For Sweden, the initial values should be:

| Signal | Green | Gray |
|---|---|---|
| Financial data | usable Bolagsverket/ESEF statement exists | no usable statement in Corpscout, or unsupported/unmatchable |
| Government contract | UHM or TED winner match exists | no match in covered sources, invalid/unmatchable ID, or stale/failed source |
| Public listing | current equity listing in in-scope venue set | no matched current listing, unresolved entity, or incomplete/stale venue snapshot |

Facet counts must include both states. The existing “Has financials”
positive-only filter becomes a two-option filter.

### Main-list icons

Each icon should have:

- color/state;
- accessible text, not color alone;
- tooltip with source scope and last refresh;
- link to evidence on the detail page for green results.

Suggested labels:

```text
Financial data available
Government contract observed
Currently equity-traded
```

Avoid the ambiguous labels “has financials” and “public company.” For
government contracts, pair the label with “observed” in accessible text and
coverage tooltips so gray never claims that no contract exists.

## Best sources found

| Source | Signal/role | Access and cadence | Entity matching | Negative-result strength | Reuse status |
|---|---|---|---|---|---|
| Bolagsverket company + SCB bulk | registry spine and eligibility | public ZIP, about weekly | direct org number | strong for registry facts | open/high-value; exact formal text still to archive |
| Bolagsverket digital annual-report bulk | statutory financial availability | public iXBRL archives, weekly/current | direct org number | cannot rule out paper filing | open/high-value; raw-document wording to confirm |
| Bolagsverket annual-report statistics API | quantify paper vs digital | public documented API | aggregate only | proves source incompleteness, not company status | official public statistics |
| ESEF filings + metrics | listed consolidated IFRS financials | public EU filing index/package | LEI -> GLEIF org number | historical filing evidence, not current listing absence | public regulatory data |
| Upphandlingsmyndigheten supplier-award dataset | primary Swedish public-award evidence | public CSV, JSON row API, annual release | direct supplier org number | partial; direct procurement and missing after-notices | free with attribution/date/period |
| TED Search API + eForms XML | EU-threshold award complement | keyless API; monthly/daily possible | winner national ID | partial; EU threshold and 2024+ module scope | notices freely reusable; metadata CC0 |
| EODHD exchange/symbol/price APIs | operational global listing and price layer | authenticated vendor API; weekly references + daily prices in our pipeline | ISIN -> GLEIF LEI -> org number | positive evidence; absence is not authoritative | current plan covers symbols/prices; ID Mapping probe returned HTTP 402 |
| ESMA FIRDS | current EEA instrument/venue spine | public weekly full + daily delta XML | issuer LEI | strong within declared MIC/instrument scope | public EU regulatory data |
| GLEIF ISIN-to-LEI + LEI records | listing identity crosswalk | public daily files | LEI `registered_as` -> org number | matching layer only | open/free mapping files |
| Nasdaq Nordic screener/API | Stockholm current instrument validation | public web endpoint; licensed production products also offered | ISIN -> GLEIF -> org number | strong for current Nasdaq scope if feed is complete/fresh | production reuse/licensing must be confirmed |
| Spotlight company/contact pages | Spotlight current issuer validation | public web pages | direct org number and LEI | strong for current Spotlight page scope | terms/republication review required |
| NGM company page / Data API | NGM venue validation and enrichment | public names page; licensed/reference API | ISIN/LEI through API/FIRDS | strong with licensed API or FIRDS | market-data policy applies |
| Wikidata | supplementary enrichment | SPARQL | some direct Swedish IDs | weak; absence is meaningless | open, but inadequate as primary |

## What I tried

1. Inspected the current Sweden Dagster pipelines, ClickHouse schemas,
   `companies_all`, detail-page queries, list columns, and filter model.
2. Queried the live ClickHouse data to measure total/active/eligible financial
   coverage and report-to-metrics loss.
3. Inspected the existing TED implementation and Swedish parser fixtures.
4. Located and downloaded the official UHM supplier-award CSV and inspected
   it with DuckDB.
5. Normalized UHM supplier identifiers and matched them to the live Sweden
   company universe.
6. Inspected the current Nasdaq Nordic screener endpoint and instrument-detail
   endpoints.
7. Downloaded the current GLEIF ISIN-to-LEI mapping temporarily and measured
   Nasdaq ISIN/LEI/org-number coverage.
8. Inspected all current Spotlight company/contact pages and verified direct
   organisation-number/LEI availability.
9. Inspected NGM's current company page, Data API description, market-data
   policy position, venue MICs, and official market structure.
10. Reviewed ESMA FIRDS bulk-access documentation and the existing ESEF/GLEIF
    data already loaded in Corpscout.
11. Reviewed official Bolagsverket guidance confirming annual filing
    obligations, paper/digital submissions, and the seven-month deadline.
12. Reviewed official UHM quality notes for direct-procurement exclusions,
    after-notice completeness, and contract-value quality.
13. Inspected the existing EODHD Dagster assets and live ClickHouse tables:
    global active/delisted symbols, MIC resolution, and daily/history prices
    are already collected, but no LEI/company mapping exists.
14. Measured live Stockholm EODHD coverage: 946 active and 670 delisted common
    stocks; 737 active common stocks have ISIN and 209 do not.
15. Probed EODHD's documented exchange-scoped ID Mapping API with the current
    subscription. It returned HTTP 402 Payment Required; no response data was
    saved.
16. Verified the open GLEIF daily ISIN-to-LEI relationship files and measured
    the existing LEI-to-company side: 114,995 Swedish LEIs currently resolve
    to live `se_companies` rows.

## Data saved

Official UHM source snapshot:

```text
companies/data/sweden/raw/bulk/uhm_contracted_bids_with_suppliers.csv
companies/data/sweden/raw/bulk/uhm_contracted_bids_with_suppliers.csv.headers
companies/data/sweden/raw/bulk/uhm_contracted_bids_with_suppliers.csv.metadata.json
```

Snapshot properties:

```text
size:   115,068,644 bytes
sha256: 4731f86f6930e346f3d0a888cf7e770ef91d1cd3946cf6371ad4c9372d40ed62
last-modified: 2026-04-28T14:58:13Z
```

Nasdaq and GLEIF files used for the one-off coverage test were temporary and
were not added to the repository. Their URLs and measured results are recorded
in this document and the source inventory.

The EODHD ID Mapping probe returned HTTP 402 before any mapping payload was
received, so no raw API response was saved.

## Recommended ingestion approach

### Phase 1 — correct product semantics

1. Keep the signal storage as plain `UInt8` (`1` = confirmed positive, `0` =
   no positive evidence).
2. Render `0` as neutral/gray everywhere; never as a negative claim.
3. Add country/signal coverage metadata for tooltips.
4. Rename the financial label to “Financial data available.”
5. Add two-state filters and facet counts.

This removes the misleading implication that absence of data means a negative
fact about the company, with no storage migration required.

### Phase 2 — Sweden procurement

1. Build the UHM CSV source as the national primary layer.
2. Enable `SWE` in the existing TED module.
3. Add a Sweden public-contract detail query using both sources.
4. Build a per-company procurement summary.
5. Add the main-list signal/filter with coverage tooltip.

This is the highest-value, lowest-uncertainty new signal.

### Phase 3 — Sweden listing

1. Ingest FIRDS weekly full plus daily delta/cancellation data into one
   country-neutral EU instrument-event history and deterministic current-state
   table. Do not filter source ingestion to Sweden.
2. Add the daily GLEIF ISIN-to-LEI relationship file to the existing GLEIF
   module and use existing LEI `registered_as` identity data.
3. Build `eodhd_company_listings` as source-specific symbol/price enrichment
   and resolution QA.
4. Build the canonical Sweden listing projection from FIRDS exact
   `(ISIN, MIC)` state, map issuer LEIs to organisation numbers, and enrich
   matching ISINs with EODHD tickers/prices.
5. Reconcile conflicts with FIRDS controlling EEA venue lifecycle and EODHD
   remaining the operational ticker/price source.
6. Validate Nasdaq, Spotlight, and NGM counts against venue sources.
7. Add current-listing icon/filter and detail evidence.

FIRDS is load-bearing for the first Sweden listing green signal and is
implemented first because the same foundation supports every later EEA
country. The UI remains green/gray: FIRDS completeness preserves the option
of a venue-scoped negative state, but red requires a separate future product
decision.

### Phase 4 — financial coverage improvement

1. Fix eligibility and audit the 570,472 -> 560,208 projection gap.
2. Add ESEF consolidated financials as a separate statement scope.
3. Ask Bolagsverket for exact access/licensing/cost details for scanned paper
   annual reports and document delivery.
4. Only if the business value justifies it, build the PDF/OCR extraction
   path.

## Open questions / risks

1. **Listing scope:** does “publicly traded” mean Swedish venues, any EEA
   venue, or anywhere worldwide? V1 should explicitly say Swedish regulated
   markets and Swedish MTF/growth markets.
2. **Personal data in procurement suppliers:** Swedish sole traders use the
   personal identity number as the organisation number, so the UHM supplier ID
   column contains personnummer for person-form suppliers (and some of the
   ~7% unmatched digits-only IDs plausibly are, too). Decide whether
   person-form suppliers are ingested/displayed at all before the procurement
   layer ships.
3. **Paper financials:** confirm whether any current Bolagsverket high-value
   API endpoint delivers scanned paper filings. Do not assume it does.
4. **Market-data rights:** confirm production storage and display rights for
   Nasdaq, NGM, and Spotlight data. Prefer FIRDS/GLEIF for the boolean when
   venue licensing is unclear.
5. **EODHD entitlement:** the current account can collect the exchange/symbol
   and price data already in production, but the ID Mapping endpoint returned
   HTTP 402. Do not design a required dependency on it without a subscription
   decision.
6. **EODHD ISIN gaps:** 209 active Stockholm common stocks currently lack ISIN
   in the symbol list. Keep those unresolved/gray unless FIRDS, an upgraded
   EODHD endpoint, or a licensed venue source supplies a deterministic ID.
7. **FIRDS interpretation:** filter equity CFIs and current termination dates
   carefully; secondary admissions should still count as publicly traded, but
   must not be described as a primary listing.
8. **FIRDS history:** full files alone are insufficient for point-in-time
   status. Preserve and apply delta, invalid, and cancellation records.
9. **Legal-form normalization:** normalize SCB numeric legal-form codes before
   calculating financial-filing eligibility.
10. **Recent-company due dates:** incorporation date is only an approximation;
   exact filing-due logic needs financial year end.
11. **Procurement values:** values are useful but lower-confidence than the
   winner boolean. Keep quality flags and do not sum overlapping UHM/TED rows
   before deduplication.
12. **Foreign suppliers/issuers:** retain unmatched foreign entities in source
   tables, but do not attach them to Swedish registry rows by name alone.
13. **Refresh failure:** with no negative state, a stale or failed source
    snapshot cannot create a false negative, but it must still be visible in
    the coverage metadata, and green evidence should carry its retrieval date
    so aging positives can be spotted.
