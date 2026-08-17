# Sweden — source inventory

| # | Source | Type | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| 1 | **Bolagsverket legal-register bulk file** | Official registry bulk | Public direct ZIP, refreshed about every 7 days | ZIP, CSV-like text | No | **recommended primary company source** |
| 2 | **SCB/FDB bulk file via Bolagsverket high-value datasets** | Statistical business register bulk | Public direct ZIP, refreshed about every 7 days | ZIP, tab-separated text | No | **recommended secondary/company universe source** |
| 3 | **Bolagsverket annual-report archives** | Official filings bulk | Public directory of ZIP archives | ZIP, nested ZIP, XHTML/iXBRL | **Yes** | **recommended primary financial source** |
| 4 | Bolagsverket Värdefulla datamängder API | Official API | OAuth2 client credentials; replacement client verified on 2026-08-17 | JSON, ZIP, iXBRL | Yes | **useful targeted source**; company refresh and digital-report discovery |
| 5 | dataportal.se / Bolagsverket source pages | Catalog/documentation | Public browser pages | HTML/DCAT metadata | No | useful_secondary_source |
| 6 | Verklig huvudman (UBO) | Official registry | Restricted | unknown | No | out of scope |
| 7 | Commercial aggregators | Third-party | Paid/keyed | JSON/PDF/iXBRL | sometimes | fallback/comparison only |

## Company-list signal sources

| # | Source | Signal | Access | Coverage/cadence | Matching | Status |
|---|---|---|---|---|---|---|
| 8 | **Upphandlingsmyndigheten — contracted bids with suppliers** | Public award | Public CSV + JSON row API | 2021–2024 in the inspected annual snapshot | Direct supplier organisation number; 18,564/19,983 normalized IDs matched live Sweden companies | **recommended national primary** |
| 9 | **TED Search API + eForms XML** | Public award | Keyless API/XML | Existing module starts 2024; monthly/current | Winner national identifier; Swedish parser already works | **recommended EU-threshold complement** |
| 10 | **EODHD exchange/symbol/price APIs** | Operational public-listing and market-data source | Authenticated vendor API | Global; current pipeline collects weekly reference snapshots and daily/history prices | Existing ISIN → GLEIF ISIN/LEI → `registered_as`; direct EODHD ID Mapping is subscription-gated | **recommended operational primary**; positive evidence first |
| 11 | **ESMA FIRDS** | Official current/historical public listing | Public weekly full + daily delta/cancellation XML | EEA instrument reference data | Issuer LEI + exact MIC + ISIN | **recommended regulatory listing spine** |
| 12 | **GLEIF ISIN-to-LEI + LEI records** | Listing identity | Public daily files | Daily; participating-NNA/historical coverage limitations | ISIN → LEI → `registered_as` Swedish org number | **recommended identity bridge** |
| 13 | Nasdaq Nordic screener/reference data | Current listing validation/enrichment | Public website endpoint; licensed production products | Current Stockholm Main + First North | ISIN → GLEIF; 655 unique Swedish org numbers matched in the 2026-07-23 test | useful, rights review required |
| 14 | Spotlight current companies/contact pages | Current listing validation/enrichment | Public website | Current pages | Direct org number + LEI; 125 Swedish registry matches in the 2026-07-23 test | useful, rights review required |
| 15 | NGM company page / Data API | Current listing validation/enrichment | Public names page; policy/licensed API for reference data | Current NGM Main + Nordic SME | Prefer FIRDS LEI/ISIN; do not auto-match names alone | useful, rights review required |
| 16 | ESEF filings | Consolidated financials + listed-issuer evidence | Public EU filing data | Annual filings, not a current venue list | LEI → existing GLEIF map; 404 Swedish registry mappings live | **recommended financial complement**, listing validation only |

## Recommended direct URLs

```text
https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/
https://catalog.upphandlingsmyndigheten.se/store/12/resource/239
https://api.ted.europa.eu/v3/notices/search
https://eodhd.com/api/exchange-symbol-list/ST
https://eodhd.com/api/id-mapping
https://registers.esma.europa.eu/publication/searchRegister?core=esma_registers_firds_files
https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files
```

## Local observed files

```text
data_model/bolagsverket_bulkfil.txt
data_model/scb_bulkfil_JE_20260629T055245_80.txt
data_model/01_1.zip
data_model/annual_reports_01_1/
```

## Recommendation

Build the Sweden ingestion from public bulk files:

```text
company bulk ZIPs + annual-report ZIPs -> raw object storage -> parser -> normalized tables -> ClickHouse
```

Do not build the full-universe ingestion around the authenticated API. Working
access is now verified, but the public bulk files remain simpler and better
aligned with batch ingestion. Use the API for bounded targeted refresh,
field-provenance checks, and digital-report discovery/retrieval.
