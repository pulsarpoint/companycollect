# Search attempts — United States

## Attempt 1

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `SEC EDGAR company facts bulk data download API`
- Language: English
- Why this query was tried: Locate the primary federal open dataset for US public companies.
- Top relevant URLs:
  - https://www.sec.gov/search-filings/edgar-application-programming-interfaces
  - https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip
  - https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip
  - https://www.sec.gov/files/company_tickers.json
- Result: Found official bulk files + REST APIs; rate limit 10 req/s/IP; User-Agent w/ email required.
- Decision: Mark SEC EDGAR `recommended`; download `company_tickers.json`.

## Attempt 2

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `SAM.gov entity registration bulk data download API public`
- Language: English
- Why this query was tried: Find federal entity/contractor registry with bulk access.
- Top relevant URLs:
  - https://open.gsa.gov/api/entity-api/
  - https://open.gsa.gov/api/sam-entity-extracts-api/
  - https://sam.gov/data-services/Entity%20Registration?privacy=Public
- Result: Public FOIA extract API (JSON/CSV), requires free account + API key; max 1M records/extract.
- Decision: Mark `recommended` (with auth caveat). No download (needs key).

## Attempt 3

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `USA business registry bulk download open data companies state secretary of state`
- Language: English
- Why this query was tried: Understand the state-level registry landscape.
- Top relevant URLs:
  - https://blog.opencorporates.com/2025/09/15/sourcing-data-directly-from-us-state-registries/
  - https://www.nass.org/ (National Association of Secretaries of State)
- Result: Confirmed no national register; 50+ state registries; bulk often paid.
- Decision: Mark state registries `useful_secondary_source`; identify free states next.

## Attempt 4

- Date/time: 2026-06-06
- Search engine or source: WebFetch (SEC) + WebSearch
- Query: SEC EDGAR API page fetch; `IRS exempt organizations business master file bulk download data.gov nonprofit`
- Language: English
- Why this query was tried: Confirm SEC bulk details; find IRS nonprofit national dataset.
- Top relevant URLs:
  - https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
  - https://www.irs.gov/pub/irs-soi/eo1.csv … eo4.csv
- Result: SEC page returned HTTP 403 to WebFetch (UA enforcement); IRS EO BMF regional CSVs found, monthly updates.
- Decision: Mark IRS EO BMF `recommended`; download a header sample via curl with proper UA.

## Attempt 5

- Date/time: 2026-06-06
- Search engine or source: WebSearch + WebFetch
- Query: `free US state business entity bulk data download CSV ...`; `Colorado secretary of state business entities bulk data download data.colorado.gov`; IRS EO BMF page fetch
- Language: English
- Why this query was tried: Identify a concrete FREE state registry exemplar and exact IRS file URLs.
- Top relevant URLs:
  - https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h
  - https://www.irs.gov/pub/irs-pdf/p5926.pdf (EO BMF data dictionary)
- Result: Colorado offers free open data (Socrata) 1M+ entities; got exact IRS eo1-4.csv URLs.
- Decision: Mark Colorado `recommended`; download Colorado + IRS samples via curl.

## Downloads performed

- `raw/bulk/sec_company_tickers.json` — HTTP 200, 795,549 bytes, 10,405 records.
- `raw/api/colorado_business_entities_sample.json` — HTTP 200, 3,123 bytes, 5 records (Socrata `$limit=5`).
- `raw/samples/irs_eo_bmf_region1_head.csv` — HTTP 206 (range), 4,001 bytes, header + sample rows only.
