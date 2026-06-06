# Germany — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `Germany Handelsregister company register bulk download open data`
- Language: English
- Why this query was tried: Find the primary national register and any bulk/open-data path.
- Top relevant URLs:
  - https://offeneregister.de/daten/
  - https://www.opensanctions.org/datasets/de_offeneregister/
  - https://www.handelsregister.de/
  - https://handelsregister.ai/en
- Result: Identified OffeneRegister.de as the main open bulk source; official register has no bulk/API.
- Decision: Deep-dive OffeneRegister; treat handelsregister.de as authoritative-but-not-bulkable.

## Attempt 2
- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `Germany Unternehmensregister API company data download`
- Language: English
- Why this query was tried: Check the central Unternehmensregister for an API/bulk option.
- Top relevant URLs:
  - https://www.unternehmensregister.de/
  - https://handelsregister.ai/en
  - https://openregister.de/en/api
- Result: Unternehmensregister = enrichment portal (financials, shareholders), per-document fee, no bulk/API. Multiple commercial APIs exist.
- Decision: Catalog Unternehmensregister as enrichment; list commercial APIs as paid option.

## Attempt 3
- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `OffeneRegister.de German company data download dataset`
- Language: English
- Why this query was tried: Locate exact OffeneRegister bulk files, formats, sizes, license.
- Top relevant URLs:
  - https://offeneregister.de/daten/
  - https://db.offeneregister.de/
  - https://blog.opencorporates.com/2019/02/06/german-company-data-now-available-for-download-via-open-knowledge-deutschland/
- Result: Confirmed ~5M companies, JSONL + SQLite, CC-BY 4.0, torrent available, ~255 MB compressed/~4 GB uncompressed.
- Decision: Find direct file URLs and download the JSONL bulk.

## Attempt 4
- Date/time: 2026-06-06
- Search engine or source: WebFetch (offeneregister.de/daten/) + WebSearch (GovData) + WebSearch (Registerbekanntmachungen XML)
- Query: README extraction + `GovData.de Unternehmen ... Handelsregister` + `handelsregister.de XML download Registerbekanntmachungen open data 2025`
- Language: English/German
- Why this query was tried: Get field list/license from README; check national open-data portal; check for any official XML/open-data feed.
- Top relevant URLs:
  - https://offeneregister.de/daten/
  - https://www.govdata.de/
  - https://discourse.opencode.de/t/opendata-zu-registerbekanntmachungen/4517
- Result: README gives OpenCorporates schema + CC-BY 4.0. GovData does not host the register as a master file. Registerbekanntmachungen only via community archive (coezbek GitHub), not official bulk.
- Decision: Confirmed no official bulk; OffeneRegister is the bulk source.

## Attempt 5
- Date/time: 2026-06-06
- Search engine or source: WebFetch (opencode discourse, db.offeneregister.de, OpenSanctions) + WebSearch (official XML 2024)
- Query: Official open-data/XML for Registerbekanntmachungen.
- Language: German/English
- Why this query was tried: Last check for any official structured/bulk feed.
- Top relevant URLs:
  - https://www.opensanctions.org/datasets/de_offeneregister/
  - https://github.com/bundesAPI/handelsregister
  - https://www.handelsregister.de/rp_web/bekanntmachungen.xhtml
- Result: OpenSanctions provides FTM mirror (13M entities, CC-BY-NC). bundesAPI = scraper (≤60/hr). db.offeneregister.de returned HTTP 502.
- Decision: Catalog OpenSanctions + bundesAPI; rely on daten.offeneregister.de directory.

## Attempt 6 (direct probing, not a search engine)
- Date/time: 2026-06-06
- Source: curl HEAD/GET against candidate file URLs
- Queries: probed several guessed URLs; discovered live directory listing at `https://daten.offeneregister.de/`
- Result: Found exact files:
  - `de_companies_ocdata.jsonl.bz2` (260,455,433 bytes, last-modified 2019-02-05) — HTTP 200
  - `handelsregister.db` (3,718,012,928 bytes, 2022-10-21)
  - `openregister.db.gz` (773,380,427 bytes, 2019)
  - `.torrent` files
- Decision: Downloaded the JSONL bulk; streamed a sample first to confirm schema.
