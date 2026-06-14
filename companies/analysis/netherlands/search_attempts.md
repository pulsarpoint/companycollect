# Netherlands — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Netherlands KvK Handelsregister API open data data.overheid.nl company register bulk download jaarrekening annual accounts XBRL financial statements UBO`
- Language: English + Dutch
- Why this query was tried: Identify the register, any open bulk/API, and financials.
- Top relevant URLs:
  - https://developers.kvk.nl/
  - https://www.kvk.nl/producten-bestellen/kvk-handelsregister-open-data-set/
  - https://data.overheid.nl/
- Result: KvK has paid APIs (Zoeken/Basisprofiel/...) + a free anonymised Open Data Set (location, registration date, SBI, dissolution); financials via XBRL; UBO restricted post-CJEU.
- Decision: Find the open datasets on data.overheid.nl and download.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — data.overheid.nl CKAN
- Query: package_search?q=KvK / handelsregister
- Result: kvk-handelsregister-open-dataset-basis-bedrijfsgegevens (CC-BY 4.0), kvk-handelsregister-open-dataset-jaarrekeningen (CC-BY 4.0), + variants.
- Decision: package_show both; get the resource URLs (kvk.nl downloads + opendata.kvk.nl HVDS APIs).

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — HVDS API probe
- Query: GET opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer/{nr}
- Result: live but HTTP 429 (rate-limited) without an API key. Developer docs confirm a free API key is required.
- Decision: Use the bulk downloads instead for samples; document the API as free-with-key.

## Attempt 4
- Date/time: 2026-06-14
- Source: curl (live) — bulk downloads
- Query: kvk-open-dataset-basis-bedrijfsgegevens.zip; kvk-open-data-set-jaarrekeningen0.zip
- Result:
  - basis CSV → 12.7 MB zip / 95 MB / 1,891,639 records (registration date, legal form, postcode region, SBI, active/insolvency) - ANONYMISED (no KvK number/name).
  - jaarrekeningen ZIP 0 → 200 MB; individual XBRL-derived XML reports with balance-sheet figures (Assets/Equity/Liabilities/Provisions/share capital) + year - ANONYMISED (no identifier). Split into ZIPs 0..5+.
- Decision: Both open (CC-BY 4.0) but anonymised. Saved + SHA-256 metadata; built a real normalized sample. Identified data = paid KvK API / free HVDS API by KvK number; commercial providers for bulk identified.
