# Estonia — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Estonia e-Business Register avaandmed ariregister open data download ettevotja JSON CSV majandusaasta aruanne financial statements`
- Language: English + Estonian terms
- Why this query was tried: Confirm the authoritative open-data source and the financial-statement availability.
- Top relevant URLs:
  - https://avaandmed.ariregister.rik.ee/en/downloading-open-data
  - https://www.rik.ee/en/e-business-register/business-register-queries
  - https://www.opensanctions.org/datasets/ee_ariregister/
- Result: e-Business Register (RIK) publishes 8 bulk datasets daily (JSON/XML, some CSV/Parquet) + ~16 XML services. Free since 1 Oct 2022. Depth includes shareholders, beneficial owners, annual reports.
- Decision: Treat avaandmed.ariregister.rik.ee as the single open spine; download CSV + financial datasets.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live, HEAD) — candidate file URLs
- Query: HEAD on yldandmed.json.zip, lihtandmed.csv.zip, kasusaajad.json.zip, financial files
- Result: yldandmed.json.zip → 225 MB (HTTP 200); kasusaajad.json.zip → 27 MB (200); some guessed names → 404.
- Decision: Resolve exact dataset URLs from the official download page, then download.

## Attempt 3
- Date/time: 2026-06-14
- Source: WebFetch + curl (live) — download page
- Query: GET /en/downloading-open-data ; grep all .zip links
- Result: Full URL list obtained: basic (lihtandmed csv/xml), general (yldandmed json/xml), registrikaardid, kaardile_kantud_isikud, osanikud (shareholders), kasusaajad (beneficial owners), kommertspandid, maarused, kandevalised_isikud; financials = 1.aruannete_yldandmed, 2.EMTAK_myygitulu, 3.myygitulu_geograafiline, 4.{2019..2025}_aruannete_elemendid; plus Parquet. License = CC-BY 4.0; daily (reports monthly).
- Decision: Download basic CSV + the three financial layers; confirm BO/shareholders reachable.

## Attempt 4
- Date/time: 2026-06-14
- Source: curl (live) — bulk downloads
- Query: GET lihtandmed.csv.zip; 1.aruannete_yldandmed; 2.EMTAK_myygitulu; 4.2024_aruannete_elemendid
- Result:
  - lihtandmed.csv.zip → 18 MB, 373,025 companies. Clean fields incl. ariregistri_kood, kmkr_nr, status, EHAK.
  - aruannete_yldandmed → 18 MB zip / 228 MB csv: report metadata (year, audited, consolidated, auditor).
  - aruannete_elemendid_2024 → 23 MB zip / 314 MB csv: financial line items (tabel, elemendi_nimetus XBRL-like, vaartus).
  - emtak_myygitulu → 10 MB zip / 57 MB csv: revenue by EMTAK activity.
- Decision: Structured financial open data confirmed. Saved + SHA-256 metadata; built a real normalized record (007 Autohaus osaühing, 11694365, EE101335276).

## Attempt 5
- Date/time: 2026-06-14
- Source: curl (live, HEAD) — beneficial owners + shareholders
- Query: HEAD kasusaajad.json.zip (27 MB), osanikud.json.zip (33 MB)
- Result: both HTTP 200. Estonia keeps beneficial owners + shareholders OPEN as bulk (unusual post-CJEU).
- Decision: Mark both recommended; not downloaded in full (size); reachability confirmed. GDPR note added.
