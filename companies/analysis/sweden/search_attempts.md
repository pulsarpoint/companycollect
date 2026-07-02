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
