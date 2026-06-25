# Pakistan Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: SECP, SECP eServices, FBR, PSX, opendata.com.pk, PBS
- Language: English
- Why this query was tried: locate the registrar, tax authority, exchange, open-data portal.
- Top relevant URLs:
  - https://www.secp.gov.pk/ → HTTP 403 (WAF)
  - https://eservices.secp.gov.pk/eServices/ → timeout (firewalled)
  - https://www.fbr.gov.pk/ → 200 ; https://www.psx.com.pk/ → 200 ; opendata.com.pk → 200
- Result: SECP blocked/firewalled; FBR, PSX, opendata reachable.
- Decision: chase the PSX data portal and the FBR ATL.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: PSX + FBR
- Query: PSX listed-companies page; dps.psx.com.pk endpoints; FBR ATL page
- Language: English
- Why this query was tried: find an open listed-companies API and the ATL download.
- Top relevant URLs:
  - https://dps.psx.com.pk/symbols → HTTP 200 JSON (1,068 symbols)
  - https://dps.psx.com.pk/company/OGDC → HTTP 200 HTML (sector, address, free float)
  - https://www.fbr.gov.pk/active-taxpayer-list-income-tax/... → 200 (informational)
- Result: PSX data portal exposes an OPEN JSON symbols API + per-company HTML pages. FBR ATL
  page is informational (per-NTN verification).
- Decision: PSX = recommended (open API). Inspect ATL category page for a bulk file.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: PSX symbols JSON + FBR ATL category page
- Query: parse psx_symbols.json; FBR /categ/active-taxpayer-list-income-tax/...
- Language: English
- Why this query was tried: confirm PSX scope; locate a bulk ATL download.
- Top relevant URLs:
  - psx_symbols.json → 1,068 symbols, 744 equities (symbol, name, sectorName)
  - FBR ATL category page → only sub-category links (71167–71170); no direct .zip/.txt
- Result: PSX symbols confirmed (equities incl. OGDC/HBL/LUCK/ENGRO). No open bulk ATL file
  located — FBR ATL is per-NTN online verification.
- Decision: PSX recommended; SECP blocked_by_authentication (firewalled); FBR ATL
  useful_secondary_source (verification, no open bulk).
