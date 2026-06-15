# Mexico — Search Attempts

## Attempt 1

- Date/time: 2026-06-16
- Source: datos.gob.mx, PSM, RPC, INEGI DENUE, SAT
- URL: https://datos.gob.mx/ ; https://psm.economia.gob.mx/PSM/ ; https://rpc.economia.gob.mx/ ; INEGI DENUE API ; SAT 69-B
- Language: Spanish
- Why: Map the open-data portal, legal registry, statistical directory, and tax authority.
- Result: datos.gob.mx 308 (redirect); PSM 200; RPC 200; DENUE query API 000 (needs token); SAT 200.
- Decision: Pursue INEGI DENUE bulk + SAT open CSVs; legal registry is per-document.

## Attempt 2

- Date/time: 2026-06-16
- Source: INEGI DENUE bulk (masiva)
- URL: https://www.inegi.org.mx/contenidos/masiva/denue/denue_01_csv.zip
- Language: Spanish
- Why: The national establishment directory is the best open business listing.
- Result: HTTP 200, application/x-zip, 6.8 MB → Aguascalientes 71,871 units, 42 cols. No token for the masiva download.
- Decision: RECOMMENDED. Used as the real sample.

## Attempt 3

- Date/time: 2026-06-16
- Source: SAT Listado 69-B
- URL: http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.csv
- Language: Spanish
- Why: Open tax-side risk list keyed on RFC.
- Result: HTTP 200, 4.5 MB → 14,247 taxpayers (RFC, name, situation).
- Decision: useful_secondary_source (RFC risk overlay).

## Attempt 4

- Date/time: 2026-06-16
- Source: PSM / RPC (legal commercial registry)
- URL: https://psm.economia.gob.mx/PSM/ ; https://rpc.economia.gob.mx/
- Language: Spanish
- Why: The legal-entity register (folio mercantil electrónico).
- Result: publication/notice portal; no open bulk or search API; certified extracts fee-based.
- Decision: blocked_by_payment. Documentation only.

## Attempt 5

- Date/time: 2026-06-16
- Source: BMV / CNBV + datos.gob.mx CKAN
- URL: https://www.bmv.com.mx/ ; https://datos.gob.mx/busca/api/3/action/package_search
- Language: Spanish/English
- Why: Listed financials + check the open-data catalogue API.
- Result: BMV 200 (issuer financials via EMISNET/SITI); datos.gob.mx legacy CKAN API returns HTML (404 for JSON) after portal revamp.
- Decision: BMV/CNBV = listed-only financials (useful_secondary); datos.gob.mx = portal only.
