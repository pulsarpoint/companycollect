# Search attempts — Kosovo

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: GET `arbk.rks-gov.net`, `opendata.rks-gov.net`, `bizneset.rks-gov.net`,
  `atk-ks.org`
- Language: Albanian / Serbian / English
- Why: locate the company register, open-data portal, and tax authority
- Result: ARBK 200; ATK 200; opendata/bizneset did not resolve
- Decision: pursue ARBK + ATK

## Attempt 2
- Date/time: 2026-06-24
- Source: ARBK SPA (`arbk.rks-gov.net`)
- Query: fetch SPA + JS chunks; find API base + endpoints
- Why: determine if there is an open API/bulk
- Result: API base `/api/api/`; endpoints `Services/KerkoBiznesin`,
  `TeDhenatBiznesit`, `EksportoBizneset`, reference/stat services. Search payload
  carries a Turnstile `token`.
- Decision: test the endpoints for open access

## Attempt 3
- Date/time: 2026-06-24
- Source: ARBK API `/api/api/Services/*`
- Query: GET reference endpoints; POST `KerkoBiznesin` without token
- Why: confirm whether any endpoint is open
- Result: **all return HTTP 401** (`application/problem+json`, Unauthorized);
  search is **Cloudflare Turnstile** CAPTCHA-gated. Not bypassed.
- Decision: classify ARBK as blocked_by_authentication; document field model from JS

## Attempt 4
- Date/time: 2026-06-24
- Source: ATK (`atk-ks.org`)
- Query: open-data section; VatRegist app; fiscal-number / inactive-taxpayer pages
- Why: find open company-level data and VAT/fiscal identifiers
- Result: **Open Data** = aggregate XLSX (sector/municipality/year), not
  company-level (verified columns). **VatRegist/SearchTaxPayer** is per-company but
  **CAPTCHA-gated** (`"Kliko 'I'm not a robot'"`). Output fields confirm
  FiscalNo/NrbID/TpStatus/TpName/Address/City/VatNo.
- Decision: ATK Open Data = useful_secondary (aggregate); VatRegist = gated per-company

## Attempt 5
- Date/time: 2026-06-24
- Source: ATK Open Data XLSX (`Nr_punto_ID-2023.xlsx`, `Deklarimi-2022.xlsx`)
- Query: download + inspect headers (zip/sharedStrings)
- Why: check if any open XLSX is company-level
- Result: aggregate dimensions only (TPER_YEAR, PERSHKRIMI_SEKTORIT, KOMUNA,
  TIPI_SUBJEKTIT, NR_PUNDHENSVE, NR_PUNTORVE) — no fiscal number / name
- Decision: confirmed aggregate; not a company register

## Attempt 6
- Date/time: 2026-06-24
- Source: financial data
- Query: public company financial-statements register
- Result: none — Kosovo has no open annual-accounts filing portal for companies;
  only ARBK capital + ATK aggregates
- Decision: record financials as not available openly
