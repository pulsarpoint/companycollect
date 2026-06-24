# Search attempts — Montenegro

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: GET `pretraga.crps.me`, `crps.gov.me`, `eprijava.tax.gov.me`,
  `data.gov.me`, gov.me Tax Administration
- Language: Montenegrin (BHS), English
- Result: crps.gov.me/pretraga.crps.me (https) did not resolve; eprijava 503;
  data.gov.me 200; gov.me 301→200
- Decision: locate the current CRPS portal; explore data.gov.me

## Attempt 2
- Date/time: 2026-06-24
- Source: `http://www.pretraga.crps.me/`
- Query: GET the legacy CRPS portal
- Result: serves a **domain-parking page** (`mydomaincontact.com/?domain_name=crps.me`)
  — the legacy domain has lapsed
- Decision: find the current portal under tax.gov.me

## Attempt 3
- Date/time: 2026-06-24
- Source: gov.me Tax Administration page (`gov.me/poreskauprava`)
- Query: find CRPS / registry search links
- Result: confirms CRPS under the tax administration (`crps@tax.gov.me`); the
  e-services portal is **`eprijava.tax.gov.me/TaxisPortal`**
- Decision: probe TaxisPortal

## Attempt 4
- Date/time: 2026-06-24
- Source: `eprijava.tax.gov.me/TaxisPortal` (and root)
- Query: GET with browser UA
- Result: **HTTP 503 "Service Unavailable"** consistently (IIS app down)
- Decision: CRPS portal currently unavailable; document identifier model, mark blocked

## Attempt 5
- Date/time: 2026-06-24
- Source: `data.gov.me` CKAN API (`/api/3/action/package_search`)
- Query: `privredna`, `preduzeca`, `kompanije`, `registar`, `biznis`
- Result: working CKAN, but **no company register** — only statistics, niche
  registers, and a **"Javna preduzeća"** (public-enterprises) XLSX
- Decision: data.gov.me = useful_secondary (public-enterprises only)

## Attempt 6
- Date/time: 2026-06-24
- Source: `data.gov.me` Javna preduzeća XLSX
- Query: download + inspect (zip/sharedStrings)
- Result: real public/state enterprises with name/status/type/founder/address/website
  (Investiciono-razvojni fond A.D., CGES AD, Luka Bar AD, Pošta CG AD, Plantaže AD…)
- Decision: use for sample anchors; note PIB/reg number not present (held by CRPS)

## Attempt 7
- Date/time: 2026-06-24
- Source: financial data + MONSTAT
- Query: open financial-statements dataset; statistical business register
- Result: none open — financials filed at CRPS (not published); MONSTAT is aggregate
- Decision: record financials as not available openly
