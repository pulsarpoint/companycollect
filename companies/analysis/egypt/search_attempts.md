# Search attempts — Egypt

## Attempt 1
- Date/time: 2026-06-25
- Source: direct probe of candidate official hosts
- Query: GET `gafi.gov.eg`, `erp.gafi.gov.eg`, `investinegypt.gov.eg`, `egx.com.eg`,
  `eta.gov.eg`, `capmas.gov.eg`
- Language: Arabic, English
- Result: gafi 302→200; erp.gafi 000; investinegypt 302; egx 200; eta 301; capmas 200
- Decision: pursue GAFI, EGX; check open-data portal

## Attempt 2
- Date/time: 2026-06-25
- Source: GAFI home + eServices
- Query: parse links (search/registry/services)
- Result: GAFI runs login-gated investor eServices (registration/incorporation by
  department); no public company search/register
- Decision: GAFI = gated; pursue EGX + open data

## Attempt 3
- Date/time: 2026-06-25
- Source: EGX (`egx.com.eg`)
- Query: ListedStocks.aspx, ListedCompanies.aspx, companiesprofilesearch.aspx,
  getinformation.aspx
- Result: ListedStocks (44 KB) + companiesprofilesearch (52 KB) load (JS-rendered);
  getinformation.aspx?type=… returned **"Request Rejected" (WAF)** / dropped
- Decision: EGX = public via browser, WAF-gated for automation

## Attempt 4
- Date/time: 2026-06-25
- Source: Egypt open-data portal
- Query: `data.gov.eg`, `egypt.gov.eg`, `enow.gov.eg`
- Result: data.gov.eg / egypt.gov.eg unreachable (000); enow 307
- Decision: no working open-data portal for company data

## Attempt 5
- Date/time: 2026-06-25
- Source: ETA (tax)
- Query: `eta.gov.eg`, `invoicing.eta.gov.eg`
- Result: e-invoicing portal up; Tax ID (الرقم الضريبي) administered there;
  per-company, not open bulk
- Decision: ETA = tax identifier source (per company)

## Attempt 6
- Date/time: 2026-06-25
- Source: identifiers
- Query: Commercial Registry number, Tax ID, Unified company number
- Result: Commercial Registry number (السجل التجاري), Tax ID (9-digit), Unified
  company number; EGX symbol/ISIN
- Decision: document identifier model
