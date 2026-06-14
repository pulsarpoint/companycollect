# Cyprus — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Cyprus Registrar of Companies open data data.gov.cy registered companies CSV download HE number bulk`
- Language: English
- Why this query was tried: Find the authoritative register + any open bulk/CSV.
- Top relevant URLs:
  - https://www.companies.gov.cy/en/
  - https://www.data.gov.cy/en/group/30
  - https://opencorporates.com/registers/58
  - https://www.opensanctions.org/datasets/cy_companies/
- Result: DRCIP = authoritative; free eSearch; DRCIP has an open-data group (#30) on data.gov.cy.
- Decision: Confirm the open CSV + its fields via OpenSanctions; treat DRCIP CSV as the open spine.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Cyprus company register annual return financial statements HE32 access download Registrar Companies efiling fees`
- Language: English
- Why this query was tried: Find financial-statement access + format + fees.
- Top relevant URLs:
  - https://www.companies.gov.cy/en/company-lifecycle/search-for-company-information
  - https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchForm.aspx
- Result: HE32 annual return filed WITH audited financial statements; scanned annual returns + financial
  statements available via a DETAILED SEARCH costing EUR 10 (scanned PDFs). Basic eSearch free.
- Decision: Financials = public but PAID (EUR 10) + document-based (PDF); not structured open.

## Attempt 3
- Date/time: 2026-06-14
- Source: WebFetch (data.gov.cy group page; OpenSanctions cy_companies) + curl (data.gov.cy CKAN API)
- Result:
  - data.gov.cy /api/3/action/* returned HTTP 404 (non-standard CKAN path); group page JS-rendered (no resource list).
  - OpenSanctions cy_companies: sources from DRCIP open data on data.gov.cy in CSV; ~567,536 companies,
    ~2.75M entities; NAMES OFFICERS (not shareholders). Confirms an open company master + officers as CSV.
- Decision: Open CSV confirmed (companies + officers). Could not resolve the exact CSV resource URL here -
  resolve via data.gov.cy/en/group/30. Documented; schematic normalized sample.
