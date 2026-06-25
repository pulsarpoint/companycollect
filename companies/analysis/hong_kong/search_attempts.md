# Hong Kong Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: Companies Registry, ICRIS e-Search, data.gov.hk, HKEX
- Language: English
- Why this query was tried: locate the official registry, its e-Search portal, the open-data
  portal, and the exchange.
- Top relevant URLs:
  - https://www.cr.gov.hk/en/home/index.htm → HTTP 200
  - https://www.e-services.cr.gov.hk/ → 307 → /ICRIS3EP/
  - https://data.gov.hk/en/ → HTTP 200
  - https://www.hkex.com.hk/?sc_lang=en → HTTP 200
- Result: All reachable; ICRIS3EP is the e-Search portal.
- Decision: query the data.gov.hk CKAN API for company datasets.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: data.gov.hk CKAN API
- Query: package_search?q=company&rows=20
- Language: English
- Why this query was tried: find official open company datasets.
- Top relevant URLs:
  - https://data.gov.hk/en-data/api/3/action/package_search?q=company → 12 datasets
- Result: The only company-register dataset is the Companies Registry "List of Newly
  Incorporated / Registered / Re-domiciled Companies" (hk-cr-crdata-list-newly-registered-companies-2526);
  the rest are statistics.
- Decision: inspect the CR dataset resources.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: data.gov.hk CKAN (dataset resources) + cr.gov.hk
- Query: package resources; download RNC063L / RNC063F CSV
- Language: English
- Why this query was tried: confirm the open CR feed schema and access.
- Top relevant URLs:
  - https://www.cr.gov.hk/docs/wrpt/RNC063/RNC063L_20241230.csv → HTTP 200 (3,286 rows)
  - https://www.cr.gov.hk/docs/wrpt/RNC063/RNC063F_20241230.csv → HTTP 200
- Result: Weekly CSVs. RNC063L (local): English/Chinese name, BR Number, Date of
  Incorporation, Date of Change of name. RNC063F (non-HK): corporate name, approved HK name,
  BR Number, Date of Registration. No personal data; identifier = BR Number.
- Decision: CR open data = recommended (incremental feed).

## Attempt 4

- Date/time: 2026-06-25
- Search engine or source: HKEX + ICRIS
- Query: ListOfSecurities.xlsx; ICRIS3EP landing
- Language: English
- Why this query was tried: cover listed companies and the authoritative full register.
- Top relevant URLs:
  - https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx → HTTP 200 but TEMPLATE skeleton
  - https://www.e-services.cr.gov.hk/ICRIS3EP/ → 303 (interactive session)
- Result: The HKEX static xlsx is a template (placeholders, dimension A1:R8) — populated
  server-side. ICRIS is interactive and document/particulars search is pay-per-use.
- Decision: HKEX = useful_secondary_source (browser-public); ICRIS = blocked_by_payment
  (authoritative full register, not open bulk).
