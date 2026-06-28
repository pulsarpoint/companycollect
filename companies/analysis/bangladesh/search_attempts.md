# Bangladesh Search Attempts

## Attempt 1

- Date/time: 2026-06-28
- Search engine or source: direct HTTP probes (official hosts)
- Query: RJSC (roc.gov.bd), eservices.roc.gov.bd, NBR, DSE, data.gov.bd, CSE
- Language: English
- Why this query was tried: locate the registrar, tax authority, exchanges, open-data portal.
- Top relevant URLs:
  - https://www.roc.gov.bd/ → SSL cert issue ; https://eservices.roc.gov.bd/ → NXDOMAIN
  - https://nbr.gov.bd/ → 200 ; https://www.dsebd.org/ → 200 ; https://data.gov.bd/ → 200 ; https://www.cse.com.bd/ → 200
- Result: RJSC cert issue / eservices unresolved; NBR, DSE, data.gov.bd, CSE reachable.
- Decision: check data.gov.bd for a register; parse DSE; confirm RJSC access.

## Attempt 2

- Date/time: 2026-06-28
- Search engine or source: RJSC (-k) + data.gov.bd + DSE
- Query: roc.gov.bd (-k); data.gov.bd home; dsebd.org/company_listing.php
- Language: English
- Why this query was tried: find the register search, an open dataset, and the listed companies.
- Top relevant URLs:
  - https://www.roc.gov.bd/ (-k) → 301 ; data.gov.bd → DKAN (statistical datasets)
  - https://www.dsebd.org/company_listing.php → 200 (~429 KB; real company names + displayCompany links)
- Result: RJSC home redirects (eservices/docs gated); data.gov.bd is DKAN statistical; DSE listing is rich.
- Decision: DSE = primary open source; inspect a DSE company detail page; search data.gov.bd DKAN.

## Attempt 3

- Date/time: 2026-06-28
- Search engine or source: DSE detail + data.gov.bd DKAN API
- Query: displayCompany.php?name=<CODE>; data.gov.bd/api/3/action/package_search?q=company
- Language: English
- Why this query was tried: confirm DSE per-company fields; check for a register dataset.
- Top relevant URLs:
  - https://www.dsebd.org/displayCompany.php?name=1JANATAMF → 200 (Trading Code, Scrip Code,
    Sector, Authorized Capital (mn), Paid-up Capital (mn), Listing Year, Market Category, Type)
  - data.gov.bd/api/3/action/package_search → 301 redirect (no clean CKAN/DKAN API; no register)
- Result: DSE detail pages are rich + parseable; data.gov.bd has no company register.
- Decision: DSE = recommended; data.gov.bd = not_company_data.

## Attempt 4

- Date/time: 2026-06-28
- Search engine or source: DSE listing parse
- Query: extract trading-code + name pairs from company_listing.php
- Language: English
- Why this query was tried: confirm coverage and build a real normalized sample.
- Top relevant URLs:
  - dsebd.org/company_listing.php → 637 (code, name) pairs parsed (e.g. AAMRANET, ACMELAB)
- Result: ~640 listed instruments; clean code→name structure (`<a ...name=CODE>CODE</a> (Name)`).
- Decision: finalize — DSE open listed source (recommended); RJSC blocked_by_payment; NBR/CSE
  useful_secondary_source.
