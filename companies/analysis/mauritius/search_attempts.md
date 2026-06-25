# Mauritius Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: CBRD (companies.govmu), CBRIS, open data portal, SEM, MRA, statsmauritius
- Language: English
- Why this query was tried: locate the registry, open-data portal, exchange, tax authority.
- Top relevant URLs:
  - https://companies.govmu.org/ → 301 → /cbrd (HTTP 200)
  - https://onlinecbris.govmu.org/ , https://opendata.govmu.org/ → DNS does not resolve
  - https://www.stockexchangeofmauritius.com/ → 200 ; https://www.mra.mu/ → 200
- Result: CBRD reachable; some subdomains NXDOMAIN; SEM and MRA up.
- Decision: follow CBRD to its search portal; find the open-data host.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: CBRD page + CBRIS portals
- Query: companies.govmu.org/cbrd links; onlinesearch.mns.global; cbris.mns.global
- Language: English
- Why this query was tried: locate the company search and any API.
- Top relevant URLs:
  - https://onlinesearch.mns.global/ → 200 (Angular SPA, "CBRD Online Search")
  - https://cbris.mns.global/cbris → 301 (registration system)
- Result: The online search SPA loads Cloudflare Turnstile (challenges.cloudflare.com/
  turnstile/v0/api.js) → CAPTCHA-gated; guessed API endpoints 404.
- Decision: CBRD CBRIS search = blocked_by_authentication (Turnstile); documents paid.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: data.govmu.org CKAN API
- Query: package_search?q=company ; q=business OR registration OR CBRD
- Language: English
- Why this query was tried: find an open company register/dataset.
- Top relevant URLs:
  - https://data.govmu.org/api/3/action/package_search?q=company → count 3
  - .../?q=business+OR+registration+OR+CBRD → count 0
- Result: Only an open ICT-companies directory (CSV, CC-BY-SA-4.0) + GPS variant + road
  accidents. No full register on the portal. data.govmu.org is the working CKAN host
  (opendata./catalogue. variants NXDOMAIN).
- Decision: ICT CSV = recommended (open, sectoral); portal = useful_secondary_source.

## Attempt 4

- Date/time: 2026-06-25
- Search engine or source: data.govmu.org resource + SEM
- Query: download ict_companies.csv ; SEM listed/issuer pages
- Language: English
- Why this query was tried: confirm the open dataset schema; cover listed companies.
- Top relevant URLs:
  - .../download/ict_companies.csv → HTTP 200 (1,060 rows; cp1252)
  - stockexchangeofmauritius.com → Official Market / DEM issuer pages (published accounts)
- Result: ICT CSV columns = Title, Address, District, Sectors, Other Related Sectors (no
  BRN/identifiers/status). SEM listed pages browser-public; no clean list/API.
- Decision: finalize — open data is sectoral only; CBRD register is Turnstile-gated.
