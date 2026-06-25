# Armenia Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: e-register.am, data.gov.am, AMX, SRC, petakamutner
- Language: English / Armenian
- Why this query was tried: locate the registry, open-data portal, exchange, tax authority.
- Top relevant URLs:
  - https://www.e-register.am/ → 301 → e-register.moj.am ; /en/ → redirect to validate.perfdrive.com
  - https://data.gov.am/ → NXDOMAIN ; https://www.petakamutner.am/ → NXDOMAIN
  - https://amx.am/en → 200 ; https://www.src.am/ → 302 → /am
- Result: State Register reachable but bot-redirected; data.gov.am/petakamutner do not resolve;
  AMX and SRC up.
- Decision: confirm the State Register bot protection; check open-data hosts and AMX/SRC.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes
- Query: e-register.moj.am, e-register.am/en/companies, opendata.am, data.opendata.am, armstat
- Language: English/Armenian
- Why this query was tried: find the register search and an open-data portal.
- Top relevant URLs:
  - https://www.e-register.am/en/companies → redirect to validate.perfdrive.com (Radware bot)
  - https://data.opendata.am/ → 200 (CKAN) ; https://opendata.am/ → 200
  - https://www.src.am/en → 200 (1.8 MB; TIN/search/taxpayer mentions)
- Result: State Register confirmed Radware Bot Manager-protected; data.opendata.am runs CKAN;
  SRC has a taxpayer search.
- Decision: query the CKAN portal for a company register; inspect SRC search.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: data.opendata.am CKAN API
- Query: package_search q=company / "legal entities" / register / juridical / petakan
- Language: English/Armenian
- Why this query was tried: find an open company-register dataset.
- Top relevant URLs:
  - .../package_search?q=company → 10 datasets (all research/survey/sectoral)
  - q="legal entities" → 0 ; q=juridical → 0 ; q=petakan → 0 ; q=register → license register etc.
- Result: No company-register dataset on the civic portal (research/sectoral only).
- Decision: Open Data Armenia = not_company_data.

## Attempt 4

- Date/time: 2026-06-25
- Search engine or source: SRC + AMX
- Query: SRC /en/search, /searchTaxpayerData; AMX issuers/instruments + api.amx.am
- Language: English
- Why this query was tried: cover tax identity and listed companies.
- Top relevant URLs:
  - https://www.src.am/en/search → 200 (878 KB search interface; /searchTaxpayerData endpoint)
  - https://amx.am/en/issuers → 3 KB SPA shell ; api.amx.am/api/v1/instruments → 404
- Result: SRC has a browser-public per-TIN taxpayer search; AMX is a JS SPA with no clean
  public API found.
- Decision: SRC = useful_secondary_source (per-TIN); AMX = useful_secondary_source (SPA).
  Finalize — Armenia is gated/lookup from this environment.
