# New Zealand — Search Attempts

## Attempt 1

- Date/time: 2026-06-15
- Source: NZ Companies Register + NZBN register
- URL: https://companies-register.companiesoffice.govt.nz/ ; https://www.nzbn.govt.nz/
- Language: English
- Why: The Companies Office runs both; NZBN is the universal business identifier.
- Result: both HTTP 200 (public search sites).
- Decision: Pursue the NZBN API (machine-readable) + Companies Register documents.

## Attempt 2

- Date/time: 2026-06-15
- Source: NZBN API gateway
- URL: https://api.business.govt.nz/gateway/nzbn/v5/entities?search-term=air new zealand
- Language: English
- Why: Authoritative entity data API.
- Result: HTTP **401** `{"message":"Access denied due to missing subscription key…"}`.
- Decision: blocked_by_authentication (free subscription key). Catalog from public docs.

## Attempt 3

- Date/time: 2026-06-15
- Source: NZBN public website search backend
- URL: https://www.nzbn.govt.nz/mynzbn/search/ (+ candidate JSON endpoints)
- Language: English
- Why: Look for an unauthenticated public search API.
- Result: search page routes through OAuth-gated api.business.govt.nz (api.business.govt.nz/oauth2). No open JSON endpoint.
- Decision: Same gated API; no open bulk.

## Attempt 4

- Date/time: 2026-06-15
- Source: Disclose Register (FMA) + Companies Register help centre
- URL: https://disclose-register.companiesoffice.govt.nz/ ; companies-register …/help-centre/
- Language: English
- Why: Find the financial-statements route and any bulk/API.
- Result: Disclose = FMC offers / managed investment schemes (FMC Act 2013), public document search. Companies Register help mentions **no** API/bulk/extract.
- Decision: Financials = FMC-reporting subset only (public documents). No free bulk.

## Attempt 5

- Date/time: 2026-06-15
- Source: data.govt.nz catalogue (CKAN)
- URL: https://catalogue.data.govt.nz/api/3/action/package_search?q=companies register
- Language: English
- Why: Check for an open company dataset.
- Result: bot-protected (Imperva "Pardon Our Interruption"); not usable here. data.govt.nz does not host the register openly.
- Decision: not_company_data; rely on the NZBN API.
