# Uzbekistan Search Attempts

## Attempt 1

- Date/time: 2026-06-26
- Search engine or source: direct HTTP probes (official hosts)
- Query: data.gov.uz, data.egov.uz, soliq.uz, stat.uz, uzse.uz, my.gov.uz
- Language: Uzbek/Russian/English
- Why this query was tried: locate the open-data portal, tax committee, statistics, exchange.
- Top relevant URLs:
  - https://data.gov.uz/ → timeout ; https://data.egov.uz/ → connection refused
  - https://soliq.uz/ → timeout
  - https://stat.uz/ → 301 → /uz (200) ; https://uzse.uz/ → 200 ; https://my.gov.uz/ → 302 → /uz (200)
- Result: data.gov.uz / data.egov.uz / soliq.uz firewalled; stat.uz, uzse.uz, my.gov.uz reachable.
- Decision: use stat.uz + uzse as reachable entry points; document the firewalled register.

## Attempt 2

- Date/time: 2026-06-26
- Search engine or source: stat.uz + uzse + data.gov.uz retry
- Query: stat.uz register/open-data links; uzse /issuers; http://data.gov.uz
- Language: Uzbek/Russian
- Why this query was tried: find a reachable register and the exchange's issuers list.
- Top relevant URLs:
  - stat.uz links out to //data.egov.uz/ (firewalled) and an ODI dataset cert (theodi.org/datasets/221261)
  - https://uzse.uz/issuers → 302 ; http://data.gov.uz → timeout
- Result: stat.uz is the EGRPO custodian but points to the firewalled portal; uzse /issuers redirects.
- Decision: chase the uzse issuers page + API.

## Attempt 3

- Date/time: 2026-06-26
- Search engine or source: uzse REST API guesses
- Query: uzse.uz/api/issuers, /api/v1/issuers, /api/listing, /api/securities, /api/executions/top10_listing
- Language: n/a
- Why this query was tried: find the SPA's data endpoint for listed issuers.
- Top relevant URLs:
  - https://uzse.uz/api/issuers → HTTP 404 JSON {"status":404,"error":"Not Found"} (REST backend exists)
  - all guessed /api/... paths → JSON 404
- Result: a REST backend exists but the correct issuers route was not located by guessing.
- Decision: uzse = useful_secondary_source (browser-public SPA; API route unknown).

## Attempt 4

- Date/time: 2026-06-26
- Search engine or source: uzse /issuers/ (followed) + stat register + data.egov retry
- Query: uzse.uz/issuers/ ; stat.uz/.../business-registers ; http://data.egov.uz
- Language: Uzbek/Russian
- Why this query was tried: confirm the issuers content and re-check reachability.
- Top relevant URLs:
  - https://uzse.uz/issuers/ → 200 (~11 KB SPA shell; no issuer names server-side)
  - stat business-registers → 404 ; http://data.egov.uz → connection refused
- Result: uzse issuers list is client-side rendered; data.egov.uz firewalled (refused).
- Decision: finalize — EGRPO/soliq firewalled (unavailable here); uzse/stat reachable secondaries.
