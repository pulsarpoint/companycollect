# Kazakhstan Search Attempts

## Attempt 1

- Date/time: 2026-06-26
- Search engine or source: direct HTTP probes (official hosts)
- Query: data.egov.kz, stat.gov.kz, KGD, KASE, egov.kz, AIX
- Language: Russian/English
- Why this query was tried: locate the open-data portal, statistics, tax authority, exchange.
- Top relevant URLs:
  - https://data.egov.kz/ → HTTP 200 (open-data portal)
  - https://stat.gov.kz/ → 200 ; https://kgd.gov.kz/ → 301 → /kk ; https://kase.kz/en/ → 200
  - https://www.aix.kz/ → 503
- Result: data.egov.kz, stat, KGD, KASE reachable; AIX down.
- Decision: search data.egov.kz for the legal-entities register.

## Attempt 2

- Date/time: 2026-06-26
- Search engine or source: data.egov.kz portal + search
- Query: home dataset links; datasets/search?text=юридические лица
- Language: Russian
- Why this query was tried: find the company/legal-entities dataset.
- Top relevant URLs:
  - /datasets/view?index=gbd_ul (ГБД ЮЛ — State Database of Legal Entities)
  - search result described gbd_ul: "регистрационные данные юридических лиц … БИН … адрес … вид деятельности … ФИО руководителя"
- Result: `gbd_ul` is the open legal-entities register (BIN, name, reg date, address, OKED,
  director).
- Decision: inspect gbd_ul and test its API.

## Attempt 3

- Date/time: 2026-06-26
- Search engine or source: data.egov.kz API
- Query: /api/v4/gbd_ul/v21 ; /api/detailed/gbd_ul ; /api/v4/gbd_ul/v21/xml
- Language: n/a
- Why this query was tried: pull register data and confirm access.
- Top relevant URLs:
  - https://data.egov.kz/api/v4/gbd_ul/v21 → HTTP 403 {"error":"API key is required"}
  - the dataset page shows the API template ...?apiKey=yourApiKey
- Result: the data.egov.kz API requires a FREE API key (registration); no key-less bulk found.
- Decision: gbd_ul = recommended (open register, free-key-gated, requires_authentication).

## Attempt 4

- Date/time: 2026-06-26
- Search engine or source: KGD + KASE
- Query: kgd.gov.kz/ru taxpayer services; kase.kz/en/shares, /en/issuers
- Language: Russian/English
- Why this query was tried: cover tax/VAT status and listed companies.
- Top relevant URLs:
  - https://kgd.gov.kz/ru → 200 (taxpayer search services / published lists)
  - https://kase.kz/en/shares → 301 ; /en/issuers → 301 (SPA / redirect)
- Result: KGD has browser-public taxpayer search/lists (by BIN/IIN); KASE listing pages
  redirect (SPA), no clean static list/API.
- Decision: KGD and KASE = useful_secondary_source. Finalize — gbd_ul is the open register.
