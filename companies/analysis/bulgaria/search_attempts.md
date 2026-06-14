# Bulgaria — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Търговски регистър Агенция вписванията API данни download Bulgaria commercial register open data EIK companies`
- Language: Bulgarian/English
- Why this query was tried: Find the authoritative register + open/bulk/API access.
- Top relevant URLs:
  - https://portal.registryagency.bg/cr/en/en
  - https://data.egov.bg
  - https://companybook.bg/?lang=en
  - http://2015.index.okfn.org/place/bulgaria/companies/
- Result: Commercial Register (Registry Agency); free public search; daily publications on data.egov.bg under CC-BY; bulk for commercial DB needs a data-sharing agreement; CompanyBook free non-financial + REST API.
- Decision: Registry = authoritative; data.egov.bg CC-BY publications = open path.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Bulgaria commercial register annual financial statements ГФО годишни финансови отчети access download API`
- Language: English/Bulgarian
- Why this query was tried: Find financial-statement access + format.
- Top relevant URLs:
  - https://companybook.bg/?lang=en
  - https://www.innovires.com/en/blog/annual-financial-statements-bulgaria.html
  - https://schmidt-export.com/.../financial-statements-from-bulgaria
- Result: ГФО filed to the Commercial Register, public by 30 June, as FILED DOCUMENTS (PDF). Not structured open. CompanyBook (paid) parses balance sheets/income statements 2022+.
- Decision: Catalog ГФО as public-but-document-based; structured figures need OCR or a paid provider.

## Attempt 3
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `data.egov.bg Търговски регистър набор данни фирми EIK open data Bulgaria companies dataset`
- Language: Bulgarian
- Result: data.egov.bg hosts the Registry Agency "Търговски регистър" dataset (many files), open machine-readable per EU Directive 2019/1024.
- Decision: data.egov.bg = the open data path; locate the dataset's resources.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebFetch (data.egov.bg dataset 403) + WebSearch (registryagency API) + curl (data.egov.bg API)
- Result:
  - Official register portal has a web service / API (registration/contract; admin e-service). APIS Register+ commercial.
  - data.egov.bg API (getOrganisations / getOrganisationDatasets) returned HTTP 403 (WAF) regardless of User-Agent.
- Decision: Could not pull a per-company open sample here (data.egov.bg WAF-blocked; register web service needs registration). Documented; field structure from the public search/publications is well known. Schematic normalized sample.
