# Ukraine — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `ЄДР Єдиний державний реєстр юридичних осіб data.gov.ua open data download XML EDRPOU bulk`
- Result: EDR published as separate XML files on data.gov.ua (Ministry of Justice); searchable by EDRPOU.
- Decision: query data.gov.ua CKAN for the dataset resources.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Ukraine company financial statements open data SMIDA stockmarket.gov.ua issuer reporting XBRL`
- Result: NSSMC/SMIDA (stockmarket.gov.ua) issuer disclosure; XBRL mandatory for IFRS reporters via a single portal, open + integrated to XBRL International.
- Decision: catalog NSSMC/SMIDA + XBRL FRS as financial sources.

## Attempt 3
- Date/time: 2026-06-15
- Source: curl (data.gov.ua CKAN package_show)
- Result: dataset `a1799820-…`, **CC-BY**, weekly. Resources: UO.zip (legal entities), FOP.zip (entrepreneurs), FSU.zip, + 3 schema zips.
- Decision: HEAD UO.zip; download schema.

## Attempt 4
- Date/time: 2026-06-15
- Source: curl
- Query: HEAD UO.zip; download UO_schema.zip
- Result: UO.zip = 325 MB (application/octet-stream). XSD elements: NAME, SHORT_NAME, OPF, EDRPOU, STAN, FOUNDERS, BENEFICIARIES, SUPERIOR_MANAGEMENT, SIGNERS, AUTHORIZED_CAPITAL, REGISTRATION, BRANCHES, TERMINATION_*, PREDECESSORS, ASSIGNEES, EXCHANGE_DATA/TAX_PAYER_TYPE. **No ADDRESS / KVED** elements.
- Decision: download UO.zip; inspect real records.

## Attempt 5
- Date/time: 2026-06-15
- Source: curl + python (zipfile)
- Query: download UO.zip (325 MB → 3.1 GB UO.xml); stream first records
- Result: real `<SUBJECT>` records (windows-1251) — EDRPOU, OPF, STAN, FOUNDERS (with names+share), BENEFICIARIES, SIGNERS (name+role керівник), AUTHORIZED_CAPITAL, REGISTRATION, TAX_PAYER_TYPE. **No address/KVED** confirmed (wartime reduction). **2,008,750** legal-entity records counted.
- Decision: EDR UO = recommended; build redacted sample (PII present).
