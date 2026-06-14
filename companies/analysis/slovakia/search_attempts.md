# Slovakia — Search Attempts

## Attempt 1

- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Slovakia RPO register pravnickych osob api.statistics.sk API JSON IČO open data`
- Language: Slovak/English
- Why: locate the official legal-entities register API.
- Top relevant URLs: slovak.statistics.sk (RPO REST API), susrrpo.docs.apiary.io, data.gov.sk.
- Result: RPO production API `https://api.statistics.sk/rpo/v1/`, CC-BY 4.0; searchable by IČO/name; V2 exists.
- Decision: test live.

## Attempt 2

- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Slovakia registeruz.sk register uctovnych zavierok API financial statements open data cruz-public`
- Language: Slovak/English
- Why: find the official financial-statements source.
- Top relevant URLs: registeruz.sk/cruz-public/home/api, utopia.sk OpenData wiki.
- Result: RÚZ Open API with accounting units + full financial reports (balance sheet/income statement); JSON; pagination.
- Decision: fetch the API doc and test the full chain.

## Attempt 3

- Date/time: 2026-06-14
- Search engine or source: WebFetch (registeruz.sk/cruz-public/home/api)
- Why: extract exact endpoints, parameters, fields, license.
- Result: endpoints uctovne-jednotky / uctovna-jednotka / uctovna-zavierka / uctovny-vykaz / sablona / classifiers; pagination `pokracovat-za-id`, `max-zaznamov` (≤10000), `zmenene-od`; **license CC0**.
- Decision: walk the chain for a real company.

## Attempt 4

- Date/time: 2026-06-14
- Search engine or source: curl (RÚZ)
- Query: `uctovne-jednotky?ico=31333532` → `uctovna-jednotka?id=154048` → `uctovna-zavierka?id=6500234` → `uctovny-vykaz`
- Result: ESET s.r.o. — master data (DIČ 2020317068, SK NACE 62090, founded 1992-09-17), 26 statements, 2024 statement with 3 report ids. ESET's reports had **empty obsah** (PDF only — large filer).
- Decision: find a company filing structured tables.

## Attempt 5

- Date/time: 2026-06-14
- Search engine or source: curl (RÚZ)
- Query: `uctovne-vykazy?zmenene-od=2026-05-01` → fetch each `uctovny-vykaz`
- Result: report 7221914 (template 687 "Úč MUJ") has `obsah.tabulky[]` = Strana aktív (46 cells), Strana pasív (44), Výkaz ziskov a strát (76) — **structured financials**, decoded positionally via `sablona?id=687` (`riadky[]` line-item labels).
- Decision: structured financials confirmed; RÚZ = recommended.

## Attempt 6

- Date/time: 2026-06-14
- Search engine or source: curl (RPO)
- Query: `api.statistics.sk/rpo/v1/search?identifier=31333532` → `entity/937053`
- Result: RPO returns full commercial-register data: identifiers/names/addresses (history), legalForms, activities[19], **statutoryBodies[3]** (directors), **stakeholders[9]** (shareholders), **equities/deposits** (share capital), predecessors, statisticalCodes. License **CC-BY 4.0** inline. Personal data present.
- Decision: RPO = recommended identity/officers/ownership source; redact PII.

## Attempt 7

- Date/time: 2026-06-14
- Source: documentation review
- Query: ORSR (orsr.sk), FinStat, data.gov.sk
- Result: ORSR commercial register is already exposed via RPO; FinStat etc. are aggregators (restricted/paid bulk).
- Decision: rely on official RPO + RÚZ; aggregators = cross-check only.
