# Belgium — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `KBO BCE Open Data download Kruispuntbank Ondernemingen Banque-Carrefour Entreprises CSV bulk enterprises Belgium`
- Language: Dutch/French/English
- Why this query was tried: Find the authoritative register + any open bulk.
- Top relevant URLs:
  - https://economie.fgov.be/en/themes/enterprises/crossroads-bank-enterprises/services-everyone/cbe-open-data
  - https://kbopub.economie.fgov.be/kbo-open-data/login
  - https://github.com/Fedict/lod-cbe
- Result: KBO/BCE Open Data = FREE bulk CSV (full + update file), free registration + terms. ~1.9M enterprises.
- Decision: Mark KBO Open Data as the open company master.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `NBB BNB Central Balance Sheet Office annual accounts XBRL bulk download open data free Belgium`
- Language: English/Dutch/French
- Why this query was tried: Find financial-statement access + format.
- Top relevant URLs:
  - https://www.nbb.be/en/central-balance-sheet-office
  - https://www.nbb.be/en/central-balance-sheet-office/consultation/web-services
  - https://developer.cbso.nbb.be
- Result: NBB CBSO = FREE annual accounts; ~99% XBRL; XBRL since 2007, CSV since 2022, PDF since 1999. Web services (free Authentic Data; paid Improved).
- Decision: Catalog NBB CBSO as the open structured-financials source.

## Attempt 3
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `KBO public search API ondernemingsnummer Belgium open data data.gov.be company register`
- Language: Dutch/English
- Result: KBO Public Search = free web (no account); Public Search Web Service API PAID (~EUR 50/2000). Free third-party REST mirrors (cbeapi.be) with a free key. Identifier = 10-digit enterprise number.
- Decision: Catalog Public Search (free web / paid API) + free REST mirrors.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebFetch (NBB web services + consult; KBO open data page)
- Result:
  - NBB web services: 5 types (Authentic Data Query + Daily Extract free; Improved paid); developer.cbso.nbb.be free account + CLIENT_ID; JSON-from-XBRL since 2022.
  - NBB CONSULT (consult.cbso.nbb.be): free per-entity PDF (1999)/XBRL (2007)/CSV (2022).
  - KBO Open Data: complete file (all active entities + establishment units) + daily update; kept 31 days; registration + terms; SFTP on request; personal data NOT for direct marketing.
- Decision: Confirmed both open paths; note free-account requirement.

## Attempt 5 (live verification — not successful for a per-company sample)
- Date/time: 2026-06-14
- Source: curl (free CBE REST mirrors) + WebFetch (cbeapi.be 403)
- Targets: cbeapi.be /api/v1/company/{n} (HTTP 401 — API key required), /api/company (404), companybelgium (DNS 000); KBO cookbook PDF (404).
- Result: could NOT pull a per-company open sample without a free account/API key (KBO bulk needs registration; free REST mirrors need a key). The KBO CSV structure + NBB XBRL are well documented.
- Decision: Document both sources + structures; schematic normalized sample (no per-company open record downloadable here).
