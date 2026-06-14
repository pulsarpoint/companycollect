# Poland — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `KRS API ms.gov.pl OdpisAktualny JSON pobieranie danych spółek dokumentacja open data`
- Language: Polish/English
- Why this query was tried: Find the authoritative company register and any open API/bulk.
- Top relevant URLs:
  - https://prs.ms.gov.pl/krs/openApi
  - https://www.gov.pl/web/sprawiedliwosc/uruchomienie-otwartego-api-krajowego-rejestru-sadowego
  - https://dane.gov.pl/en/dataset/27606,api-krajowego-rejestru-sadowego-api-krs
- Result: KRS has a FREE open API (OdpisAktualny/OdpisPelny), JSON, no auth, personal data anonymized; rejestr P/S.
- Decision: Treat KRS API as the open spine; test it live.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `sprawozdania finansowe KRS RDF e-Sprawozdania finansowe XML XBRL pobieranie API darmowe dostęp`
- Language: Polish
- Why this query was tried: Find financial-statement access + format.
- Top relevant URLs:
  - https://ekrs.ms.gov.pl/rdf/pd/search_df
  - https://www.biznes.gov.pl/pl/opisy-procedur/-/proc/643
- Result: Financial statements filed as structured XML (MF logical schema); FREE per-company search/download from RDF (XML + PDF); XBRL for listed.
- Decision: Catalog RDF as the open structured financial source.

## Attempt 3 (live verification)
- Date/time: 2026-06-14
- Source: curl
- Query: `GET https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000026438?rejestr=P&format=json`
- Result: HTTP 200, 59 KB JSON. Structure: odpis.{rodzaj, naglowekA, dane.dzial1..6}. dzial1.danePodmiotu.identyfikatory = {regon, nip}; nazwa; formaPrawna; siedzibaIAdres incl. website; kapital. dzial3 = PKD + wzmianki o złożonych dokumentach + rok obrotowy. dzial6 = likwidacja/upadłość.
- Decision: Save sample; KRS confirmed as rich open spine.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebSearch + WebFetch
- Queries: `Biała lista podatników VAT API Ministerstwo Finansów ... dane.gov.pl`; `Repozytorium Dokumentów Finansowych KRS bezpłatne pobieranie`; WebFetch prs.ms.gov.pl/krs/openApi
- Language: Polish/English
- Result: White list = free API (wl-api.mf.gov.pl) + daily flat file; bridges NIP/REGON/KRS + bank accounts + VAT status. RDF free per-company download confirmed (ekrs.ms.gov.pl/rdf/). prs.ms.gov.pl openApi page is JS-rendered (minimal content fetched).
- Decision: Catalog white list as the open VAT bridge; test it live.

## Attempt 5 (live verification)
- Date/time: 2026-06-14
- Source: curl
- Query: `GET https://wl-api.mf.gov.pl/api/search/nip/5250007738?date=2026-06-12`
- Result: HTTP 200. result.subject = {name, nip, regon, krs, statusVat=Czynny, workingAddress, accountNumbers, representatives, partners, registrationLegalDate, ...}. Confirms NIP<->REGON<->KRS bridge + bank accounts.
- Decision: Save sample; build normalized record from KRS + white list. Note REGON length differs (KRS 14-digit, white list 9-digit).

## Attempt 6 (documented, not downloaded)
- Date/time: 2026-06-14
- Source: prior knowledge + inventory
- Result: CEIDG (sole traders, free token API), REGON/GUS BIR1 (all entities, free key), CRBR (free beneficial ownership), dane.gov.pl (catalog). Commercial aggregators (Rejestr.io, MGBI) resell open data.
- Decision: Catalog CEIDG/REGON for full coverage, CRBR for ownership; aggregators optional.
