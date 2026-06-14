# Austria — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Firmenbuch Österreich Abfrage Verrechnungsstelle justizonline Jahresabschluss Bilanz kostenlos zugang Daten`
- Language: German
- Why this query was tried: Find the authoritative register + financial access + free vs paid.
- Top relevant URLs:
  - https://justizonline.gv.at/jop/web/firmenbuchabfrage
  - https://www.justiz.gv.at/service/datenbanken/firmenbuch/firmenbuchabfrage...
  - https://www.wko.at/.../uebermittlung-der-bilanzen-an-das-firmenbuch-finanzonline
- Result: Firmenbuch = authoritative; free brief Teilauszug + paid full extract/documents/Jahresabschluss via Verrechnungsstellen. Since 1.1.2026 filing via JustizOnline.
- Decision: Treat Firmenbuch + Jahresabschluss as authoritative-but-paid.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `GISA Gewerbeinformationssystem Austria open data data.gv.at download CSV Unternehmen Gewerbeberechtigungen`
- Language: German/English
- Why this query was tried: Find a FREE open company/trade dataset.
- Top relevant URLs:
  - https://www.gisa.gv.at/
  - https://www.data.gv.at/katalog/dataset/gewerbe-in-osterreich/...
  - https://www.bmwet.gv.at/.../GISA_Gewerbeinformationssystem.html
- Result: GISA = free per-company web queries; data.gv.at hosts "Gewerbe in Österreich" (active trade authorizations WITHOUT personal data) as open CSV/JSON.
- Decision: Mark GISA open dataset as the best open subset.

## Attempt 3
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Austria company register open data data.gv.at Firmenbuch Unternehmensregister bulk financial statements API`
- Language: English
- Result: Firmenbuch API exists (free but requires Austrian ID). Jahresabschluss publicly accessible FOR A FEE. firmafind = commercial JSON API (Firmenbuch + accounts). Clearing houses: HF data, Lexunited, etc.
- Decision: Catalog the ID-gated API + paid aggregators.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebSearch + WebFetch + curl
- Queries/targets: `data.gv.at Datensatz Unternehmen Firmen GISA Gewerbe CSV`; WebFetch justizonline firmenbuchabfrage (SPA, minimal); `Ediktsdatei Insolvenzdatei open data API`; curl iwg.justiz.gv.at edikte API.
- Result: data.gv.at confirms "Gewerbe in Österreich" (GISA CSV/JSON, no personal data) + trade-code list. Insolvency: free web queries (edikte.justiz.gv.at); structured JSON feed (iwg.justiz.gv.at) returned a LOGIN wall (IWG licence required).
- Decision: GISA open; insolvency feed license-gated (web queries free).

## Attempt 5 (download attempts — not successful)
- Date/time: 2026-06-14
- Source: curl + WebFetch + data.gv.at CKAN API
- Targets: data.gv.at CKAN package_search/package_show (returned SPA HTML, not JSON); the gewerbe-in-osterreich resource URL (HTTP 404); guessed GISA file URLs (DNS fail / 404); Vienna WFS Gewerbe (typeName exception).
- Result: Could NOT resolve the direct GISA open-data file URL in this environment (data.gv.at is JS-fronted; resource host not reachable via guessed paths). The dataset + resources are confirmed to exist and be open.
- Decision: Document the open GISA dataset and its access path; mark the direct file URL as a follow-up (resolve via the portal UI / CKAN resource id). No per-company open file downloaded -> schematic normalized sample.
