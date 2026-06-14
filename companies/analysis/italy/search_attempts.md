# Italy — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Registro Imprese InfoCamere open data API dati aziende download bulk Camere di Commercio`
- Language: Italian/English
- Why this query was tried: Find the authoritative register and any open bulk/API path.
- Top relevant URLs:
  - https://registroimprese.infocamere.it/l-anagrafe-nazionale-delle-imprese
  - https://accessoallebanchedati.registroimprese.it/abdo/api
  - https://opendata.marche.camcom.it/
  - http://www.datiopen.it/it/catalogo-opendata/infocamere
- Result: Registro Imprese = authoritative (InfoCamere), API + Telemaco but PAID. Regional InfoCamere open data exists (CC-BY) but looks aggregate.
- Decision: Treat Registro Imprese as authoritative-but-paid; inspect the regional open data and financials path.

## Attempt 2
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Registro Imprese bilanci XBRL deposito conti annuali accesso dati Telemaco visura società Italia`
- Language: Italian
- Why this query was tried: Find financial data (bilanci) access and format.
- Top relevant URLs:
  - https://www.registroimprese.it/deposito-bilanci
  - https://www.registroimprese.it/area-utente
  - https://www.ba.camcom.it/bari/registro-imprese/bilanci-xbrl
- Result: Bilanci deposited in **XBRL** (taxonomy 2018-11-04), retrievable per company via Telemaco / RI as PDF/HTML/XLS/CSV (+ EN/FR/DE). PAID per document; no open bulk.
- Decision: Catalog Bilanci XBRL as the (paid) financial source.

## Attempt 3
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `startup innovative PMI registro imprese open data CSV download dati.gov.it imprese`
- Language: Italian
- Why this query was tried: Find any FREE per-company open dataset.
- Top relevant URLs:
  - https://startup.registroimprese.it/
  - https://www.mimit.gov.it/it/open-data
  - https://opendata.marche.camcom.it/
- Result: Innovative startups + PMI innovative lists are free, weekly, per-company (XLS), IODL 2.0 / CC-BY. The one open per-company dataset (subset).
- Decision: Mark startup/PMI innovative as recommended open subset.

## Attempt 4
- Date/time: 2026-06-14
- Source: WebFetch (startup.registroimprese.it; RI API page) + WebSearch (opendata.marche)
- Result:
  - RI API page lists services: Ricerca Anagrafica, Protesti, Visura Amministratori, Visure, **Bilancio XBRL** — paid, "Scarica le API" / contact a consultant.
  - Startup portal: XLS/PDF downloads; bulk API needs **digital signature / PEC + conditions**.
  - opendata.marche = AGGREGATE company statistics (imprese-attive-ateco.csv etc.).
- Decision: Confirm aggregate nature by downloading a real CSV + catalog.

## Attempt 5 (live downloads)
- Date/time: 2026-06-14
- Source: curl
- Result:
  - `imprese-attive-ateco.csv` → HTTP 200, 12 KB — columns `Settore Ateco 2025; Divisione Ateco 2025; <month>; <month>` with COUNTS (aggregate, not per-company). CC-BY 4.0.
  - `dcat-opendata-catalog.rdf` → HTTP 200, 386 KB — all dataset titles are "Imprese Attive ... per Territorio/Comune/ATECO/Tempo" (aggregate).
  - Startup direct CSV/JSON guesses → 404 (behind dynamic portal).
- Decision: Saved real aggregate CSV + catalog with metadata/SHA-256; confirmed no open per-company master.

## Attempt 6
- Date/time: 2026-06-14
- Source: WebSearch (x2)
- Queries: `elenco startup innovative open data dati.gov.it download CSV ... campi`;
  `Italy company financial data Cerved Atoka AIDA Bureau van Dijk bilanci API paid`
- Language: Italian/English
- Result: Startup data via MIMIT open data (IODL 2.0). Financial aggregators: **AIDA** (BvD/Orbis, ~900k cos, 10-yr financials), **Atoka** (~6M cos, Cerved-sourced), **Cerved** (official RI distributor), CRIBIS.
- Decision: Catalog aggregators as the paid route to financials at scale; record ANAC/ISTAT/GLEIF as open secondary.
