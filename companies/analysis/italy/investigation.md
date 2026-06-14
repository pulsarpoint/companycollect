# Italy — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: IT | Languages: Italian, English

## Goal

Find official/reliable public sources for Italian company data — prioritising bulk-ingestible open data
and **financial data** (bilanci) — and leave a reproducible trail with samples and licensing.

## Summary of findings

Italy has a strong, **centralised authoritative register** but a **mostly closed (paid) access model**:

- The **Registro Imprese** (Business Register), run by the **Chambers of Commerce** via **InfoCamere**,
  is the authoritative, complete, per-company source — but access is **paid** (Web Service/API + the
  **Telemaco** portal). There is **no open bulk master**.
- **Financials (bilanci)** are deposited in **XBRL** (a real strength) but are likewise **paid per
  document**. No open bulk.
- **Open data** exists but is **aggregate/statistical** (counts by territory/ATECO/time) plus a few
  **per-company subsets** (innovative startups/PMI innovative, procurement suppliers, LEI holders).

### 1. Registro Imprese / InfoCamere — authoritative, PAID
- Publisher: Chambers of Commerce (CCIAA) via **InfoCamere** (their IT consortium). Official.
- Access: **Telemaco** portal + Web Service/API at `accessoallebanchedati.registroimprese.it/abdo/api`:
  **Ricerca Anagrafica**, **Visure** (ordinary/historical), **Visura Amministratori**, **Bilancio XBRL**,
  **Protesti**. All **paid/contractual**; no open bulk.
- Holds: denominazione, **Codice Fiscale / Partita IVA**, **numero REA** (province-scoped), legal form,
  ATECO activity, capital, status (active/liquidation/bankruptcy/ceased), administrators, PEC, addresses.
- Best for authoritative per-company lookups and documents — not bulk ingestion at zero cost.

### 2. Registro Imprese — Bilanci XBRL — FINANCIALS (paid)
- Companies deposit annual accounts (bilanci) at the Registro Imprese in **XBRL** (taxonomy "2018-11-04",
  mandatory from 2019). Retrievable per company via Telemaco / registroimprese.it / the Bilancio XBRL API
  as **PDF/HTML/XLS/CSV** (and EN/FR/DE prospetto).
- **Paid per document; no open bulk, no free API.** The systematic XBRL format is a strength, but access
  is gated. The realistic financial source for the population.

### 3. InfoCamere / CCIAA regional open data — OPEN but AGGREGATE
- Portals like **opendata.marche.camcom.it ("Open Data Imprese Italia")** publish CC-BY 4.0 datasets:
  active companies by **territory + ATECO + time** (monthly/quarterly), bankruptcies (fallimenti),
  active persons, demographics. Formats: CSV, RDF/XML, XLSX, OData, JSON.
- ✅ Downloaded `imprese-attive-ateco.csv` (real, CC-BY) + the DCAT catalog — confirmed **aggregate**,
  not a per-company master. Good for denominators/benchmarks only.

### 4. Innovative startups & PMI innovative — OPEN per-company SUBSET
- **startup.registroimprese.it** / **MIMIT** publish weekly lists of **innovative startups** and
  **innovative SMEs** — free, per-company (denominazione, CF, sede, ATECO, etc.), **XLS** (+ a bulk API
  needing PEC + acceptance of conditions). License: IODL 2.0 / CC-BY.
- The only **free per-company** dataset, but a small subset of the economy.

### 5. Commercial aggregators — PAID, rich financials at scale
- **AIDA** (Bureau van Dijk / Moody's Orbis) — ~900k companies, **10-year XBRL financials**.
- **Cerved** — an official Registro Imprese distributor; credit + financial data + API.
- **Atoka** (Cerved-sourced) — ~6M Italian companies, daily, API.
- **CRIBIS** (CRIF). The realistic path to **financials + company master at scale** if budget allows.

### 6. Secondary open backdoors
- **ANAC** (Autorità Nazionale Anticorruzione) — open procurement data with **supplier CF/PIVA** →
  an open list of companies that have won/participated in public contracts.
- **ISTAT — ASIA** (statistical business register) — aggregate only.
- **GLEIF LEI** — open global LEI data; Italian LEI holders (a subset).
- **dati.gov.it** — national open-data catalog (discovery hub).
- **Agenzia delle Entrate / VIES** — VAT (Partita IVA) validation, not a listing.

## Conclusion

- **Authoritative + financials**: Registro Imprese (InfoCamere/Telemaco) — **paid**; bilanci in XBRL.
- **Free open**: only **aggregate statistics** (InfoCamere regional open data) + **per-company subsets**
  (innovative startups/PMI, ANAC suppliers, LEI). No open per-company master, no open bulk bilanci.
- **At scale**: a **commercial aggregator** (Cerved/AIDA/Atoka) is the realistic route for a full company
  master with financials.

## Risks / open questions

- **No open per-company master** — full coverage needs paid InfoCamere or an aggregator.
- **Financials paid** — XBRL is standardized but gated; budget per-document fees or an aggregator + an
  XBRL parsing step (Italian civil-code taxonomy).
- **Identifiers**: company keyed on **Codice Fiscale** (often = Partita IVA but not always); **REA** is
  province-scoped. Plan CF/PIVA/REA reconciliation.
- **License**: open datasets CC-BY 4.0 / IODL 2.0 (attribution); InfoCamere/aggregator data contractual.
- **Coverage skew**: open per-company data (startups/PMI innovative) is a small, non-representative subset.
