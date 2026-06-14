# Company data sources for Italy

## Status

### Company registry data
- Official bulk data: **not found** (no open bulk master of the Registro Imprese)
- Official API: **found but paid** (InfoCamere Web Service / `accessoallebanchedati.registroimprese.it`:
  Ricerca Anagrafica, Visure, Visura Amministratori, Bilancio XBRL, Protesti) + Telemaco portal
- Open data portal: **found** (dati.gov.it; regional InfoCamere "Open Data Imprese Italia") — but the
  company open data is **aggregate/statistical**, not a per-company master
- Open per-company subset: **found** — **innovative startups & PMI innovative** lists (free, weekly)
- License: **mixed** — InfoCamere open data CC-BY 4.0; startup/PMI innovative IODL 2.0 / CC-BY;
  Registro Imprese documents are paid/contractual
- Recommended ingestion path: **paid InfoCamere API / Telemaco** (authoritative) or a **commercial
  aggregator** (Cerved/AIDA/Atoka); **open data only covers a subset + aggregates**

### Financial data (annual accounts / bilanci) — XBRL but PAID
- Official bulk data: **not found** (no open bulk of bilanci)
- Official API: **paid** — InfoCamere "Bilancio XBRL" service / Telemaco; bilanci deposited in **XBRL**
  (taxonomy 2018-11-04), retrievable per company as PDF/HTML/XLS/CSV (+ EN/FR/DE)
- Format: **XBRL** (machine-readable, standardized) — strong, but access is per-document **paid**
- Recommended ingestion path: **commercial aggregator** (AIDA/Bureau van Dijk, Cerved, Atoka, CRIBIS)
  for financials at scale, or **paid per-company XBRL** via InfoCamere/Telemaco

## Best source

There is **no free open per-company master** for Italy. The authoritative source is the
**Registro Imprese** (InfoCamere / Chambers of Commerce), accessed via **paid** Web Service/API or the
**Telemaco** portal — it holds company identity (CF/Partita IVA, REA), administrators, ATECO activity,
capital, status, and **bilanci in XBRL**. For free, only **aggregate statistics** (regional InfoCamere
open data, CC-BY 4.0 — ✅ a real CSV downloaded here) and the **innovative startups/PMI innovative**
per-company lists are available. For financials at scale the realistic path is a **commercial
aggregator** (Cerved/AIDA/Atoka), all of which resell Registro Imprese data with 10-year XBRL financials.

## Next action

1. Decide access model: **paid InfoCamere/Telemaco** (authoritative, per-company) vs **commercial
   aggregator** (Cerved/AIDA/Atoka — bulk + API + financials) vs **open subset** (startup/PMI innovative).
2. For a free start: ingest the **innovative startups/PMI innovative** lists + ANAC procurement supplier
   identifiers (CF/PIVA) as an open seed; enrich via paid lookups.
3. **Financials:** plan XBRL parsing (Italian civil-code taxonomy) over paid per-company bilanci, or use
   an aggregator's structured financials API.
4. Confirm licenses (CC-BY 4.0 / IODL 2.0 for open data; contractual for InfoCamere/aggregators).

See `investigation.md` for detail and `source_inventory.md` for the table.
