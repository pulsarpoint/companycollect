# Spain — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: ES | Languages: Spanish (Castilian), English

## Goal

Find official/reliable public sources for Spanish company data — prioritising bulk-ingestible open data
and **financial data** (annual accounts) — and leave a reproducible trail with samples and licensing.

## Summary of findings

Spain has a **strong open path for company identity/events** but a **mostly paid path for financials**:

- **Identity/events are open**: the **BORME** (Boletín Oficial del Registro Mercantil) is published as
  open data by the **BOE** (Agencia Estatal Boletín Oficial del Estado) with a free REST API and
  per-province act XML. **OpenMercantil** turns this stream into a CC-BY 4.0 company master DB.
- **The authoritative register** (Registro Mercantil / CORPME, registradores.org) is **per-document
  paid**, no bulk, no free API.
- **Financials**: **listed companies** are fully open via **CNMV** (XBRL). **Non-listed companies**
  deposit annual accounts (XBRL) at the Registro Mercantil, retrievable **per company for ~€9–20** —
  cheap but **not bulk and not free**.

### 1. BORME via BOE open-data API — RECOMMENDED (open, authoritative events)
- Publisher: **Agencia Estatal Boletín Oficial del Estado (BOE)** — official.
- API: `https://www.boe.es/datosabiertos/api/borme/sumario/{YYYYMMDD}` → daily summary (JSON/XML),
  then per-province act bulletins in **HTML/PDF/XML** (`.../diario_borme/xml.php?id=BORME-A-YYYY-N-PP`).
- Coverage: **Sección I (Empresarios — actos inscritos) 2009→present**; Sección II (anuncios) 2001→.
- Content: registered acts — **Constitución** (incorporation, with capital, address, objeto social/CNAE,
  administrators), Nombramientos/Ceses (appointments), Ampliación de capital, Disolución, etc., plus
  **Datos registrales** (Tomo/Folio/Sección/**Hoja** registral identifier).
- Format reality: the act text is **semi-structured Spanish prose** inside `<p class="articulo">`
  (company) / `<p class="parrafo">` (acts) — needs NLP/regex parsing. Tools: **bormeparser** (Python).
- ✅ Downloaded & verified: a summary (`borme_sumario_20240115.json`) and a province act XML
  (`borme_A_2024_10_04_almeria.xml`).
- License: BOE open-data reuse (open) — confirm attribution terms.

### 2. OpenMercantil — RECOMMENDED (open reconstructed master)
- Community/NGO project that ingests BORME (D+1), parses it against typed schemas, and republishes.
- **~2.8M companies, ~5.8M acts (2009→present)**. Bulk **CSV (210 MB, 5.8M rows, 12 cols) + Parquet**,
  plus a **REST API** (`/api`) and per-company export. Full bulk file is "Próximamente"; **samples,
  per-company export, and API are live now**.
- Columns: Date, Section, Province, Company Name, **CIF**, Website, **Capital**, Address, **Workers**,
  Act Type, Details, ID. Sample DB columns: `slug,name,cif,province,first_seen,last_seen,acts_count`.
- **Explicitly EXCLUDES** full financial statements, revenue, employee counts beyond what BORME states.
- Identifier caveat: only **~18.2%** of rows carry a validated CIF (BORME acts often omit the CIF).
- License: **CC BY 4.0** (commercial use permitted with attribution).
- ✅ Downloaded & verified: `openmercantil_muestra_empresas_100.csv`, `openmercantil_muestra_nuevas_50.json`.

### 3. Registro Mercantil / CORPME (registradores.org) — authoritative, paid
- The official commercial register (provincial registries + Registro Mercantil Central for denominations).
- "Información Mercantil Interactiva" 24/7; search by **NIF/name**; **per-document fees**.
- **No bulk download, no free API.** Best for authoritative single-company verification + documents.

### 4. Registro Mercantil — Depósito de Cuentas Anuales — FINANCIALS (paid, per-company)
- Companies deposit annual accounts under the Código de Comercio; the registry holds them in **XBRL**
  (individual + consolidated), plus PDF (audit report, memoria, informe de gestión) in a ZIP.
- Retrieved **per company for ~€8.99–€20** via registradores.org; **no registration required**; retained
  **6 years**. Includes the **cuadro de posición económico-financiera** vs sector.
- **No bulk, no free API** — the central limitation for financials at scale.

### 5. CNMV — FINANCIALS for listed/issuer companies — RECOMMENDED (open XBRL)
- Comisión Nacional del Mercado de Valores. Publishes **Información Financiera Anual (IFA)** and
  **Intermedia (IFI/IPP)** for entities with securities admitted to trading, as **open XBRL + PDF**.
- Tooling: `cnmv.es/ipps/` (XBRL viewer/download) and **datos.gob.es** datasets; XBRL since 2005.
- Small population (hundreds of issuers) but **fully open** and standardized — the open financial source.

### 6. datos.gob.es — national open data portal
- Catalog (CKAN). Hosts BORME, CNMV datasets, DIRCE, and many regional company/contract datasets.
- Useful for discovery; not a company master file by itself.

### 7. INE — DIRCE (Directorio Central de Empresas) — aggregate only
- The statistical business register: ~3.31M active companies (1 Jan 2025), but published as
  **aggregate tables** (by region/CNAE/size/legal form), **not a per-company list**. Not a master source.

### 8. Registro Central de Titularidades Reales — beneficial ownership (restricted)
- Central beneficial-ownership register. Access restricted/fee-based; not open bulk. Out of scope here.

### 9. Commercial aggregators — paid (fresh + financials)
- **eInforma (Informa D&B)**, **Axesor**, **Iberinform**, **Datacentric**. Structured company + financial
  data + documents via API/portal. The turnkey path to financials at scale if budget allows.

## Conclusion

- **Open company master**: ingest **OpenMercantil** (CC-BY) and/or build from the **BORME BOE API**.
- **Financials**: **CNMV open XBRL** for listed; for the general population, financials are **XBRL but
  paid per-company** at the Registro Mercantil (~€9–20) or via a commercial aggregator. **No free open
  bulk of annual accounts exists.**
- Identity is open and cheap; financials are XBRL-standardized but gated behind small per-document fees.

## Risks / open questions

- **CIF coverage** in open data is low (~18% in OpenMercantil) — joining to tax id / financials is harder.
- **BORME parsing** — acts are free-text Spanish prose; needs a robust parser (bormeparser) and
  deduplication into a company master (the "Hoja registral" is the stable per-company key).
- **Financials not open in bulk** — scale requires paid registry lookups or an aggregator.
- **License** — confirm BOE reuse attribution and OpenMercantil CC-BY attribution before redistribution.
- **No single clean status/CNAE** in BORME without parsing; DIRCE has CNAE but only in aggregate.
