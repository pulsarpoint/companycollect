# Cyprus — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: CY | Languages: Greek, English

## Goal

Find official/reliable public sources for Cypriot company data — prioritising bulk-ingestible open data and
**financial data** — and leave a reproducible trail with samples and licensing.

## Summary of findings

Cyprus is **partial-open** (Bulgaria-like): the **company register** is **open** (CSV on data.gov.cy, incl.
**officers**) plus a free eSearch, but the **financials** (audited statements filed with the **HE32** annual
return) are **public only via a paid €10 detailed search** as **scanned PDFs** — not structured open data.

### 1. Department of Registrar of Companies and Intellectual Property (DRCIP) — authoritative
- Publisher: **DRCIP** (Τμήμα Εφόρου Εταιρειών), Ministry of Energy, Commerce and Industry. Official; the
  competent authority for all registered business entities in Cyprus.
- Access:
  - **Free eSearch** — `efiling.drcor.mcit.gov.cy` / `companies.gov.cy` — search a company / business name /
    partnership / overseas company by name or registration number; **basic info free**.
  - **Open data (CSV)** — the DRCIP publishes **CSV** files on **data.gov.cy** (Registrar group #30): the
    list of all registered organisations + their **officers**. Confirmed via the **OpenSanctions cy_companies**
    dataset, which sources from **data.gov.cy in CSV** — **~567,536 companies**, **~2.75M entities** total, and
    **names their officers** (but not shareholders).
  - Exact CSV resource URL not resolved in this environment (data.gov.cy uses a non-standard CKAN path; `/api/3`
    returned 404; portal pages JS-rendered). Resolve via the portal UI (data.gov.cy/en/group/30).
- Fields (eSearch / open CSV): **registration number (HE…)**, organisation **name**, **type** (company /
  business name / partnership / overseas), **status** (e.g. operational / struck-off / dissolved), **registration
  date**, **registered address**, and **officers** (directors/secretary).

### 2. HE32 annual return + audited financial statements — FINANCIALS (paid, document-based)
- Each year a company files the **HE32 annual return** together with a copy of the **audited financial
  statements** of the prior year. (Private companies file the HE32I online via the e-filing system.)
- Access: the free eSearch shows the filing exists; the actual **scanned annual returns + financial statements**
  are obtained via a **detailed search costing €10** (per company) — **scanned PDFs**.
- So financials are **public** but **paid (€10/detailed search)** and **document-based (PDF/scanned)** — **not**
  structured open data (no XBRL/CSV of figures). Extracting figures needs **OCR/parsing** or a commercial
  provider.

### 3. data.gov.cy — national open data portal
- Cyprus's open-data portal (1200+ datasets). Hosts the DRCIP **Registrar group #30** with the open company
  CSV. CKAN-like API but on a **non-standard path** (the standard `/api/3/action/*` returned 404 here).

### 4. UBO register — beneficial ownership (restricted)
- The DRCIP maintains the **register of beneficial owners**. **Access conditions / fee** apply (post-CJEU).
  Not open bulk. Out of scope.

### 5. Other
- **Tax Department** — **TIC** (Tax Identification Code) / VAT; VAT = `CY` + 8 digits + letter.
- **OpenCorporates** (register #58) and **OpenSanctions cy_companies** (CC-BY-NC mirror of the open data).
- Commercial aggregators (CyprusRegistry.com, Kyckr, …) — paid enrichment + documents.

## Conclusion

- **Registry**: open — DRCIP CSV on data.gov.cy (companies + **officers**) + free eSearch.
- **Financials**: **paid (€10 detailed search) + document-based (PDF)** — parse/OCR or a commercial provider.
  No open structured bulk.
- **Join**: single **HE registration number**. Cyprus sits between the open group (BE/PL/HR/NO/FR) and the
  paid-financials group: registry open (CSV + officers), financials paid + PDF.

## Risks / open questions

- **data.gov.cy CSV URL unresolved here** — resolve the DRCIP dataset's resource URLs via the portal
  (data.gov.cy/en/group/30); confirm exact columns + the open licence.
- **Financials are paid + PDF** — €10 per detailed search; structured figures need OCR/parsing or a provider.
- **Identifiers**: registration number prefix encodes entity type (HE = company); TIC = tax id; VAT separate.
- **PII**: officers are in the open CSV (GDPR); beneficial owners restricted.
- Could not download a per-company sample here (data.gov.cy resource URL unresolved; financials paid) —
  documented; the open dataset's fields + scale (~567k companies, officers) are confirmed via OpenSanctions.
