# Germany — Company Data Investigation

Date: 2026-06-06
Investigator: company-open-data-discovery skill (Claude)
Country code: DE | Languages: German, English

## Goal

Find official/reliable public sources for German company data, prioritising bulk-ingestible
open data, and leave a reproducible trail with samples and licensing notes.

## Summary of findings

Germany has **no official open bulk dataset or open API** for its commercial register. The
authoritative register (Handelsregister) is:

- **Fragmented** across 150+ local courts (Amtsgerichte), each with its own numbering.
- **Free to view** since 2022-08-01, but with **no bulk download** and **no public API**.
- Governed by **terms that forbid automated mass retrieval** (community tools self-limit to
  ≤60 lookups/hour; mass scraping may breach §§303a/303b StGB).

The practical landscape:

### 1. OffeneRegister.de — RECOMMENDED (open bulk)
- Run by Open Knowledge Foundation Deutschland from OpenCorporates data.
- ~5.1M companies + ~4M officers; OpenCorporates company schema.
- Live directory listing at `https://daten.offeneregister.de/`:
  - `de_companies_ocdata.jsonl.bz2` — 260 MB (2019) ✅ **downloaded & verified**
  - `handelsregister.db` — 3.7 GB SQLite (**2022**, more recent)
  - `openregister.db.gz` — 773 MB (2019)
  - `.torrent` files for the 2019 artifacts
- License stated as CC-BY 4.0 by OffeneRegister; the OpenSanctions mirror tags it CC-BY-**NC** 4.0.
  **Confirm before commercial use.**
- Main weakness: 2019 snapshot is stale (the SQLite is 2022).

### 2. OpenSanctions de_offeneregister — useful secondary (entity graph)
- Same underlying OffeneRegister data reshaped into FollowTheMoney entities + relationships
  (13.0M entities: 4.6M companies, 3.5M people, 0.9M orgs).
- `entities.ftm.json` ≈ 6.4–6.85 GB. Reprocessed 2026-06-05 but coverage still ends 2019-02.
- License: CC-BY-NC 4.0 (commercial needs separate license). Not downloaded (size).

### 3. Official Handelsregister (handelsregister.de) — authoritative, not bulkable
- No bulk, no API. Per-document structured XML ("SI") only. Announcements (Registerbekanntmachungen)
  visible only 8 weeks. Best for authoritative single-company verification.

### 4. Unternehmensregister (unternehmensregister.de) — enrichment
- Central portal: Handelsregister + Bundesanzeiger + annual financial statements + shareholder lists.
- Basic data free; full documents per-document fee (~€1). Per-document XML; no bulk/API.
- Best for **enrichment** (financials, shareholders) on specific companies.

### 5. BRIS / European e-Justice Portal — official EU single-company lookup
- EU Business Registers Interconnection System, real-time from national registers, provides EUID.
- Single-company search only; no bulk export.

### 6. bundesAPI/handelsregister — community scraper
- Python CLI scraping handelsregister.de extended search. Unofficial, ≤60 lookups/hour.
- Viable only for low-volume targeted lookups.

### 7. Commercial APIs — fresh/complete but paid
- handelsregister.ai, OpenRegister.de, Viaductus, Kausate, Implisense (RapidAPI).
- Daily-updated structured data + document retrieval. The realistic path to **current** data.

### 8. GovData.de — national open data catalog
- 120k+ datasets, CKAN API. Does not host the register as a company master file, but useful for
  regional business/statistical datasets.

## Financial data (annual financial statements / Jahresabschluss)

> Added 2026-06-13 — the user explicitly requested financial data. Researched separately below.

### Legal & structural background
- **§325 HGB** obliges all capital companies (GmbH, UG (haftungsbeschränkt), AG, KGaA, and
  GmbH & Co. KG) to **disclose** their annual financial statements. The amount disclosed scales
  with **size class** (§§267, 267a HGB):
  - *Micro* — balance sheet only, may merely **deposit** (hinterlegen) it (not publicly published, but viewable on request since 2022).
  - *Small* — abridged balance sheet + notes (no P&L required).
  - *Medium / Large* — full balance sheet, profit & loss, notes, management report, audit opinion.
- **Where filed:**
  - Fiscal years **≥ 2022** → filed exclusively via the **Unternehmensregister** publication platform (DiRUG, effective 2022-08-01).
  - Fiscal years **< 2022** → **Bundesanzeiger** (bundesanzeiger.de).
- **Free viewing:** Since DiRUG, viewing disclosed statements is **free and needs no registration**
  on unternehmensregister.de / bundesanzeiger.de. (Historically some access was fee-based; that changed.)
- **Format:** Submissions are **XBRL** (German **HGB** taxonomy; **IFRS** / **US-GAAP** also supported;
  **ESEF iXBRL** for capital-market issuers). In practice many published statements are rendered as
  **HTML/PDF**, so clean machine-readable XBRL is not always retrievable from the public view.

### What does NOT exist
- **No official open/bulk dataset** of financial statements.
- **No free official retrieval API.** The official **"Massendatenschnittstelle"**
  (`webservice@rt.bundesanzeiger.de`) and the Publikations-Plattform web service are for
  **delivering/submitting** statements in XML/XBRL by large filers — **not** for downloading the
  population of filed statements.
- The open **OffeneRegister.de** bulk (the recommended company-registry source) contains
  **registration + officers only — no financials** (confirmed from its README).

### Realistic financial-data paths
1. **Commercial financial APIs (recommended for scale).**
   - **OpenRegister.de** — documents a *Bundesanzeiger* source returning structured **JSON** instead
     of PDF: balance-sheet totals & component breakdowns, revenue/profitability, net income, equity,
     cash position, employee counts; "hundreds of thousands" of companies, **daily** updates.
   - **North Data**, **handelsregister.ai**, **Implisense**, **Viaductus**, **Kausate**,
     **Creditreform**, **Dun & Bradstreet / Bisnode** — financials + documents, paid.
   - Trade-off: budget required; redistribution governed by contract.
2. **Free targeted retrieval — `bundesanzeiger` (bundesAPI `deutschland` package).**
   - `Bundesanzeiger().get_reports("<company name>")` queries the **public Bundesanzeiger search**
     and returns a dict of report title → full-text/HTML content, including
     `"Jahresabschluss zum Geschäftsjahr vom … bis zum …"`. Uses a bundled **ML model to solve the
     search captcha**.
   - **Per-company, captcha/rate-limited, unofficial.** Good for enriching specific target companies;
     **not** a way to ingest all German financials. Output still needs XBRL/HTML → structured parsing.
3. **Manual / per-document** via unternehmensregister.de — free to view, fine for occasional lookups.

### Financial-data conclusion
- For **financials at scale**, the only realistic structured path is a **commercial API** (OpenRegister,
  North Data, …). There is **no open bulk** equivalent to the company-registry OffeneRegister dump.
- For **low-volume enrichment** of specific companies, the free `bundesanzeiger` Python tool works but
  requires captcha handling, respect for portal terms, and an XBRL/HTML parsing layer.
- Either way, budget an **XBRL parsing step** (Arelle / Brel / tidyxbrl) mapping HGB taxonomy concepts
  to a normalized financials model (see `schema_notes.md`).

## Conclusion

- For a **broad free baseline**: ingest the downloaded OffeneRegister JSONL (and/or the 2022 SQLite).
- For **freshness/completeness**: a commercial API is the only realistic structured option; otherwise
  targeted, rate-limited lookups against the official portal.
- No official government bulk/API exists; do not expect one.

## Risks / open questions

- **License ambiguity** (CC-BY vs CC-BY-NC) — must be resolved before commercial use/redistribution.
- **Staleness** — open bulk is 2019 (SQLite 2022); dissolved/new companies since are missing.
- **No tax_id/VAT** in the open dataset; would need separate enrichment (e.g. VIES for VAT validation).
- **Identifier fragmentation** — register numbers are court-scoped; `company_number` is a synthetic key.
- **Financials have no open bulk** — scale requires a paid API; the free route is captcha/rate-limited
  per-company scraping whose terms restrict mass automated retrieval.
- **Financial free-viewing ≠ free reuse** — confirm reuse/redistribution rights for any financial
  statements regardless of the free public-viewing access.
- **XBRL/HGB parsing effort** — financial statements need taxonomy-aware parsing; size-class variation
  means many filings (micro/small) carry only a balance sheet, no P&L/revenue.
