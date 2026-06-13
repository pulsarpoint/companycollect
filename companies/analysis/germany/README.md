# Company data sources for Germany

## Status

### Company registry data
- Official bulk data: **not found** (no official bulk dump from Handelsregister/Unternehmensregister)
- Official API: **not found** (no official open API; commercial third-party APIs exist)
- Open data portal: **found** (OffeneRegister.de — community/NGO open data; GovData.de national catalog)
- License: **known with caveat** (OffeneRegister states CC-BY 4.0; OpenSanctions mirror labels it CC-BY-NC 4.0 — confirm before commercial use)
- Recommended ingestion path: **bulk download** (OffeneRegister) for breadth + **commercial API or targeted lookups** for freshness

### Financial data (annual financial statements / Jahresabschluss)
- Official bulk data: **not found** (no open dump of financial statements)
- Official retrieval API: **not found** (official XML/XBRL interfaces are for *submitting* statements, not bulk retrieval)
- Free viewing: **found** — since the DiRUG law (2022-08-01) anyone can **view financial statements free of charge, without registration**, at unternehmensregister.de (statements for fiscal years ≥2022) and bundesanzeiger.de (pre-2022). Free *viewing* ≠ free *bulk reuse*.
- Format: **XBRL** (HGB / IFRS / US-GAAP taxonomies; ESEF iXBRL for listed issuers), but many filings render as HTML/PDF.
- Coverage: all capital companies (GmbH, UG, AG, GmbH & Co. KG) must disclose under **§325 HGB**; detail depends on size class (§§267/267a HGB) — micro/small file far less than large.
- Recommended ingestion path: **commercial financial API** (OpenRegister, North Data, etc.) for structured JSON at scale, OR free **targeted per-company retrieval** via the `bundesanzeiger` Python tool (captcha/rate-limited — not bulk).

## Best source

**OffeneRegister.de** — `https://daten.offeneregister.de/de_companies_ocdata.jsonl.bz2`

The only free, openly-licensed **bulk** dataset covering essentially all German companies
(~5.1M companies + ~4M officers), published by the Open Knowledge Foundation Deutschland from
OpenCorporates data. It is JSONL (OpenCorporates company schema), 260 MB compressed / ~4 GB
uncompressed, **downloaded and verified** into `raw/bulk/`.

Key limitation: coverage is mostly **2017–2019** (snapshot is stale). A newer SQLite snapshot
(`handelsregister.db`, 2022) exists in the same directory for more recent data.

The **official** Handelsregister (handelsregister.de) is the authoritative source but offers
**no bulk download and no open API**, is fragmented across 150+ local courts, and its terms forbid
automated mass retrieval. For fresh, complete, structured data the realistic options are
**paid commercial APIs** (handelsregister.ai, OpenRegister, Viaductus, Kausate) or **targeted,
rate-limited lookups** via the official portal / `bundesAPI/handelsregister`.

## Best source — financial data

There is **no open/bulk source** for German financial statements. Two realistic paths:

1. **Commercial financial API (recommended for scale)** — e.g. **OpenRegister.de** exposes a
   Bundesanzeiger source as structured **JSON** (balance-sheet totals, revenue, net income, equity,
   cash, employee counts) with daily updates over hundreds of thousands of companies; also North Data,
   handelsregister.ai, Implisense, Creditreform, Dun & Bradstreet. Paid, redistribution by contract.
2. **Free targeted retrieval** — the `bundesanzeiger` module of the `deutschland` Python package
   (bundesAPI) fetches a company's published reports (incl. `Jahresabschluss…`) by querying the
   public Bundesanzeiger search and solving its captcha with a bundled ML model. Returns report
   title → full-text/HTML content. **Per-company, captcha- and rate-limited — viable for lookups,
   not for ingesting the whole population.**

The official **Unternehmensregister bulk interface** (`webservice@rt.bundesanzeiger.de`,
"Massendatenschnittstelle") is for **delivering/submitting** statements in XML/XBRL by large filers,
**not** for bulk retrieval — so it does not help ingestion.

## Next action

1. Decide freshness requirement:
   - **Historical / breadth baseline** → ingest the downloaded `de_companies_ocdata.jsonl.bz2` (done: bulk saved, normalized sample created).
   - **More recent (2022)** → download `handelsregister.db` (3.7 GB SQLite) from the same directory.
   - **Current + maintained** → evaluate a commercial API (budget required) or build targeted lookups respecting the 60/hour limit.
2. Confirm the license (CC-BY vs CC-BY-NC) with OffeneRegister/OpenCorporates before any commercial redistribution.
3. Map the OpenCorporates schema to the internal company model using `schema_notes.md`.
4. **Financials:** decide between a commercial API (budget; structured JSON at scale) and free
   per-company `bundesanzeiger` retrieval (no budget; captcha/rate-limited; XBRL/HTML parsing needed).
   Either way, plan an XBRL/HGB-taxonomy parsing step — see the financial schema in `schema_notes.md`.
