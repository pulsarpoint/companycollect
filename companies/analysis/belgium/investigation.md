# Belgium — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: BE | Languages: Dutch (NL), French (FR), German (DE), English

## Goal

Find official/reliable public sources for Belgian company data — prioritising bulk-ingestible open data
and **financial data** — and leave a reproducible trail with samples and licensing.

## Summary of findings

Belgium is **top-tier open**: the company register **and** the annual accounts are **free and
machine-readable**, both via a **free registration/account** (not payment).

- **KBO/BCE Open Data** — a free **bulk CSV** company master (all active enterprises + establishment units).
- **NBB Central Balance Sheet Office** — **free, structured XBRL** annual accounts (≈99% XBRL, back to 2007).
- Everything joins on the **Ondernemingsnummer** (10-digit enterprise number = VAT root).

### 1. KBO/BCE Open Data — RECOMMENDED (open company master)
- Publisher: **FOD Economie / SPF Économie** (Crossroads Bank for Enterprises / Kruispuntbank van
  Ondernemingen / Banque-Carrefour des Entreprises). Official.
- Access: free **bulk CSV** download — a **complete file** (all active registered entities + establishment
  units) + a **daily update file**; made available **daily**, kept **31 days**. Requires free
  **registration + terms acceptance** (no payment). Portal: kbopub.economie.fgov.be/kbo-open-data ; **SFTP**
  on request (kbo-bce-webservice@economie.fgov.be).
- Content (standard KBO open-data CSV set): **enterprise.csv** (ondernemingsnummer, status, legal form,
  legal situation, start date, type), **establishment.csv** (vestigingseenheidsnummer), **denomination.csv**
  (names by language/type), **address.csv**, **activity.csv** (NACE-BEL codes), **contact.csv**,
  **branch.csv**, **code.csv** (code-label lookups), **meta.csv** (snapshot metadata).
- License: **Licence-BCE-Open-Data** — reuse allowed; **personal data must not be reused for direct
  marketing**.
- ~1.9M+ enterprises. The open company spine.

### 2. NBB Central Balance Sheet Office — RECOMMENDED (open structured financials)
- Publisher: **Nationale Bank van België / Banque Nationale de Belgique (NBB/BNB)** — Centrale des bilans /
  Balanscentrale. Official. Collects the annual accounts of most Belgian legal entities and **makes them
  available free of charge** to the public. ≈**99% filed in XBRL**.
- Access:
  - **CONSULT** (consult.cbso.nbb.be) — free per-entity download: **PDF** (since 1999), **XBRL** (since
    2007), **CSV** (since 2022).
  - **Web services** (developer.cbso.nbb.be, **free account**): **Authentic Data Query** + **Authentic Data
    Daily Extract** are **free**; **Improved Data** (NBB-rectified) is **paid**. JSON-from-XBRL since 2022
    (last 3 years); XBRL archives back to 2007.
  - Also **Extract** application and **NBB.Stat**.
- Content: full **balance sheet + income statement** in **XBRL** under the Belgian GAAP schemas
  (**micro / abbreviated (verkort/abrégé) / full (volledig/complet)**) — structured numeric financials.
- The standout: **free, structured XBRL financials in bulk** back to 2007 — among the best open financial
  sources of any country analysed.

### 3. KBO Public Search + free REST mirrors — lookup
- **KBO Public Search** (kbopub.economie.fgov.be) — free **web** search (no account), NL/FR/DE/EN. The
  Public Search **Web Service API** is **paid** (~€50 / 2000 requests).
- **Free third-party REST APIs** (cbeapi.be, companybelgium.be, …) mirror the KBO open data with a free
  tier; they require a **free API key** (verified: 401 without a key). Convenience for lookups, not the bulk path.

### 4. UBO register — beneficial ownership (restricted)
- Register of **uiteindelijke begunstigden / bénéficiaires effectifs**. Access via **MyMinfin**, restricted
  to authorities / obliged entities / **legitimate interest** (fee). **Not** open bulk. Out of scope.

### 5. data.gov.be + Moniteur Belge + commercial
- **data.gov.be** — national open-data catalog (lists the KBO open data + others).
- **Moniteur Belge / Belgisch Staatsblad** — official gazette (company publications; free search). Useful
  for acts/announcements, not a bulk master.
- **Commercial aggregators** — Companyweb, Trends Top, **Bureau van Dijk (Bel-First)**, Graydon — paid
  enrichment/scores; not needed given the open sources.

## Conclusion

- **Company master**: KBO/BCE Open Data bulk CSV (free, registration).
- **Financials**: NBB CBSO **free structured XBRL** (web services free account, or CONSULT per-entity).
- **Join**: single **Ondernemingsnummer** across both (= VAT root). Belgium is effectively a fully open
  company-data jurisdiction, with best-in-class structured financials.

## Risks / open questions

- **Both bulk sources need a free account/registration** — not payment, but not anonymous; provision
  credentials (KBO portal/SFTP; NBB developer CLIENT_ID).
- **KBO license**: reuse OK but **no direct marketing** with personal data — respect it.
- **NBB "Improved Data" is paid**; the free **Authentic Data** is the as-filed version (use it).
- **XBRL parsing**: Belgian GAAP taxonomy with micro/abbreviated/full variants + yearly versions — the
  parser must handle schema variants; small companies disclose less (often no income statement detail).
- **Identifiers**: Ondernemingsnummer (10 digits, VAT = `BE` + number); vestigingseenheidsnummer for
  establishments. Could not pull a per-company open sample here (bulk needs registration; free REST mirrors
  need an API key) — documented; structures are well documented.
