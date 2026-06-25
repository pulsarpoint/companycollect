# Morocco — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Morocco, download/sample where allowed, and document a
reproducible trail. Do not bypass access controls.

## What was found

### 1. OMPIC / directinfo.ma — official company register (reCAPTCHA + paid)

- **OMPIC** (Office Marocain de la Propriété Industrielle et Commerciale) runs the
  **Registre Central du Commerce (RCC)** and serves company data through
  **`directinfo.ma`** (verified, HTTP 200; `www.ompic.ma` timed out). Its model:
  - a **free basic search** ("Recherche avancée" — Registre central du Commerce,
    entreprise, ICE, marque, brevet) — but **Google reCAPTCHA-gated** (the page loads
    `recaptcha/api.js?onload=onloadCallback`). **Not bypassed.**
  - **paid detailed data** — company profiles, **Bilans** (financial statements),
    statistics;
  - a documented **subscription API** (OMPIC "CAPI"/certificate).
- There is **no open bulk register and no open API**. The richest source, but
  reCAPTCHA-gated for search and paid for detail.

### 2. Casablanca Stock Exchange — listed companies + financials (OPEN)

- **`casablanca-bourse.com`** (FR site) is open. The **issuer listing**
  (`/fr/listing-des-emetteurs`, HTTP 200) lists all listed companies — **verified
  live**: **AFMA SA**, **Afric Industries SA**, **Alliances Développement Immobilier
  SA**, **Atlanta Sanad**, plus banks and holdings sections (and well-known blue
  chips such as Attijariwafa Bank, Maroc Telecom, BCP, Cosumar).
- **Issuer publications / financial results** (`/fr/publications-des-emetteurs`,
  HTTP 200) provide listed-company **financial statements**. The site also has
  market instruments/actions pages.
- **Listed companies only** (~75). Private-company financials are not here.

### 3. data.gov.ma — national open data (CKAN; no company register)

- **`data.gov.ma`** is a **working CKAN** portal (`/data/api/3/action/package_search`
  responds). But it has **no company register** — searches for `entreprise`,
  `registre commerce`, `ompic`, `societe` returned only **statistics** (Bank
  Al-Maghrib insurance-company patrimony, CNSS declarant counts, ANRT data) and
  unrelated datasets. No OMPIC company dataset.

### 4. Tax — DGI

- The **DGI** (Direction Générale des Impôts) administers the **Identifiant Fiscal
  (IF)** and the **ICE** is the cross-agency unified id. Per-company; not open bulk.

## Conclusion

Morocco's official registry (**OMPIC**) serves company data via **directinfo.ma** but
the **free search is reCAPTCHA-gated** and **detailed data + Bilans + API are paid**,
with no open bulk/API. The one genuinely **open** source is the **Casablanca Stock
Exchange** (issuer listing + financial publications) — **verified live**.
**data.gov.ma** is real CKAN but has **no company register** (statistics only). So
there is **no open bulk corporate register and no open private financials** —
ingestion is `blocked` (OMPIC) + open-for-listed (Casablanca Bourse). Identifiers:
**ICE** (15-digit unified id), **RC**, **IF** (tax). Currency **MAD**; French +
Arabic. Managers/shareholders are personal data (Law 09-08) — redact. No access
controls were bypassed; the sample uses **Casablanca-verified + public-knowledge
listed companies with null OMPIC identifiers** (nothing fabricated).
