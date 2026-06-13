# Investigation — Company data for France

Date: 2026-06-06
Country: France (FR) — slug `france`
Languages searched: French, English

## Summary

France has one of the richest open-company-data ecosystems in Europe. There is
a clear authoritative master register (INSEE Sirene) published as fully open bulk
data, multiple official APIs, and a complementary legal register (INPI RNE).
Beneficial ownership exists (RBE) but access is regulated and not freely open.

## The landscape

France splits "company data" across a few official producers:

1. **INSEE — répertoire Sirene** is the *administrative/statistical* register.
   It assigns the **SIREN** (9-digit legal-unit id) and **SIRET** (14-digit
   establishment id) that everything else keys on. It is the spine: name, legal
   category, NAF/APE activity code, headcount band, addresses, administrative
   state (active/ceased), creation date. Fully open (ODbL), bulk + daily API.

2. **INPI — Registre National des Entreprises (RNE)** is the *legal* register
   (successor to the centralized RCS / Infogreffe role for open data). Richer
   legal identity: share capital, legal representatives (dirigeants), beneficial
   owners, company acts/statutes (since 1993) and non-confidential annual
   accounts (since 2017). Bulk via SFTP after free registration, plus an API.

3. **DILA — BODACC** is the *official gazette* of civil & commercial
   announcements: the event stream of the company lifecycle (creation,
   modification, radiation, collective procedures, account filings, sales).
   Best used for change detection, not as a master list.

4. **DINUM — Annuaire des Entreprises / API Recherche d'Entreprises** is the
   citizen-facing *aggregator*. It rebuilds an Elasticsearch index from INSEE +
   INPI + others and exposes a clean, no-auth search API. Ideal entry point.

5. **DINUM — API Entreprise** is a restricted government gateway (habilitation
   required), not for open reuse — listed only for completeness.

## What was found (and verified)

- **Sirene bulk** — resolved the live monthly stock files via the data.gouv.fr
  dataset API. Current (2026-06-01) dated URLs captured in `source_inventory.json`.
  CSV.zip + Parquet for both legal units (~960 MB) and establishments (~2.83 GB).
  Not downloaded here due to size; URLs and stable landing page recorded.
- **API Recherche d'Entreprises** — called live, HTTP 200, real records for
  "la poste" saved to `raw/api/recherche_entreprises_sample.json`. No auth.
- **BODACC** — called live via Opendatasoft Explore v2.1, HTTP 200,
  `total_count` = 49,386,809, sample saved to `raw/api/bodacc_annonces_sample.json`.
- **API Sirene** and **RNE** — documented; both require a free account/key, so
  no live key-based call was made (no credentials, and we do not bypass auth).

## Financial data (annual accounts / comptes annuels)

> Added 2026-06-14 — the user requested financial data. France is a **standout for OPEN financials**.

### How French company financials are published
- Commercial companies deposit annual accounts (**comptes annuels** / bilans) at the greffes; the
  **INPI RNE** captures the **non-confidential** figures and republishes them as **open data** —
  **balance sheet, income statement, fixed assets, depreciation, provisions**, for filings since
  **2017-01-01** (format moved to **JSON** from 2023). Access: **data.inpi.fr** SFTP bulk + RNE API,
  free account.
- **Confidentiality option:** micro and small companies may legally file accounts as **confidential**
  (déclaration de confidentialité), so they are **absent** from the open set. Coverage is therefore
  **partial** (skewed toward larger companies).

### The easy open win — `finances` block (no auth)  ✅ verified
- The **API Recherche d'Entreprises** (no key) returns a per-company **`finances`** object keyed by
  year with **`ca`** (chiffre d'affaires / revenue) and **`resultat_net`** (net income). Example
  captured live: SIREN 356000000 LA POSTE → `{"2024": {"ca": 34569000000, "resultat_net": 1722000000}}`.
- This is the fastest way to attach headline financials to the whole company spine **for free**, but it
  is only two figures (CA + résultat net) and only where accounts are non-confidential.

### Full statements
- **INPI RNE comptes annuels** (bulk/API) — the full non-confidential balance sheet + income statement
  line items. The path when you need more than CA + résultat net.
- **data.economie.gouv.fr — "Documents et comptes des entreprises"** — Opendatasoft dataset (Open
  Licence 2.0) listing/serving company documents incl. comptes; useful for discovery/links.

### Restricted (not open) financial sources — for completeness
- **API Entreprise** (DINUM, habilitation only) brokers **DGFIP chiffres d'affaires** (last 3 years)
  and **Banque de France bilans** (last 3 years). Richer/authoritative but **reserved for
  administrations** — not open reuse.
- **Banque de France** Centrale de bilans / FIBEN — not open.

### Financial conclusion
- France gives **open financials**: headline CA + résultat net via the no-auth Recherche API, and full
  non-confidential statements via INPI RNE comptes annuels. The only real limit is the
  **confidentiality option** (partial coverage of small firms), not access or cost.

## Recommendation

- **Master list / identifiers:** INSEE Sirene bulk (Parquet preferred), refreshed
  monthly, with daily deltas from API Sirene.
- **Legal enrichment:** INPI RNE bulk (SFTP) for capital, dirigeants, accounts.
- **Lifecycle events:** BODACC API for ongoing change detection.
- **Fast prototype / lookups:** API Recherche d'Entreprises (no auth, works now).
- **Financials:** `finances` block (CA + résultat net) from the Recherche API for breadth at zero cost;
  INPI RNE comptes annuels bulk for full balance-sheet/income-statement detail. Expect partial coverage
  due to the confidentiality option.

## Risks / open questions

- Sirene direct file URLs are dated and rotate monthly — automation MUST resolve
  them via the stable landing page / data.gouv.fr dataset API, not hardcode them.
- Bulk sizes are large (multi-GB); confirm environment capacity before download.
- Beneficial ownership (RBE) is regulated — not part of the open bulk.
- NAF/APE codes are transitioning (NAF Rev2 → NAF2025); both codes now appear in
  Sirene/Recherche API. Mapping tables needed for consistent activity coding.
- ODbL (Sirene) requires attribution + share-alike on derived databases —
  see `license_notes.md`.
- **Financial coverage is partial** — the legal confidentiality option removes many small companies'
  accounts from the open set; micro-entrepreneurs lack DGFIP revenue. Do not treat missing financials
  as zero.
- The Recherche API `finances` block is only **CA + résultat net**; full statements need INPI RNE.
