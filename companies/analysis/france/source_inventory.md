# Source inventory — France

| Source | Org | Type | Access | Auth | Formats | License | Status |
|---|---|---|---|---|---|---|---|
| **Base Sirene (SIREN/SIRET)** | INSEE | Bulk registry | Public | No | CSV/ZIP, Parquet | ODbL | **recommended** |
| **API Sirene** | INSEE | Registry API | Public + free key | Yes | JSON | ODbL | **recommended** |
| **API Recherche d'Entreprises** | DINUM | Aggregator API | Public | No | JSON | Open | **recommended** |
| **RNE (Data INPI)** | INPI | Bulk + API | Public + free acct | Yes (SFTP/API) | JSON/XML/PDF | Open data | **recommended** |
| **BODACC** | DILA | Gazette / events API | Public | No | JSON/CSV/XLSX | Licence Ouverte 2.0 | useful_secondary |
| **INPI RNE — Comptes annuels** | INPI | Financials bulk+API | Public + free acct | Yes (SFTP/API) | JSON/PDF | Open data | **recommended** (financials) |
| **Recherche API — `finances` block** | DINUM | Financials via aggregator | Public | No | JSON | Open | **recommended** (headline financials) |
| **Documents et comptes des entreprises** | MinÉco | Doc/accounts catalog | Public | No | JSON/CSV | Licence Ouverte 2.0 | useful_secondary |
| **API Entreprise** | DINUM | Gov gateway (incl. DGFIP CA, BdF bilans) | Restricted | Yes (habilitation) | JSON | Restricted | blocked_by_license |

## Quick reference — endpoints

- **Sirene bulk landing (stable):** https://www.sirene.fr/sirene/public/static/open-data
- **Sirene dataset (data.gouv.fr):** https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret
  - StockUniteLegale CSV.zip (~960 MB), StockEtablissement CSV.zip (~2.83 GB), Parquet variants
- **API Sirene docs:** https://portail-api.insee.fr/catalog/api/2ba0e549-5587-3ef1-9082-99cd865de66f/doc
- **API Recherche d'Entreprises:** `GET https://recherche-entreprises.api.gouv.fr/search?q=...` (no auth)
- **RNE / Data INPI:** https://data.inpi.fr/  (bulk via SFTP after free registration)
- **BODACC records API:** `GET https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records`

## Financial endpoints

- **Recherche API `finances` block** (no auth): `GET https://recherche-entreprises.api.gouv.fr/search?q=...`
  → each result has `finances: { "<year>": { "ca": <revenue>, "resultat_net": <net income> } }`
- **INPI RNE comptes annuels:** https://data.inpi.fr/ (SFTP bulk + RNE API after free account) —
  full non-confidential balance sheet + income statement since 2017 (JSON since 2023)
- **Documents et comptes:** `GET https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/documents-et-comptes-des-entreprises/records`
- **API Entreprise (restricted):** DGFIP chiffres d'affaires + Banque de France bilans (habilitation only)

## Verified live

- (2026-06-06) API Recherche d'Entreprises — HTTP 200 → `raw/api/recherche_entreprises_sample.json`
- (2026-06-06) BODACC annonces-commerciales — HTTP 200, total_count 49,386,809 → `raw/api/bodacc_annonces_sample.json`
- (2026-06-06) Sirene bulk URLs — resolved live dated URLs from data.gouv.fr API (not downloaded, large)
- (2026-06-14) Recherche API **`finances` block** — HTTP 200, real CA + résultat net → `raw/api/recherche_entreprises_finances_sample.json` (LA POSTE 2024: ca 34,569,000,000 / resultat_net 1,722,000,000)
