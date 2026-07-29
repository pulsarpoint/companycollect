# Search attempts — France

## Attempt 1

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `INSEE SIRENE base entreprises téléchargement données data.gouv.fr bulk download`
- Language: French/English
- Why this query was tried: Locate the authoritative national business register bulk download.
- Top relevant URLs:
  - https://www.insee.fr/fr/information/3591226
  - https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret
  - https://www.sirene.fr/sirene/public/static/open-data
- Result: Confirmed Sirene open data with 5 stock files (legal units, establishments, historic, succession links).
- Decision: Mark Sirene bulk as primary recommended source; resolve concrete file URLs.

## Attempt 2

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `API Sirene INSEE portail-api.insee.fr documentation entreprises`
- Language: French/English
- Why this query was tried: Find the official daily-updated registry API.
- Top relevant URLs:
  - https://portail-api.insee.fr/
  - https://portail-api.insee.fr/catalog/api/2ba0e549-5587-3ef1-9082-99cd865de66f/doc
- Result: Confirmed API Sirene on new portal; requires free account + subscription for key.
- Decision: Recommended for daily deltas; auth required, no live call without credentials.

## Attempt 3

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `INPI RNE Registre National des Entreprises données ouvertes API data.inpi.fr`
- Language: French/English
- Why this query was tried: Find the legal register (capital, dirigeants, accounts).
- Top relevant URLs:
  - https://data.inpi.fr/
  - https://www.inpi.fr/ressources/formalites-dentreprises/registre-national-entreprises
- Result: RNE confirmed; bulk via SFTP after free registration + JSON/PDF API, daily.
- Decision: Recommended for legal enrichment.

## Attempt 4

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `recherche-entreprises.api.gouv.fr annuaire entreprises API DINUM documentation`
- Language: English
- Why this query was tried: Find a no-auth aggregator API for quick prototyping.
- Top relevant URLs:
  - https://www.data.gouv.fr/dataservices/api-recherche-dentreprises
  - https://recherche-entreprises.api.gouv.fr/search
  - https://github.com/annuaire-entreprises-data-gouv-fr/search-api
- Result: Confirmed public no-auth search API (Elasticsearch over INSEE+INPI+...).
- Decision: Recommended entry point; tested live (see Attempt 6).

## Attempt 5

- Date/time: 2026-06-06
- Search engine or source: WebSearch
- Query: `BODACC data.gouv.fr open data annonces commerciales API export JSON`
- Language: French/English
- Why this query was tried: Find the official gazette / company lifecycle event feed.
- Top relevant URLs:
  - https://bodacc-datadila.opendatasoft.com/explore/dataset/annonces-commerciales/
  - https://www.data.gouv.fr/datasets/bodacc
- Result: BODACC via Opendatasoft, Licence Ouverte 2.0, JSON/CSV/XLSX export, no auth.
- Decision: Useful secondary source for change detection; tested live (Attempt 6).

## Attempt 6 (live verification)

- Date/time: 2026-06-06
- Source: direct HTTPS calls (curl) + data.gouv.fr dataset API (WebFetch)
- Queries:
  - `GET https://recherche-entreprises.api.gouv.fr/search?q=la%20poste&per_page=2` → HTTP 200, saved
  - `GET https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records?limit=2` → HTTP 200, total_count 49,386,809, saved
  - data.gouv.fr dataset `5b7ffc618b4c4169d30727e0` → resolved live Sirene stock file URLs + sizes
- Result: Two no-auth APIs verified returning real data; Sirene bulk URLs resolved.
- Decision: Save samples to `raw/api/`, build normalized sample, record sizes (bulk not downloaded).

---

# Financial-data follow-up (2026-06-14)

## Attempt 7
- Date/time: 2026-06-14
- Source: WebSearch (x2)
- Queries: `INPI RNE comptes annuels open data bulk download bilans data.gouv.fr`;
  `recherche-entreprises API finances chiffre d'affaires résultat net annuaire entreprises`
- Language: French/English
- Why: Determine whether French financials are open and how to access them.
- Result: INPI RNE publishes **non-confidential comptes annuels** (bilan + compte de résultat + immobilisations/amortissements/provisions) since 2017 (JSON since 2023) via data.inpi.fr SFTP/API. The Recherche API and API Entreprise both surface financials; API Entreprise (restricted) brokers DGFIP CA + Banque de France bilans.
- Decision: Treat INPI RNE comptes annuels as the open full-statement source; probe the Recherche API for an open finances block.

## Attempt 8 (live verification)
- Date/time: 2026-06-14
- Source: curl `GET https://recherche-entreprises.api.gouv.fr/search?q=la%20poste&per_page=3`
- Result: HTTP 200. Each result carries a **`finances`** object: `{"2024":{"ca":34569000000,"resultat_net":1722000000}}` for SIREN 356000000 LA POSTE (other matches `null` = confidential/none). Saved to `raw/api/recherche_entreprises_finances_sample.json`.
- Decision: Catalog the open `finances` block (no auth) as the headline-financials source; INPI RNE for full statements. Record confidentiality caveat.

## Attempt 9
- Date/time: 2026-06-14
- Source: WebFetch — data.economie.gouv.fr "Documents et comptes des entreprises"; data.gouv.fr RNE dataset
- Result: RNE confirms non-confidential annual-accounts data (balance sheet, income statement, fixed assets, depreciation, provisions) since 2017-01-01, JSON since 2023. "Documents et comptes" dataset under Open Licence 2.0 (document links + metadata + confidentiality).
- Decision: Add INPI comptes annuels + Documents-et-comptes to the inventory; document the confidentiality coverage limit.

---

# Current revalidation and expansion (2026-07-28)

## Attempt 10 — public structured financial ratios

- Source/query: official data.gouv.fr and data.economie.gouv.fr search for
  `ratios financiers BCE INPI`.
- URL: https://www.data.gouv.fr/datasets/ratios-financiers-bce-inpi
- Live call: `GET /api/explore/v2.1/catalog/datasets/ratios_inpi_bce/records?where=siren="356000000"&limit=20`
- Result: HTTP 200, 6,542,232 total records and 17 La Poste rows. Revenue,
  margin, EBE, EBIT, net income and many ratios are structured and public.
- Decision: Make this the first recommended financial ingestion. Key by SIREN,
  closing date and `type_bilan`.

## Attempt 11 — full financial statements without authentication

- Source/query: official data.gouv.fr search for detailed company financial
  data in Parquet.
- URL: https://www.data.gouv.fr/datasets/donnees-financieres-detaillees-des-entreprises-format-parquet
- Result: official Open Licence 2.0 Parquet, 2,820,473,022 bytes, containing
  detailed 2033/2050 statement data.
- Decision: Recommend when full line-item detail is needed; do not download
  during research because of size.

## Attempt 12 — daily enriched company bulk

- Source/query: official Annuaire des Entreprises bulk-data page.
- URL: https://www.data.gouv.fr/datasets/donnees-des-entreprises-utilisees-dans-lannuaire-des-entreprises
- Result: daily legal-unit and establishment Parquet files with many official
  enrichment flags. Observed sizes were about 1.14 GB and 1.68 GB.
- Decision: Recommend as the most efficient non-financial enrichment feed.

## Attempt 13 — INPI account data and confidentiality

- Source: https://data.inpi.fr/content/editorial/Acces_API_Entreprises
- Result: INPI documents daily API/SFTP access to non-confidential annual
  accounts since 2017 and reports approximately 1.5M filings/year with about
  45% confidentiality.
- Decision: Keep as authoritative full-filing source but explicitly model
  confidentiality and account setup.

## Attempt 14 — ESG and issuer notices (live verification)

- Calls:
  - `GET https://data.ademe.fr/data-fair/api/v1/datasets/bilan-ges/lines?size=2`
  - `GET https://journal-officiel-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/balo/records?limit=2`
- Result: HTTP 200; BEGES reported 11,620 records and BALO 147,849 notices.
  Both returned SIREN-linked records.
- Decision: Add BEGES as ESG facts and BALO as a specialist event/notices feed.

## Attempt 15 — restricted data checks

- Sources:
  - https://www.data.gouv.fr/dataservices/api-registre-des-beneficiaires-effectifs-rbe
  - https://entreprendre.service-public.fr/actualites/A17554
  - https://www.data.gouv.fr/dataservices/api-entreprise
- Result: RBE access requires authorization or legitimate interest; API
  Entreprise requires habilitation.
- Decision: Record both for completeness and exclude them from general open
  ingestion.
