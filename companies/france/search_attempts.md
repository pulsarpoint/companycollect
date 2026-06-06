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
