# Source inventory — France

Revalidated 2026-07-28. The machine-readable version is
[`source_inventory.json`](source_inventory.json).

| Source | Adds | Access | License | Recommendation |
|---|---|---|---|---|
| INSEE Base Sirene | Company and establishment spine | Public bulk; API needs free key | Open Licence 2.0 | Keep as primary |
| Ratios financiers BCE/INPI | CA, margin, EBE, EBIT, net income, 15+ ratios | Public API/export, no auth | Open Licence 2.0 | **Add first** |
| Detailed financial Parquet | Detailed 2033/2050 statement fields | Public 2.82 GB file | Open Licence 2.0 | Add when full detail is needed |
| INPI RNE annual accounts | Authoritative filings and full non-confidential statements | Free account, API/SFTP | INPI open-data terms | Add after account setup |
| API Recherche d'Entreprises | Headline CA/net income and many flags | Public search API, 7 req/s | Source-specific open terms | Lookup/enrichment only |
| Annuaire enriched bulk | Association, ESS, mission, EGAPRO, Qualiopi, RGE, FINESS, BIO, SIAE and more | Public daily Parquet | Open Licence 2.0 | **Add for enrichment** |
| INPI RNE legal data | Capital, officers, acts/statutes, establishments | Free account, API/SFTP | INPI open-data terms | Recommended |
| BODACC | Creation/change/closure/insolvency/account-deposit events | Public API/export | Open Licence 2.0 | Recommended event feed |
| ADEME BEGES | Emissions, reporting year, targets/actions | Public API/export | Open Licence 2.0 | Useful ESG fact table |
| BALO | Issuer, bank and public-offering notices | Public API/export | Open Licence 2.0 | Useful specialist feed |
| INPI IP APIs | Trademarks, patents and designs | Free account | INPI reuse terms | Useful specialist feed |
| API Entreprise | DGFIP CA and Banque de France balances | Habilitation required | Restricted | Do not use generally |
| RBE beneficial owners | Ownership/control | Authorized/legitimate-interest access | Restricted by law | Do not ingest openly |

## Financial endpoints

- Ratios API:
  `https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/ratios_inpi_bce/records`
- Ratios Parquet export:
  `https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/ratios_inpi_bce/exports/parquet`
- Detailed financial Parquet dataset:
  `https://www.data.gouv.fr/datasets/donnees-financieres-detaillees-des-entreprises-format-parquet`
- Recherche API:
  `GET https://recherche-entreprises.api.gouv.fr/search?q=<SIREN>&per_page=1`
- INPI enterprise data/API/SFTP:
  `https://data.inpi.fr/content/editorial/Acces_API_Entreprises`

## Other-data endpoints

- Annuaire enriched bulk:
  `https://www.data.gouv.fr/datasets/donnees-des-entreprises-utilisees-dans-lannuaire-des-entreprises`
- BODACC:
  `https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records`
- ADEME BEGES:
  `https://data.ademe.fr/data-fair/api/v1/datasets/bilan-ges/lines`
- BALO:
  `https://journal-officiel-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/balo/records`
- INPI intellectual property:
  `https://data.inpi.fr/content/editorial/apis_pi`

## Verified live

- 2026-07-28: Recherche API returned current La Poste registry, enrichment and
  `finances` data.
- 2026-07-28: Ratios BCE/INPI returned 17 rows for SIREN `356000000`; the
  complete and consolidated rows prove that `type_bilan` is part of the key.
- 2026-07-28: ADEME BEGES returned 11,620 total records.
- 2026-07-28: BALO returned 147,849 total notices.

Raw responses and SHA-256 metadata are in
`../../data/france/raw/api/`.
