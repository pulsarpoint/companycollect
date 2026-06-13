# Base Sirene (SIREN/SIRET) — Field Catalog

## Source Summary

- Country: France
- Source type: official_registry_bulk
- Organization: INSEE
- URL: https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret
- License: **ODbL** (attribution + share-alike on derived databases)
- Access: public, no auth (bulk); daily deltas via API Sirene (free key)
- Freshness: monthly stock (01 of month) + daily API
- Record shape: **StockUniteLegale** (legal units, ~25M) + **StockEtablissement** (establishments, ~36M); CSV.zip + Parquet
- Primary keys: `siren` (unit), `siret` (establishment)
- Join keys: `siren`, `siret`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| UniteLegale.siren | siren | Company id (9) | string | identifier | `356000000` | spine PK |
| UniteLegale.denominationUniteLegale | denominationUniteLegale | Legal name | string | legal_name | `LA POSTE` | null for individuals |
| UniteLegale.categorieJuridiqueUniteLegale | …categorieJuridique… | Legal form code | string | legal_form | `5510` | label table |
| UniteLegale.activitePrincipaleUniteLegale | …activitePrincipale… | NAF/APE | string | activity | `53.10Z` | Rev2/NAF2025 |
| UniteLegale.etatAdministratifUniteLegale | …etatAdministratif… | A/C | string | status | `A` | |
| UniteLegale.dateCreationUniteLegale | …dateCreation… | Creation date | date | date | `1991-01-01` | incorporation |
| UniteLegale.trancheEffectifsUniteLegale | …trancheEffectifs… | Employee band | string | employment | `53` | band only |
| UniteLegale.statutDiffusionUniteLegale | …statutDiffusion… | O/P | string | metadata | `O` | P → mask PII |
| UniteLegale.categorieEntreprise | categorieEntreprise | PME/ETI/GE | string | metadata | `GE` | |
| Etablissement.siret | siret | Establishment id (14) | string | identifier | — | SIREN+NIC |
| Etablissement.etablissementSiege | etablissementSiege | Is head office | boolean | metadata | `true` | registered address |
| Etablissement.libelleVoieEtablissement | numeroVoie/typeVoie/libelleVoie | Street | string | address | — | concat cols |
| Etablissement.codePostalEtablissement | codePostal… | Postal code | string | address | — | |
| Etablissement.libelleCommuneEtablissement | libelleCommune… | Commune | string | geography | — | |
| Etablissement.codeCommuneEtablissement | codeCommune… | INSEE commune code | string | geography | — | dép = first 2 |
| Etablissement.etatAdministratifEtablissement | …etatAdministratif… | A/F | string | status | `A` | |

## Interpretation Notes

- **The spine.** Sirene assigns the **SIREN** (legal unit) and **SIRET** (establishment) that every
  other French source keys on. Two stock files: legal units + establishments (join on `siren`); pick
  `etablissementSiege=true` for the registered address.
- **Codes need INSEE label tables**: `categorieJuridique`, `trancheEffectifs`, NAF (Rev2 + NAF2025).
- **No financials, no capital, no directors** here — those come from INPI RNE / the Recherche API.
- **ODbL share-alike**: a publicly redistributed derived *database* must be ODbL + credit INSEE.
- **Diffusion**: honor `statutDiffusionUniteLegale=P` (opted-out individual entrepreneurs) — mask PII.
- No `sample_record.json`: bulk not downloaded (multi-GB); field list is from the documented Sirene
  schema (`schema_notes.md`). Use the Parquet variant for analytics loads.
