# Schema notes — France

## Identifiers

- **SIREN** — 9 digits, identifies a *legal unit* (the company). Primary key.
- **SIRET** — 14 digits = SIREN (9) + NIC (5), identifies an *establishment*
  (a physical site of the company). One SIREN has 1..N SIRET.
- **NIC** — last 5 digits of SIRET; the `siege` (HQ) establishment has a
  specific NIC flagged by `etablissementSiege = true`.
- **TVA intracommunautaire (VAT)** — derivable: `FR` + key(2) + SIREN.
  Not stored in Sirene; computable or from RNE.
- **NAF/APE** — activity code. Transitioning NAF Rev2 → **NAF2025**; both codes
  now present in Sirene & Recherche API (`activite_principale`,
  `activite_principale_naf25`). Keep a mapping table.

## Source field maps

### API Recherche d'Entreprises (verified sample)

Top-level company fields: `siren`, `nom_complet`, `nom_raison_sociale`, `sigle`,
`nombre_etablissements`, `nombre_etablissements_ouverts`, `nature_juridique`,
`etat_administratif` (A=active / C=cessée), `date_creation`, `dirigeants[]`,
`siege{...}`.
`siege` object: `siret`, `adresse`, `code_postal`, `commune`, `libelle_commune`,
`departement`, `activite_principale`, `activite_principale_naf25`,
`coordonnees` (lat,lon), `date_creation`, `date_mise_a_jour_insee`.

### Sirene bulk — StockUniteLegale (legal units)

`siren`, `denominationUniteLegale`, `categorieJuridiqueUniteLegale`,
`activitePrincipaleUniteLegale`, `etatAdministratifUniteLegale`,
`dateCreationUniteLegale`, `nomUniteLegale` / `prenom*` (for individuals),
`statutDiffusionUniteLegale` (O/P — diffusion opt-out), `caractereEmployeur*`,
`trancheEffectifsUniteLegale`.

### Sirene bulk — StockEtablissement (establishments)

`siren`, `siret`, `etablissementSiege`, `numeroVoieEtablissement`,
`typeVoieEtablissement`, `libelleVoieEtablissement`, `codePostalEtablissement`,
`libelleCommuneEtablissement`, `codeCommuneEtablissement`,
`activitePrincipaleEtablissement`, `etatAdministratifEtablissement`,
`dateCreationEtablissement`, geo `coordonnee*`.

### INPI RNE (legal enrichment)

Legal form, `capital`, `denomination`, commercial name, `sigle`, main activity,
`representants`/dirigeants (with roles), beneficial owners (restricted),
establishment addresses, `actes`, `comptes annuels` (non-confidential).

### BODACC (events)

`id`, `registre` (contains SIREN), `dateparution`, `familleavis_lib`
(creation / modification / radiation / procédure collective / dépôt de comptes),
`tribunal`, `commercant`, `ville`, `cp`, structured `jugement` / `depot` blocks.

## Mapping to internal company model

| Internal field        | Sirene (UniteLegale)              | Recherche API            | RNE              |
|-----------------------|-----------------------------------|--------------------------|------------------|
| company_id            | siren                             | siren                    | siren            |
| registration_number   | siren                             | siren                    | siren            |
| tax_id / vat_id       | (compute FR VAT from siren)       | (compute)                | tva if present   |
| legal_name            | denominationUniteLegale           | nom_complet              | denomination     |
| normalized_name       | (lowercase/strip)                 | nom_raison_sociale       | denomination     |
| company_type          | categorieJuridiqueUniteLegale     | nature_juridique         | formeJuridique   |
| status                | etatAdministratifUniteLegale (A/C)| etat_administratif       | etat             |
| incorporation_date    | dateCreationUniteLegale           | date_creation            | dateImmatric.    |
| dissolution_date      | (from periodes / etat=C)          | date_fermeture (siege)   | dateRadiation    |
| registered_address    | siege establishment address       | siege.adresse            | siege address    |
| municipality          | libelleCommuneEtablissement       | siege.libelle_commune    | commune          |
| region/department     | code departement                  | siege.departement        | departement      |
| country               | "France"                          | "France"                 | "France"         |
| source_url/name/at    | dataset URL + retrieved_at        | endpoint + retrieved_at  | feed + retrieved |
| raw_record            | full row                          | full JSON object         | full JSON        |

## Encoding / formats

- Sirene CSV: UTF-8, comma-separated, header row. Parquet variant recommended
  for analytics loads (smaller, typed).
- Dates: ISO `YYYY-MM-DD`.
- `etat_administratif`: `A` = active, `C` = ceased/closed.
- `statutDiffusion`: respect `P` (partial/opted-out individuals) — mask PII.
