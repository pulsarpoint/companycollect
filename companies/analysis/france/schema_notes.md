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

### Financials — Recherche API `finances` block (verified)

```
finances : object keyed by year, e.g.
  "finances": { "2024": { "ca": 34569000000, "resultat_net": 1722000000 } }
    ca           integer  - chiffre d'affaires (revenue), EUR
    resultat_net integer  - résultat net (net income), EUR; negative = loss
```
- Only these two figures; only years with a non-confidential filing. `null`/absent when confidential
  or not filed. Source: derived from INPI RNE comptes annuels.

### Financials — INPI RNE comptes annuels (full statements)

Non-confidential annual accounts since 2017 (JSON since 2023). Expected concepts:
```
exercice (year), type de comptes (complet / simplifié / consolidé), confidentialité (bool)
bilan:   actif (total, immobilisations, actif circulant), passif (capitaux propres, dettes)
compte de résultat: chiffre d'affaires, résultat d'exploitation, résultat net
immobilisations, amortissements, provisions
```
- Coverage partial: micro/small firms may file **confidential** accounts (excluded). Currency EUR.

### Financials — Ratios BCE/INPI (verified 2026-07-28)

Primary grain:

```text
(siren, date_cloture_exercice, type_bilan)
```

`type_bilan` is required because one company/date can have multiple statements.
Observed values include `C` (complete), `S` (simplified) and `K`
(consolidated). Preserve the source code until an authoritative code list is
loaded.

Observed measures:

```text
chiffre_d_affaires, marge_brute, ebe, ebit, resultat_net
taux_d_endettement, ratio_de_liquidite, ratio_de_vetuste
autonomie_financiere, poids_bfr_exploitation_sur_ca
couverture_des_interets, caf_sur_ca, capacite_de_remboursement
marge_ebe, resultat_courant_avant_impots_sur_ca
poids_bfr_exploitation_sur_ca_jours, rotation_des_stocks_jours
credit_clients_jours, credit_fournisseurs_jours, confidentiality
```

Amounts are EUR. Ratio units must be documented per field before exposing them
in product UI; do not assume every numeric ratio is a percentage.

### Detailed financial Parquet

The observed 2.82 GB Parquet resource uses SIREN, closing date and balance type
plus an associative collection of detailed base tax-form 2033/2050 fields.
Ingest the source in long form first (`statement_key`, `field_code`, `value`)
unless a stable code dictionary justifies a wide ClickHouse table.

### Annuaire enriched bulk and API complements

Useful legal-unit flags include association/RNA, ESS, société à mission,
EGAPRO, responsible procurement, Alim'Confiance, training organization,
Qualiopi, SIAE, FINESS, ADEME aid, lawyers, collective agreements and
patrimoine vivant. Establishment-level lists/flags include FINESS, BIO, RGE and
UAI. Model evidence-bearing identifiers as arrays rather than only booleans.

### ADEME BEGES and BALO

- BEGES key candidates: `(siren_principal, id)` with `siret`, reporting year,
  emissions categories/scopes, reduction targets and actions.
- BALO key: `id_annonce`; `siren` is an array because one notice can reference
  multiple companies. Keep notice category, publication date, PDF link and
  full text separately.

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
| financials[]          | —                                 | finances{} (ca, résultat net) | comptes annuels (full bilan + résultat) |
| financial ratios      | —                                 | —                        | Ratios BCE/INPI keyed by date + type |
| enrichment flags      | —                                 | complements{}            | Annuaire bulk fields |
| raw_record            | full row                          | full JSON object         | full JSON        |

## Encoding / formats

- Sirene CSV: UTF-8, comma-separated, header row. Parquet variant recommended
  for analytics loads (smaller, typed).
- Dates: ISO `YYYY-MM-DD`.
- `etat_administratif`: `A` = active, `C` = ceased/closed.
- `statutDiffusion`: respect `P` (partial/opted-out individuals) — mask PII.
