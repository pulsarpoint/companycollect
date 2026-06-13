# RNE — Registre National des Entreprises (Data INPI) — Field Catalog

## Source Summary

- Country: France
- Source type: official_registry_bulk_and_api (legal register)
- Organization: INPI
- URL: https://data.inpi.fr/ (SFTP bulk + RNE API after free account)
- License: open data (INPI conditions of reuse); **beneficial ownership (RBE) restricted**
- Access: public with **free account** (auth)
- Freshness: daily; JSON
- Record shape: nested JSON per company (formality → identité / représentants / composition / comptes)
- Primary keys: `siren`
- Join keys: `siren`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| siren | siren | Company id | string | identifier | join to spine |
| …identite…denomination | denomination | Legal name | string | legal_name | nested |
| …formeJuridique | formeJuridique | Legal form | string | legal_form | |
| …montantCapital | montantCapital | **Share capital** | decimal | financial | register capital, not accounts |
| …capitalVariable | capitalVariable | Variable capital | boolean | financial | |
| composition.pouvoirs[] | representants | Directors | array | person | **PII**; roles |
| beneficiairesEffectifs[] | RBE | Beneficial owners | array | ownership | **RESTRICTED — planning-only** |
| …objet | objet | Purpose (objet social) | string | activity | free text |
| …dateImmatriculation | dateImmatriculation | Registration date | date | date | |
| actes[] | actes | Acts/statutes docs | array | document | PDF, since 1993 |
| comptesAnnuels[] | comptes annuels | Annual-accounts refs | array | filing | figures → inpi_comptes_annuels |
| etablissements[] | etablissements | Establishments | array | address | overlaps Sirene |

## Interpretation Notes

- **The legal register** — the richest *legal* identity beyond Sirene: **share capital**, **directors
  with roles**, corporate purpose, acts/statutes, and references to annual accounts. Joins to everything
  on `siren`.
- **Beneficial ownership (RBE)** is **restricted/regulated** (post-2022 CJEU ruling) — **not** in the
  open feed; cataloged as **planning-only**, do not ingest as open.
- **PII**: représentants/dirigeants and (where applicable) beneficial owners are natural persons — GDPR.
- **Nested JSON**: exact paths vary by personne morale vs personne physique and by formality version —
  verify against a live RNE record before finalizing the parser. No `sample_record.json` here (requires a
  free INPI account; not fetched — auth not bypassed).
- **Format note**: RNE moved to JSON; older data and documents (actes/comptes) are PDF.
