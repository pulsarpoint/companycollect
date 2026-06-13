# BODACC — Annonces Commerciales — Field Catalog

## Source Summary

- Country: France
- Source type: official_gazette_api (event stream)
- Organization: DILA
- URL: `https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records`
- License: **Licence Ouverte / Open Licence v2.0**
- Access: public, no auth (Opendatasoft quota)
- Freshness: daily; ~49.4M records
- Record shape: Opendatasoft v2.1 `{ results: [...], total_count }`
- Primary keys: `id`
- Join keys: `registre` (contains SIREN)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| results[].id | id | Announcement id | string | identifier | — | PK |
| results[].registre | registre | RCS ref (contains SIREN) | string | identifier | — | extract SIREN to join |
| results[].familleavis_lib | familleavis_lib | Announcement type | string | filing | `Création`,`Radiation`,`Dépôt des comptes` | event type |
| results[].dateparution | dateparution | Publication date | date | date | — | event date |
| results[].tribunal | tribunal | Court/greffe | string | metadata | — | |
| results[].commercant | commercant | Company/trader name | string | legal_name | — | |
| results[].ville | ville | City | string | geography | — | |
| results[].cp | cp | Postal code | string | address | — | |
| results[].jugement | jugement | Judgment block | object | filing | — | insolvency details |
| results[].depot | depot | Accounts-filing block | object | filing | — | dépôt des comptes signal |
| results[].publicationavis | publicationavis | Bulletin ref | string | metadata | — | |

## Interpretation Notes

- **The lifecycle event stream**, not a master list. Best for **change detection**: créations,
  modifications, radiations, **procédures collectives** (insolvency), **dépôts de comptes**, ventes/cessions.
- **Join via SIREN** parsed out of `registre`. Attach events to the Sirene spine.
- **`depot` (dépôt des comptes)** announces that accounts were filed — a **trigger to refresh financials**
  for that SIREN (the figures live in INPI comptes annuels, not here).
- **`jugement`** carries insolvency/safeguard/liquidation detail — a strong **status** signal
  (procédure collective → distressed/closing).
- Verified live 2026-06-06 (total_count 49,386,809). No `sample_record.json` retained here (sample not in
  the current data folder); structure from `schema_notes.md` + the inventory.
