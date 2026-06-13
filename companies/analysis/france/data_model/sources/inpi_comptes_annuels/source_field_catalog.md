# INPI RNE — Comptes Annuels (Annual Accounts) — Field Catalog

> **OPEN** full financial statements for **non-confidential** filings since 2017 (JSON since 2023).
> Requires a free INPI account. Field names are derived from the documented RNE comptes-annuels structure
> and the French liasse fiscale / PCG; exact JSON paths to be confirmed against a live record.

## Source Summary

- Country: France
- Source type: official_financial_disclosure
- Organization: INPI
- URL: https://data.inpi.fr/ (SFTP bulk + RNE API; free account)
- License: open data (INPI conditions); **non-confidential filings only**
- Access: public with free account (auth)
- Freshness: daily; filings since 2017-01-01
- Record shape: per-company per-exercice (bilan + compte de résultat), JSON; older as PDF
- Primary keys: `siren + dateClotureExercice`
- Join keys: `siren`

## Fields

| Path | Source field (FR) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| siren | siren | Company id | string | identifier | join |
| bilan.dateClotureExercice | dateClotureExercice | Fiscal year-end | date | date | per-statement key |
| typeBilan | typeBilan | complet/simplifié/consolidé | string | filing | drives nullability |
| confidentialite | confidentialite | Confidential flag | boolean | metadata | excluded if true |
| bilan.actif.totalActif | total actif | Total assets | decimal | financial | EUR |
| bilan.actif.immobilisations | immobilisations | Fixed assets | decimal | financial | |
| bilan.actif.actifCirculant | actif circulant | Current assets | decimal | financial | |
| bilan.passif.capitauxPropres | capitaux propres | Equity | decimal | financial | |
| bilan.passif.dettes | dettes | Liabilities | decimal | financial | |
| compteResultat.chiffreAffaires | chiffre d'affaires | Revenue | decimal | financial | full figure behind API `ca` |
| compteResultat.resultatExploitation | résultat d'exploitation | Operating result | decimal | financial | |
| compteResultat.resultatNet | résultat net | Net income | decimal | financial | = API `resultat_net` |
| amortissements | amortissements/provisions | Depreciation/provisions | decimal | financial | |
| deviseComptes | devise | Currency | string | financial | usually EUR |

## Interpretation Notes

- **The full-statement source.** Where the Recherche API gives only `ca` + `resultat_net`, INPI comptes
  annuels gives the **full balance sheet + income statement** (per *poste* of the liasse fiscale), plus
  fixed assets, depreciation and provisions, for **non-confidential** filings since 2017.
- **Confidentiality is the coverage limit.** Micro/small companies may file **confidential** accounts
  (`confidentialite=true`) — excluded from the open figures. `typeBilan=simplifié` discloses fewer lines.
  Model financial fields as **nullable**.
- **Two-figure consistency.** `chiffreAffaires` and `resultatNet` here are the same concepts the
  aggregator surfaces as `ca`/`resultat_net` — use this source when you need more than the two headline
  figures.
- **Currency** usually EUR; store it per statement.
- No `sample_record.json` (free INPI account required; auth not bypassed). Confirm exact JSON paths and
  poste codes against a live RNE comptes-annuels record before finalizing the parser.
