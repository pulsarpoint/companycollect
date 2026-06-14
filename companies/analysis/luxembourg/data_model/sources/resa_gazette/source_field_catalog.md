# RESA — Recueil Électronique des Sociétés et Associations (legal gazette) Field Catalog

## Source Summary

- Country: Luxembourg
- Source type: official_gazette
- Organization: Luxembourg Business Registers (LBR)
- URL: https://www.lbr.lu/resa/
- License: public (free)
- Access: public
- Freshness: continuous
- Record shape: legal-act publications referencing the RCS number
- Primary keys: `publication_id`
- Join keys: `rcs_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| rcs_number | Numéro RCS | Company id | string | identifier | — | join key |
| publication_type | type de publication | Act type | string | filing | — | incorporation/amendment/accounts/dissolution |
| publication_date | date de publication | Publication date | date | date | — | timeline |

## Interpretation Notes

- **Events/history, not a structured master.** RESA (which replaced the Mémorial C) is the **electronic gazette**
  of company legal publications — incorporations, statute amendments, **accounts deposits**, dissolutions —
  each referencing the **RCS number**. Verified reachable. Free to consult. Useful to track lifecycle events and
  corroborate filings; it is not a structured company master and is consulted via the LBR site. Join on
  rcs_number.
