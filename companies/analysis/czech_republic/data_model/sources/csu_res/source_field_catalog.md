# RES — Registr ekonomických subjektů (ČSÚ) Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: statistical_business_register
- Organization: Český statistický úřad (ČSÚ)
- URL: https://www.czso.cz/csu/res/registr_ekonomickych_subjektu
- License: Open data (ČSÚ; attribute)
- Access: public
- Freshness: regular
- Record shape: statistical register record keyed by IČO
- Primary keys: `ico`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ico | IČO | Company id | string | identifier | — | join key |
| cz_nace | CZ-NACE | Primary activity | string | activity | — | primary flagged |
| institucionalni_sektor | institucionální sektor | Institutional sector | string | metadata | — | ESA |
| kategorie_poctu_zamestnancu | kategorie počtu zaměstnanců | Employee size band | string | employment | — | banded only |

## Interpretation Notes

- The statistical register feeding ARES (`stavZdrojeRes`). Its distinctive value over ARES/Justice is the
  **primary CZ-NACE activity**, the **institutional sector**, and the **employee-size band** — useful for
  segmentation. Exact employee headcount is **not** in open data (bands only). Join on IČO.
