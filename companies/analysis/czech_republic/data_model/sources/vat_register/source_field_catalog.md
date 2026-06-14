# Registr plátců DPH / VAT payer register (Finanční správa + VIES) Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: official_tax
- Organization: Finanční správa ČR / EU VIES
- URL: https://adisspr.mfcr.cz/dpr/DphReg
- License: validation only (not redistributable as a list)
- Access: public
- Freshness: real-time
- Record shape: per-DIČ lookup/validation
- Primary keys: none (not a master)
- Join keys: `ico`, `dic`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| dic | DIČ | VAT/tax id | string | identifier | CZ27082440 | CZ + IČO |
| vat_status | stav plátce DPH | VAT registration validity | string | status | — | VIES |
| nespolehlivy_platce | nespolehlivý plátce | Unreliable-payer flag | boolean | status | — | CZ risk signal |
| zverejnene_ucty | zveřejněné účty | Published bank accounts | array | raw_extension | — | tax context |

## Interpretation Notes

- **Enrichment/validation, not a master.** Use it to confirm a DIČ (CZ + IČO) and VAT registration, and to pick
  up the **"unreliable VAT payer" (nespolehlivý plátce)** flag — a distinctive Czech risk signal — plus any
  **published bank accounts**. VIES validates a supplied number; neither endpoint enumerates companies, and the
  data is not redistributable as a bulk list. Join on IČO/DIČ.
