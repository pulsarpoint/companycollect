# VIES — EU VAT validation (HU VAT) Field Catalog

## Source Summary

- Country: Hungary
- Source type: official_tax
- Organization: European Commission / NAV
- URL: https://ec.europa.eu/taxation_customs/vies/services/checkVatService (SOAP)
- License: validation only
- Access: public
- Freshness: real-time
- Record shape: per-VAT validation
- Primary keys: none
- Join keys: `vat_id`, `adoszam`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| vatNumber | vatNumber | HU EU VAT | string | identifier | HU10841713 | HU + 8-digit base |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation only.** VIES validates a Hungarian EU VAT number (**közösségi adószám** = `HU` + the 8-digit tax
  base / törzsszám) and may return name/address. It does **not** enumerate companies and is not redistributable
  as a list. Enrichment for a known adószám/VAT.
