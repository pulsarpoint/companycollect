# AED / VIES — VAT validation (LU VAT) Field Catalog

## Source Summary

- Country: Luxembourg
- Source type: official_tax
- Organization: AED (Administration de l'enregistrement, des domaines et de la TVA) / European Commission
- URL: https://ec.europa.eu/taxation_customs/vies/services/checkVatService (SOAP)
- License: validation only
- Access: public
- Freshness: real-time
- Record shape: per-VAT validation
- Primary keys: none
- Join keys: `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| vatNumber | vatNumber | LU VAT number | string | identifier | LU12345678 | LU + 8 digits |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation only; fills the VAT gap.** The Luxembourg VAT number (`LU` + 8 digits) is **separate** from the
  RCS number and the matricule, and is **not** in the free RCS data. VIES validates a supplied number and may
  return name/address; it does **not** enumerate companies and is not redistributable as a list. There is **no
  open RCS↔VAT crosswalk** — associate VAT to the RCS number via name matching or a commercial provider.
