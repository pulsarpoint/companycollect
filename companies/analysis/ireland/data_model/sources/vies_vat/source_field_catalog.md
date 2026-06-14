# Revenue / VIES — VAT validation (IE VAT) Field Catalog

## Source Summary

- Country: Ireland
- Source type: official_tax
- Organization: Revenue Commissioners / European Commission
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
| vatNumber | vatNumber | IE VAT number | string | identifier | IE1234567T | not in CRO data |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation only; fills the VAT gap.** The Irish VAT number (`IE` + 7 digits + 1–2 letters) is **not** in the
  CRO open data, so VAT must be sourced separately. VIES validates a supplied number and may return name/address;
  it does **not** enumerate companies and is not redistributable as a list. There is **no open CRO↔VAT
  crosswalk** — associate VAT to the CRO number via name/address matching or a commercial provider.
