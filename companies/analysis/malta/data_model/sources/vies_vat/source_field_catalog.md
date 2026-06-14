# CFR / VIES — VAT validation (MT VAT) Field Catalog

## Source Summary

- Country: Malta
- Source type: official_tax
- Organization: Commissioner for Revenue (CFR) / European Commission
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
| vatNumber | vatNumber | MT VAT number | string | identifier | MT12345678 | MT + 8 digits |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation only; fills the VAT gap.** The Maltese VAT number (`MT` + 8 digits) is **separate** from the
  registration number and is **not** in the free register data. VIES validates a supplied number and may return
  name/address; it does **not** enumerate companies and is not redistributable as a list. There is **no open
  registration-number↔VAT crosswalk** — associate VAT via name matching, the paid MBR API, or a commercial
  provider.
