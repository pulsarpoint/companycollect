# VIES — EU VAT validation (EL VAT) Field Catalog

## Source Summary

- Country: Greece
- Source type: official_tax
- Organization: European Commission / AADE
- URL: https://ec.europa.eu/taxation_customs/vies/services/checkVatService (SOAP)
- License: validation only
- Access: public
- Freshness: real-time
- Record shape: per-VAT validation
- Primary keys: none
- Join keys: `afm`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| vatNumber | vatNumber | EL VAT number | string | identifier | EL123456789 | EL + ΑΦΜ |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation only.** VIES validates a Greek VAT number (`EL` + the 9-digit ΑΦΜ) and may return name/address.
  Verified reachable (HTTP 405 = needs SOAP POST). It does **not** enumerate companies and is not redistributable
  as a list. Enrichment for a known ΑΦΜ/VAT.
