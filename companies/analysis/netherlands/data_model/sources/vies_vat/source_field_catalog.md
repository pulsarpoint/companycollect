# Belastingdienst / VIES — VAT validation (NL btw-nummer) Field Catalog

## Source Summary

- Country: Netherlands
- Source type: official_tax
- Organization: Belastingdienst / European Commission
- URL: https://ec.europa.eu/taxation_customs/vies/services/checkVatService (SOAP)
- License: validation only
- Access: public
- Freshness: real-time
- Record shape: per-VAT validation
- Primary keys: none
- Join keys: `vat_id`, `rsin`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| vatNumber | vatNumber | NL btw-nummer | string | identifier | NL123456789B01 | NL + RSIN + B + 2 |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation / derivation.** The Dutch VAT number (**btw-nummer**) is `NL` + 9 digits + `B` + 2-digit suffix;
  for legal entities the 9 digits equal the **RSIN**, so VAT is **derivable** once you have the RSIN (from the
  paid KvK API). VIES validates a supplied number and may return name/address; it does not enumerate companies
  and is not redistributable as a list. Join on `rsin`/`vat_id`.
