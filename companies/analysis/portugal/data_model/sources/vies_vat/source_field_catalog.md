# AT / VIES — VAT validation (PT NIF) Field Catalog

## Source Summary

- Country: Portugal
- Source type: official_tax
- Organization: Autoridade Tributária e Aduaneira (AT) / European Commission
- URL: https://ec.europa.eu/taxation_customs/vies/services/checkVatService (SOAP)
- License: validation only
- Access: public
- Freshness: real-time
- Record shape: per-VAT validation
- Primary keys: none
- Join keys: `nipc`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| vatNumber | vatNumber | PT VAT/NIF | string | identifier | PT500000000 | PT + NIPC |
| valid | valid | Validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | may be suppressed |

## Interpretation Notes

- **Validation + a free name signal.** The Portuguese VAT/NIF for companies is simply `PT` + the 9-digit
  **NIPC** (the NIF equals the NIPC). VIES validates a supplied number and **may return name/address** — a free
  way to confirm a company's name for a **known NIPC** (the register itself is paid). It does not enumerate
  companies and is not redistributable as a list. Join on `nipc`/`vat_id`.
