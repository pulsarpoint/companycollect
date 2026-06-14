# Tax Department — TIC / VAT Field Catalog

## Source Summary

- Country: Cyprus
- Source type: official_tax
- Organization: Tax Department (Τμήμα Φορολογίας)
- URL: https://www.mof.gov.cy/mof/tax/taxdep.nsf (VAT validation via VIES)
- License: validation only — not redistributable as a list
- Access: public
- Freshness: real-time validation
- Record shape: per-identifier lookup/validation
- Primary keys: none (not a master)
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| tic | TIC | Tax Identification Code (tax id) | string | identifier | (none) | separate from HE and VAT |
| vat_number | VAT number | CY + 8 digits + letter | string | identifier | CY12345678X | validate via VIES |
| vat_status | VAT status | VIES validity | string | status | valid | point-in-time |

## Interpretation Notes

- **Three identifiers, three sources.** Cyprus separates the **HE registration number** (DRCIP, the spine),
  the **TIC** (Tax Department, the tax id), and the **VAT number** (`CY` + 8 digits + letter). The open
  register carries only the HE number; TIC and VAT come from here.
- **Validation, not enumeration.** VIES validates a **supplied** VAT number; it does not list companies. This
  source enriches/validates known companies — it is not a company master and is not redistributable as a list.
