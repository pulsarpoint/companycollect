# Estonian Tax and Customs Board — VAT (KMKR) / tax datasets / VIES Field Catalog

## Source Summary

- Country: Estonia
- Source type: official_tax
- Organization: Maksu- ja Tolliamet (EMTA)
- URL: https://www.emta.ee/
- License: validation (VIES) / open (EMTA tax datasets)
- Access: public
- Freshness: real-time (VIES) / periodic (datasets)
- Record shape: per-number validation + periodic open tax datasets
- Primary keys: none (not a master)
- Join keys: `registrikood`, `kmkr_nr`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| kmkr_nr | KMKR | VAT number | string | identifier | EE101335276 | already in register |
| vat_valid | VAT validity | VIES validity | boolean | status | — | point-in-time |
| tax_debt | maksuvõlg | Tax debt / paid taxes | decimal | financial | — | EMTA open datasets |

## Interpretation Notes

- **Enrichment/validation.** KMKR (VAT) is already in the register basic data; EMTA/VIES **validate** it. EMTA
  additionally publishes **tax-debt / paid-taxes** open datasets keyed on registrikood — a useful risk/size
  signal. VIES validates a supplied number and does not enumerate companies; the e-Business Register is the
  company master. Tax datasets documented here, not parsed.
