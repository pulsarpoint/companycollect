# VID — State Revenue Service VAT register / VIES (LV VAT) Field Catalog

## Source Summary

- Country: Latvia
- Source type: official_tax
- Organization: Valsts ieņēmumu dienests (VID) / European Commission
- URL: https://www.vid.gov.lv/ ; VIES https://ec.europa.eu/taxation_customs/vies/
- License: validation / open lists
- Access: public
- Freshness: real-time
- Record shape: per-number validation (VIES) + VID published VAT-payer lists
- Primary keys: none
- Join keys: `regcode`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| vat_id | PVN numurs | LV VAT number | string | identifier | LV40103550818 | LV + regcode |
| vat_valid | VAT validity | VIES/VID validity | boolean | status | — | point-in-time |
| name / address | name/address | Name/address (if returned) | string | legal_name | — | cross-check |

## Interpretation Notes

- **VAT fills a small gap.** The Latvian VAT number (**PVN reģistrācijas numurs**) is simply **`LV` + the
  11-digit regcode** — derivable from the open register. VID publishes VAT-payer information and VIES validates a
  given number. Validation/enrichment only; not a company master. Join on **regcode** / vat_id.
