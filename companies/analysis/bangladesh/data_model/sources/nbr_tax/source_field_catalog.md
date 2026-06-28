# National Board of Revenue (NBR) — taxpayer / BIN verification Field Catalog

## Source Summary

- Country: Bangladesh
- Source type: tax_register
- Organization: National Board of Revenue (NBR), Bangladesh (nbr.gov.bd)
- URL: https://nbr.gov.bd/
- License: restricted
- Access: **browser-public per-BIN/TIN verification** (no bulk/API)
- Freshness: live
- Record shape: per-BIN or per-TIN verification result
- Primary keys: bin
- Join keys: bin, e_tin, taxpayer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| bin | BIN | VAT Business Identification Number | string | identifier |  | VAT id |
| e_tin | e-TIN | Taxpayer Identification Number | string | identifier |  | income-tax id |
| taxpayer_name | Taxpayer Name | Taxpayer name | string | legal_name |  | individuals = personal data |
| vat_status | VAT Registration Status | VAT status | string | status |  | active/inactive |

## Interpretation Notes

- The **National Board of Revenue** provides **BIN** (VAT Business Identification Number) and
  **e-TIN** (income-tax) verification; VAT registration runs through the separate NBR VAT
  online system. Access is **per-BIN/TIN verification** (browser-public), **not** a bulk
  download or open API. Documented from public knowledge; no per-BIN values were captured.
- **Identifiers**: **BIN** (VAT) and **e-TIN** (income tax). These complement RJSC/DSE with
  tax identity, but they do **not** directly key to RJSC — join by **name** (and BIN/TIN where
  matched). Covers **individuals** too — individual taxpayer data is personal data; redact.
- No `sample_record.json`: per-BIN/TIN verification, nothing captured.
