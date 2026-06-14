# Commercial aggregators (CyprusRegistry, Kyckr, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract. Fields described from public product
> documentation; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Cyprus
- Source type: commercial_api
- Organization: various private vendors (CyprusRegistry, Kyckr, …)
- URL: https://cyprusregistry.com/
- License: commercial / paid (per-vendor contract)
- Access: paid
- Freshness: from the register (vendor refresh)
- Record shape: vendor JSON or PDF (company master + parsed financials + officers + documents)
- Primary keys: `registration_number`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.registration_number | registration_number | HE number (vendor key) | string | identifier | (none — paid) | join key |
| company.financials[] | financials | Parsed structured financials | array | financial | (none — paid) | EUR; from HE32 PDFs |
| company.officers[] | officers | Officers (± shareholders) | array | person | (none — paid) | PII |
| company.documents[] | documents | Filed documents | array | document | (none — paid) | evidence |

## Interpretation Notes

- **The realistic path to structured financials at scale.** Because the official Cyprus financials are
  **scanned PDFs behind a EUR 10 detailed search**, vendors that pre-parse those PDFs are the practical way to
  get **structured** balance-sheet/income-statement figures without running OCR yourself.
- **Proprietary & paid.** Redistribution and persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Where a vendor only re-exposes open DRCIP fields, prefer the
  open `drcip_register` source.
