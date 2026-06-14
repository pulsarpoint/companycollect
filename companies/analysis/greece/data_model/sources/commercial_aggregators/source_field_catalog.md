# Commercial aggregators (ICAP/CRIF, Kyckr, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract. Fields described from public product documentation;
> no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Greece
- Source type: commercial_api
- Organization: various private vendors (ICAP/CRIF, Kyckr, …)
- URL: https://www.icapcrif.com/
- License: commercial / paid (per-vendor contract)
- Access: paid
- Freshness: from GEMI/AADE
- Record shape: vendor JSON or PDF (company master + parsed financials + officers + credit)
- Primary keys: `afm`
- Join keys: `afm`, `gemi_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.financials[] | financials | Parsed structured financials | array | financial | (paid) | EUR; from GEMI PDFs |
| company.officers[] | officers | Officers (± ownership) | array | person | (paid) | PII |
| company.credit_score | credit_score | Credit/risk rating | string | raw_extension | (paid) | proprietary |

## Interpretation Notes

- **The realistic path to structured financials at scale.** Because GEMI financials are **PDFs** and the API is
  **reCAPTCHA-protected**, vendors that pre-parse GEMI/AADE data are the practical way to get **structured**
  balance-sheet/income-statement figures (and credit data) without OCR-ing PDFs yourself.
- **Proprietary & paid.** Redistribution/persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Where a vendor only re-exposes GEMI fields, prefer GEMI.
- Join on **ΑΦΜ** / GEMI number.
