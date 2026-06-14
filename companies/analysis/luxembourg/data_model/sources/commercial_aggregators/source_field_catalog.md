# Commercial aggregators (Kyckr, Creditreform, B2B, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract. Fields described from public product documentation;
> no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Luxembourg
- Source type: commercial_api
- Organization: various private vendors (Kyckr, Creditreform, B2B Group, …)
- URL: https://www.kyckr.com/
- License: commercial / paid (per-vendor contract)
- Access: paid
- Freshness: from RCS
- Record shape: vendor JSON or PDF (company master + parsed financials + officers + documents)
- Primary keys: `rcs_number`
- Join keys: `rcs_number`, `matricule`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.financials[] | financials | Parsed structured financials | array | financial | (paid) | EUR; from RCS comptes annuels |
| company.officers[] | officers | Directors/managers | array | person | (paid) | PII |
| company.documents[] | documents | Filed documents | array | document | (paid) | evidence |

## Interpretation Notes

- **The realistic path to bulk + structured financials.** Because the RCS has **no open bulk/API** (captcha-gated
  search) and the comptes annuels are **PDF**, vendors that pre-index the RCS and pre-parse the accounts are the
  practical way to get **bulk** company data and **structured** financials at scale without OCR-ing PDFs yourself.
- **Proprietary & paid.** Redistribution/persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Where a vendor only re-exposes free RCS fields/documents,
  prefer the RCS directly. Join on **rcs_number** / matricule / vat_id.
