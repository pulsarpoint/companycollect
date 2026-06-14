# Commercial aggregators (Kyckr, Creditinfo, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract. Fields described from public product documentation;
> no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Malta
- Source type: commercial_api
- Organization: various private vendors (Kyckr, Creditinfo, …)
- URL: https://www.kyckr.com/
- License: commercial / paid (per-vendor contract)
- Access: paid
- Freshness: from MBR
- Record shape: vendor JSON or PDF (company master + parsed financials + officers/shareholders + documents)
- Primary keys: `registration_number`
- Join keys: `registration_number`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.financials[] | financials | Parsed structured financials | array | financial | (paid) | EUR; from MBR annual accounts |
| company.officers_shareholders[] | officers / shareholders | Officers + shareholders | array | person | (paid) | PII |
| company.documents[] | documents | Filed documents | array | document | (paid) | evidence |

## Interpretation Notes

- **A realistic path to bulk + structured financials.** Alongside the paid MBR API, vendors that index the MBR
  and pre-parse the annual accounts are a practical way to get **bulk** company data and **structured**
  financials at scale without OCR-ing the paid PDFs yourself (the free web search is WAF-blocked; documents are
  paid).
- **Proprietary & paid.** Redistribution/persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Where a vendor only re-exposes MBR fields, prefer the MBR/API.
  Join on **registration_number** / vat_id.
