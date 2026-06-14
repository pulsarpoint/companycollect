# Commercial aggregators (Racius, Informa D&B / einforma, Iberinform, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract (Racius offers free basic search). Fields described
> from public product documentation; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Portugal
- Source type: commercial_api
- Organization: various private vendors (Racius, Informa D&B/einforma, Iberinform, …)
- URL: https://www.racius.com/
- License: commercial / paid (some free basic search)
- Access: paid
- Freshness: from the register / IES
- Record shape: vendor JSON or PDF (company master + officers/shareholders + parsed IES financials + credit)
- Primary keys: `nipc`
- Join keys: `nipc`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.basics | NIPC/firma/CAE/capital/estado | Company master | object | identifier | (free-ish) | Racius free basic search |
| company.financials[] | financials | Parsed IES financials | array | financial | (paid) | EUR; IES not openly published |
| company.officers_shareholders[] | gerência / sócios | Officers + shareholders | array | person | (paid) | PII |
| company.credit | credit / risco | Credit/risk rating | string | raw_extension | (paid) | proprietary |

## Interpretation Notes

- **The realistic path to identified data + structured financials.** Because the register is **paid** and the
  **IES** financials are **not openly published**, vendors that resell the register + pre-parse the IES are the
  practical way to get **identified** company data and **structured financials** linked to a named company. Some
  (Racius) expose **free basic search** (NIPC, name, CAE, capital, status) — a partly-free identified route.
- **Proprietary & paid.** Redistribution/persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Join on **NIPC** / vat_id.
