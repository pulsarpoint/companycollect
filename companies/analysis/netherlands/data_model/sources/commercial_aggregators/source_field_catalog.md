# Commercial aggregators (Company.info, Graydon/CreditSafe, Kyckr, ...) Field Catalog

> **Planning-only.** Proprietary, paid, per-vendor contract. Fields described from public product documentation;
> no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Netherlands
- Source type: commercial_api
- Organization: various private vendors (Company.info, Graydon/CreditSafe, Kyckr, …)
- URL: https://www.company.info/
- License: commercial / paid (per-vendor contract)
- Access: paid
- Freshness: from KvK
- Record shape: vendor JSON (identified company master + officers + shareholders + parsed financials + group)
- Primary keys: `kvkNummer`
- Join keys: `kvkNummer`, `rsin`, `vat_id`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.financials[] | financials | Identified parsed financials | array | financial | (paid) | EUR; linked to KvK number |
| company.officers_shareholders[] | officers / shareholders / group | Officers + owners + group | array | person | (paid) | PII |
| company.credit | credit | Credit/risk rating | string | raw_extension | (paid) | proprietary |

## Interpretation Notes

- **The route to IDENTIFIED data + financials at scale.** Because the open KvK datasets are **anonymised** and the
  paid KvK API delivers identity but not necessarily multi-year parsed financials, vendors (Company.info,
  Graydon/CreditSafe, Kyckr) that combine the KvK register with parsed jaarrekeningen + group structure + credit
  are the practical way to get **identified** company data and **structured financials linked to named companies**
  at scale.
- **Proprietary & paid.** Redistribution/persistence are governed by the vendor contract — keep entirely
  **planning-only** until a contract is in place. Where a vendor only re-exposes KvK fields, prefer the KvK
  API / open data. Join on **kvkNummer** / rsin / vat_id.
