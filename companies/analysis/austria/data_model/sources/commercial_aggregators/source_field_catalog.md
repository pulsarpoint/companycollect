# Commercial Aggregators — Field Catalog

> **PLANNING-ONLY (paid).** The realistic path to a **full company master + structured financials at scale**
> in Austria, since the Firmenbuch + Jahresabschluss are paid and there is no open bulk. Cataloged from
> public vendor docs; no records/values copied.

## Source Summary

- Country: Austria
- Source type: commercial_api
- Organization: Compass-Verlag / KSV1870 / firmafind / Dun & Bradstreet
- URL: https://firmafind.at/docs (and others)
- License: commercial / paid — planning-only
- Access: paid (API key)
- Freshness: daily
- Record shape: structured JSON per company (Firmenbuch master + Jahresabschluss financials)
- Primary keys: `firmenbuchnummer`
- Join keys: `firmenbuchnummer`, `uid`

## Fields

| Field (planning) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|
| company.firmenbuchnummer | Company register number | string | identifier | join |
| company.name | Legal name | string | legal_name | |
| company.uid | VAT id | string | identifier | ATU######## |
| company.officers | Directors | array | person | PII |
| financials[].year | Fiscal year | integer | date | multi-year |
| financials[].revenue | Revenue | decimal | financial | AT disclosure limits |
| financials[].net_income | Net income | decimal | financial | |
| financials[].total_assets | Total assets | decimal | financial | |

## Interpretation Notes

- **Why include it**: Austria has no open company master and no open bulk financials; aggregators
  **pre-parse the Firmenbuch + Jahresabschluss into structured JSON** (multi-year financials, officers) —
  the only practical way to get the full population with financials without building paper/PDF parsing.
- **firmafind** exposes Firmenbuch + annual accounts as JSON sourced from the BMJ; **Compass** and
  **KSV1870** are long-standing AT business-data houses; **Dun & Bradstreet** for global coverage.
- Same disclosure reality as the official source: revenue/net_income limited for small companies.
- No `sample_record.json` — paid; values not retrievable under planning-only terms. Exact JSON keys per vendor.
