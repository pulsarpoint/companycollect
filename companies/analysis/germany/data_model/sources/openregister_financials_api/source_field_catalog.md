# OpenRegister.de — Bundesanzeiger Financial API — Field Catalog

> **PLANNING-ONLY (paid).** Cataloged from public vendor documentation only
> (https://docs.openregister.de/sources/bundesanzeiger). No records or values were retrieved; exact
> JSON keys are unknown until contracted. Included because it is the realistic path to **structured
> financials at scale** that no open source provides.

## Source Summary

- Country: Germany
- Source type: commercial_api
- Organization: OpenRegister (private vendor)
- URL: https://docs.openregister.de/sources/bundesanzeiger
- License: commercial / paid; redistribution governed by contract
- Access: paid (API key)
- Freshness: daily (added as companies publish in Bundesanzeiger)
- Record shape: structured JSON; two endpoints — **financial indicators** (key metrics) and
  **detailed reports** (balance sheet + income statement)
- Coverage: "hundreds of thousands" of German companies
- Primary keys: vendor company id (+ fiscal period)

## Fields

| Field (planning) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|
| total_assets | Balance sheet total | decimal | financial | "Complete balance sheets" |
| asset_liability_breakdown | Component breakdown | object | financial | structure TBD |
| revenue | Turnover | decimal | financial | only where P&L exists |
| profitability | Profitability metrics | object | financial | derived |
| net_income | Annual result | decimal | financial | |
| equity | Total equity | decimal | financial | |
| cash_position | Cash / liquid funds | decimal | financial | |
| employees | Employee count | integer | employment | |
| fiscal_period | Year/period | string | date | multi-year history implied |
| company_ref | Vendor company id | string | identifier | join lever |

## Interpretation Notes

- The vendor's selling point is **JSON instead of PDF** — it pre-parses Bundesanzeiger filings into
  structured fields, removing the XBRL/HTML parsing burden that the official source imposes.
- Two documented endpoints: a lightweight **indicators** endpoint (key metrics) and a **detailed
  reports** endpoint (full balance sheet + income statement).
- Same underlying reality as `unternehmensregister_financials`: figures depend on the filer's size
  class, so `revenue`/`net_income` are present mainly for medium/large companies.
- **Comparable vendors** with German financials (any could substitute here): North Data,
  handelsregister.ai, Implisense, Creditreform, Dun & Bradstreet / Bisnode.
- No `sample_record.json` — paid source; values not retrievable under planning-only terms.
