# Commercial Aggregators Field Catalog

> **PLANNING-ONLY / LICENSE-UNCERTAIN.** Private vendors that scrape/repackage NBRP
> identity (masothue.com, infodoanhnghiep.com) and listed-company financials
> (vietstock, cafef, fireant). Cataloged from public docs; no records copied. Use
> only as a cross-check; verify against official sources.

## Source Summary

- Country: Vietnam
- Source type: aggregator
- Organization: various private vendors
- URL: https://masothue.com/ (and others)
- License: restricted / vendor terms
- Access: public search / paid bulk-API
- Freshness: varies
- Record shape: planning-only
- Primary keys: `ma_so_thue`
- Join keys: `ma_so_thue`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_identity | identity (from NBRP) | Company identity | object | identifier | planning-only; scraped |
| listed_financials | financials (from HOSE/HNX) | Listed financials | array | financial | planning-only; vendor terms |

## Interpretation Notes

- A **convenience layer** over the gated NBRP and the listed-company disclosures —
  but **not official** and **license-uncertain** (terms of use restrict reuse).
  Keep **planning-only**; if used, verify every field against the official NBRP /
  SSC sources. Join on the tax code (= enterprise code).
