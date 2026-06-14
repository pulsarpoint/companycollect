# FinStat / Commercial Aggregators Field Catalog

> **PLANNING-ONLY / PAID.** Cataloged from public product docs only. FinStat and
> similar vendors aggregate the free RPO + RÚZ data and add a proprietary credit
> rating. No records retrieved. The official APIs supersede this for everything
> except the rating.

## Source Summary

- Country: Slovakia
- Source type: aggregator
- Organization: FinStat s.r.o. (and similar)
- URL: https://www.finstat.sk/api
- License: commercial / restricted
- Access: paid (API key)
- Freshness: derived from RPO/RÚZ
- Record shape: planning-only
- Primary keys: `ico`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company | company master | Identity + financials | object | identifier | planning-only; from RPO/RÚZ |
| rating | rating | Credit/risk score | string | raw_extension | planning-only; proprietary |
| financials_history[] | financials | Pre-parsed multi-year financials | array | financial | planning-only; convenience |

## Interpretation Notes

- Adds **no authoritative fields** over the free official RPO + RÚZ — its only
  genuine extra is a **proprietary credit rating** and the convenience of
  pre-decoded financials. Keep **planning-only**; paid, restricted redistribution.
