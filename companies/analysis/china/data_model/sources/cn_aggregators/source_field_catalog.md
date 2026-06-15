# Commercial Aggregators (Qichacha / Tianyancha / Aiqicha) Field Catalog

> **PLANNING-ONLY / LICENSE-UNCERTAIN.** Private vendors that resell GSXT identity
> (+ shareholders/officers) and listed financials via paid APIs. Anti-bot (HTTP
> 419). Cataloged from public docs; no records copied. Use only with a licence;
> verify against official sources.

## Source Summary

- Country: China
- Source type: aggregator
- Organization: various private vendors (Qichacha 企查查, Tianyancha 天眼查, Aiqicha 爱企查)
- URL: https://www.tianyancha.com/ (and others)
- License: restricted / vendor terms
- Access: public search / paid bulk-API
- Freshness: varies
- Record shape: planning-only
- Primary keys: `uscc`
- Join keys: `uscc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company_identity | identity (from GSXT) | Identity | object | identifier | planning-only; paid |
| shareholders_officers | 股东/主要人员 | Shareholders + officers | array | ownership | planning-only; PII (PIPL) |
| listed_financials | 财务数据 | Listed financials | array | financial | planning-only; listed only |

## Interpretation Notes

- The **realistic route to coverage** (and to **shareholders/officers**, which the
  gated GSXT does not expose openly) — but **not official**, **paid**, and
  **license-uncertain**, with anti-bot protection. Keep **planning-only**; if used
  under licence, verify every field against the official GSXT / cninfo sources, and
  handle personal data per PIPL + cross-border data-export rules. Join on the USCC.
