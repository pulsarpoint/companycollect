# Credit China (信用中国) Field Catalog

> **DOCUMENTED-ONLY / SECONDARY.** Credit + administrative-penalty portal (NDRC).
> Bot-protected (HTTP 412); some downloadable penalty/redlist datasets, but **not
> the company register**. Cataloged from public docs; no records retrieved.

## Source Summary

- Country: China
- Source type: official_registry
- Organization: National Public Credit Information Center (NDRC)
- URL: https://www.creditchina.gov.cn/
- License: restricted/unclear
- Access: public (bot-protected)
- Freshness: periodic
- Record shape: credit/penalty records
- Primary keys: `uscc`
- Join keys: `uscc`

## Fields

| Path (ZH) | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| 行政处罚 | administrative_penalties | Penalty records | array | filing | planning-only; risk signal |
| 红名单/黑名单 | redlist_blacklist | Trust/discredited status | string | status | planning-only |

## Interpretation Notes

- A **credit / compliance** layer (administrative penalties, red/black lists) keyed
  on **USCC** — useful as a **risk signal**, not as the company register (which it
  does not publish in full). Bot-protected; reuse terms unclear → planning-only /
  cross-check.
