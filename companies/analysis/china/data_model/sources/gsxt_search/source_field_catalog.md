# GSXT — National Enterprise Credit Information Publicity System Field Catalog

> **DOCUMENTED-ONLY / GATED.** The authoritative register, but per-company queries
> require **real-name authentication (since 2021) + a CAPTCHA**, it is
> Chinese-only, has **no open API/bulk**, and is frequently unreachable externally
> (HTTP 521). Fields from the public search UI; **no values retrieved**; controls
> not bypassed. No `sample_record.json`.

## Source Summary

- Country: China
- Source type: official_registry
- Organization: State Administration for Market Regulation (SAMR)
- URL: https://www.gsxt.gov.cn/
- License: restricted/unclear (no open re-use terms)
- Access: public per-company search (real-name authentication + CAPTCHA)
- Freshness: real-time
- Record shape: HTML per-company result
- Primary keys: `uscc` (统一社会信用代码)
- Join keys: `uscc`

## Fields (from the public search UI)

| Path (ZH) | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| 统一社会信用代码 | uscc | USCC (18-char) | string | identifier | id + taxpayer id; join key |
| 企业名称 | qiye_mingcheng | Company name | string | legal_name | Chinese |
| 类型 | leixing | Company type | string | legal_form | 有限责任公司 / 股份有限公司 |
| 登记状态 | dengji_zhuangtai | Status | string | status | 存续/在营/注销/吊销 |
| 法定代表人 | fading_daibiaoren | Legal representative | string | person | **PII (PIPL) — redact** |
| 注册资本 | zhuce_ziben | Registered capital | string | financial | subscribed; CNY |
| 成立日期 | chengli_riqi | Establishment date | date | date | |
| 住所 | zhusuo | Registered address | string | address | |
| 经营范围 | jingying_fanwei | Business scope | string | activity | free text; no code |

## Interpretation Notes

- **The authoritative identity source**, but **gated**: queries need real-name
  authentication + a CAPTCHA; **no open bulk/API**; Chinese-only; often
  unreachable externally. Full coverage realistically needs a **licensed
  commercial provider** (see `cn_aggregators`).
- **USCC = company id = taxpayer id** (one 18-char number) — the universal join
  key. China has **no separate VAT number**.
- **Legal representative** is the only open analogue to officers — **personal data
  (PIPL)**, redact. Shareholders (股东) exist on GSXT but are gated.
- **Business scope** is free text — there is **no coded activity classification**
  shown.
- Do **not** bypass the real-name auth / CAPTCHA or run automated queries.
