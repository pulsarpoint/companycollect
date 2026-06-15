# China — Schema Notes

## Identifiers

- **USCC — Unified Social Credit Code (统一社会信用代码)** — **18-character**
  alphanumeric; the unified company id, which **doubles as the taxpayer id**.
  (Since 2015 it replaced the old 注册号 registration number + tax number.) The
  universal join key.
- **No separate VAT number** — China levies VAT, but the taxpayer id is the USCC.
- Old **注册号 (registration number)** — 15-digit; legacy.
- Listed companies also have a **stock code** (e.g. 600519 SSE; 000001 SZSE).

## GSXT per-company search (fields shown on the public result)

| Field (ZH) | Meaning |
|---|---|
| 统一社会信用代码 | USCC (18-char) = company id = taxpayer id |
| 企业名称 | Company name (Chinese) |
| 类型 | Company type (有限责任公司 LLC, 股份有限公司 joint-stock, 个体工商户 sole trader, …) |
| 登记状态 | Status (存续/在营 active, 注销 deregistered, 吊销 revoked, 迁出 moved) |
| 法定代表人 | Legal representative (PERSONAL DATA) |
| 注册资本 | Registered capital |
| 成立日期 | Establishment date |
| 住所 | Registered address |
| 经营范围 | Business scope (free text; not a coded activity classification) |

No open API/bulk — read from the gated (real-name + CAPTCHA) per-company search.

## Listed-company financials (cninfo / SSE / SZSE)

Per issuer: 资产负债表 (balance sheet), 利润表 (income statement), 现金流量表
(cash flow), 年报/半年报 (annual/interim reports). PDF; CNY (RMB); Chinese GAAP
(ASBE). Listed companies only.

## Mapping to internal model

| Internal | China source |
|---|---|
| company_id | USCC |
| registration_number | USCC (or legacy 注册号) |
| tax_id | USCC (same) |
| vat_id | not_available (no separate VAT number; taxpayer id = USCC) |
| legal_name | GSXT 企业名称 |
| company_type / legal_form | GSXT 类型 |
| status | GSXT 登记状态 (map active/deregistered/revoked) |
| incorporation_date | GSXT 成立日期 |
| dissolution_date | implied by status (注销) |
| registered_address | GSXT 住所 |
| activity_code | not_available (经营范围 is free text; no GB/T industry code shown) |
| financials | cninfo/SSE/SZSE (listed only); non-listed not_available |
| officers / legal representative | GSXT 法定代表人 (PII; redact) |
| owners | GSXT 股东 (shareholders; gated) / aggregators |

## Gotchas

- **Chinese-only**, UTF-8. **No open bulk/API** — register is real-name + CAPTCHA
  gated; financials listed-only.
- USCC = company id = taxpayer id (one number); no separate VAT.
- Legal-representative / shareholder names are personal data (PIPL) — redact;
  mind cross-border data-export rules.
- The committed normalized sample is **schematic** (no real per-company record
  lawfully bulk-downloadable).
