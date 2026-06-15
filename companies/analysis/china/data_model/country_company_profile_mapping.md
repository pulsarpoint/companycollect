# China Company Profile — Source Mapping

> **Portal-gated / no open bulk.** The authoritative register (GSXT, run by
> SAMR) requires real-name authentication + a per-query CAPTCHA and exposes no
> open API or bulk download (often unreachable externally, HTTP 521). Full
> coverage realistically needs a licensed commercial provider. Everything keys
> on the **USCC** (统一社会信用代码, 18-char), which is both the company id and
> the **taxpayer id** — China has **no separate VAT number**. Financials are
> open only for **listed** issuers (cninfo / SSE / SZSE; CNY; ASBE). This model
> is largely **planning-only**.

## Field mapping

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.uscc | gsxt_search | 统一社会信用代码 | uscc | GSXT authoritative | Company id + taxpayer id. Gated. |
| tax_identifiers.tax_id | gsxt_search | 统一社会信用代码 | uscc | = USCC | Same value as registration.uscc. |
| tax_identifiers.vat_id | — | — | — | n/a | No separate VAT in China. |
| legal_identity.legal_name | gsxt_search | 企业名称 | uscc | GSXT authoritative | Gated. |
| legal_identity.company_type | gsxt_search | 类型 | uscc | GSXT authoritative | 有限责任公司 / 股份有限公司 / ... |
| status.status_raw / status | gsxt_search | 登记状态 | uscc | GSXT authoritative | Map 存续/在营→active, 注销→deregistered, 吊销→revoked. |
| incorporation.establishment_date | gsxt_search | 成立日期 | uscc | GSXT authoritative | Gated. |
| capital.registered_capital | gsxt_search | 注册资本 | uscc | GSXT authoritative | Subscribed capital, CNY. |
| registered_location.registered_address | gsxt_search | 住所 | uscc | GSXT authoritative | Gated. |
| activity.business_scope | gsxt_search | 经营范围 | uscc | GSXT authoritative | Free text; no coded classification. |
| officers[] (legal rep) | gsxt_search | 法定代表人 | uscc | GSXT authoritative | PERSONAL DATA (PIPL) — redact. |
| financial_statements[] | cninfo_disclosure | 资产负债表 / 利润表 / 现金流量表 | stock_code → uscc | cninfo/SSE/SZSE | LISTED ONLY; CNY; ASBE; planning-only. |
| (risk enrichment) | credit_china | 行政处罚 / 红黑名单 | uscc | secondary | Not in profile; risk signal only. Bot-protected (HTTP 412). |
| (identity + ownership at scale) | cn_aggregators | identity / 股东 / 财务 | uscc | last resort | Paid / license-uncertain; verify vs official. PIPL. |

## Source precedence

1. **gsxt_search** (GSXT / SAMR) — authoritative for identity, status, capital,
   address, scope, legal representative. **Gated** (real-name + CAPTCHA; no open
   bulk/API).
2. **cninfo_disclosure** — authoritative for **listed** financials only.
3. **credit_china** — secondary risk/compliance enrichment (penalties, red/black
   lists); not identity or financials; bot-protected.
4. **cn_aggregators** (Qichacha / Tianyancha / Aiqicha) — only realistic route to
   bulk identity + shareholders/officers, but **paid / license-uncertain** and
   not authoritative; verify every field against official sources.

## Join keys

- **USCC** (18-char) is the universal join key across all sources and is also the
  taxpayer id.
- Listed financials join via **stock_code → USCC** (issuers map their listing
  code to their USCC in disclosures).

## Missing / restricted data

- **No open bulk register** — GSXT is real-name + CAPTCHA gated; no API.
- **No separate VAT id** — taxpayer id = USCC.
- **No coded activity classification** in the open identity view — 经营范围 is
  free text.
- **Non-listed financials are not publicly disclosed** — only listed issuers
  (cninfo/SSE/SZSE).
- **Shareholders/officers** beyond the legal representative are not open — only
  via paid aggregators.
- **PIPL** governs personal data (legal rep, shareholders); **cross-border
  data-export** rules apply to any transfer out of China.
