# cninfo (巨潮资讯网) — Listed-Company Financials Field Catalog

> **DOCUMENTED-ONLY / LISTED ISSUERS ONLY.** The CSRC-designated disclosure
> platform (+ SSE/SZSE/BSE). The only open route to Chinese financials, but only
> the listed population. Cataloged from public docs; no records retrieved.

## Source Summary

- Country: China
- Source type: official_financial
- Organization: CSRC-designated platform (Shenzhen Securities Information Co.); SSE / SZSE / BSE
- URL: http://www.cninfo.com.cn/ ; http://www.sse.com.cn/ ; http://www.szse.cn/
- License: issuer disclosure (open to view; redistribution per CSRC/exchange terms)
- Access: public (per-issuer)
- Freshness: annual/quarterly
- Record shape: per-issuer disclosure documents (PDF)
- Primary keys: `stock_code` + `uscc`
- Join keys: `uscc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| stock_code | 证券代码 | Stock code | string | identifier | listed only |
| 资产负债表 | balance_sheet | Balance sheet | object | financial | CNY; ASBE |
| 利润表 | income_statement | Income statement | object | financial | CNY |
| 现金流量表 | cash_flow | Cash flow | object | financial | CNY |
| 年报/半年报 | annual/interim report | Reports + announcements | array | document | listed only |

## Interpretation Notes

- The **only open financials** in China, but **listed issuers only** (A/B shares;
  H-shares via HKEX). Per-issuer PDFs to **ASBE (Chinese GAAP)**, in **CNY/RMB**;
  **no clean open bulk API**. Link the **stock code** to the **USCC** by issuer to
  join with GSXT identity.
- **Non-listed companies' financials are not publicly disclosed** — a structural
  gap for the vast majority of Chinese companies.
