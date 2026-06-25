# Saudi Exchange (Tadawul) — listed-company financials Field Catalog

## Source Summary

- Country: Saudi Arabia
- Source type: financial_disclosure
- Organization: Saudi Exchange (Tadawul)
- URL: https://www.saudiexchange.sa/
- License: public disclosure
- Access: **public via browser; WAF-gated** ("Access Denied") for automation
- Freshness: event-driven / quarterly
- Record shape: listed-company profile (browser-public; WAF-gated)
- Primary keys: symbol
- Join keys: symbol, isin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | Al Rajhi Bank | listed only |
| symbol | Symbol | Tadawul ticker (4-digit) | string | identifier | 2222, 1120, 2010 | listed key |
| isin | ISIN | Securities id | string | identifier | SA... | Saudi ISINs start SA |
| sector | Sector | TASI sector | string | activity | Energy, Banking | |
| financial_statements | Financial Statements | Financials | array | financial |  | SAR; listed only |
| disclosures | Disclosures | Announcements | array | filing |  | WAF-gated |

## Interpretation Notes

- **Saudi Exchange (Tadawul)** publishes **listed-company** profiles, disclosures, and
  financial statements (the issuer directory). It is **public via the browser** but
  returned **HTTP 403 "Access Denied" (WAF)** for automated requests. **Not
  bypassed** — example values are **public-knowledge** Tadawul symbols (2222 = Saudi
  Aramco; 1120 = Al Rajhi Bank; 2010 = SABIC; 7010 = STC).
- **Join**: the **4-digit symbol** / **ISIN** keys the listed entity; join to the MoC
  Commercial Register by company name / Unified Number. Currency **SAR**.
- **Scope**: listed companies only; private-company financials are not public.
