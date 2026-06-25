# DFM & ADX — listed-company financials Field Catalog

## Source Summary

- Country: United Arab Emirates
- Source type: financial_disclosure
- Organization: Dubai Financial Market (DFM) / Abu Dhabi Securities Exchange (ADX)
- URL: https://www.dfm.ae/
- License: public disclosure
- Access: **public via browser; WAF/auth-gated** for automation
- Freshness: event-driven / quarterly
- Record shape: listed-company profile (browser-public; WAF/auth-gated feeds)
- Primary keys: symbol
- Join keys: symbol, isin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | Emaar Properties PJSC | listed only |
| symbol | Symbol | DFM/ADX ticker | string | identifier | EMAAR, ENBD, FAB | listed key |
| exchange | Exchange | DFM/ADX/Nasdaq Dubai | string | metadata | DFM | |
| isin | ISIN | Securities id | string | identifier | AE... | UAE ISINs start AE |
| sector | Sector | Exchange sector | string | activity | Real Estate, Banking | |
| financial_statements | Financial Statements / Disclosures | Financials | array | financial |  | AED; listed only |

## Interpretation Notes

- **DFM** (Dubai Financial Market) and **ADX** (Abu Dhabi Securities Exchange) — plus
  **Nasdaq Dubai** — publish **listed-company** profiles, disclosures, and financial
  statements. These are **public via the browser** but **WAF/auth-gated** for
  automation: **ADX** returned **HTTP 403 (WAF)**; **DFM** is a SPA whose market-data
  feeds (`connexions.dfm.ae/ext/p/arena/api/v2`, `feeds.dfm.ae`) are **auth-gated**.
  **Not bypassed** — example values are **public-knowledge tickers** (EMAAR = Emaar
  Properties; ENBD = Emirates NBD on DFM; FAB = First Abu Dhabi Bank on ADX).
- **Join**: the **symbol / ISIN** keys the listed entity; join to the registry
  (NER / emirate DED) by company name. Currency **AED**.
- **Scope**: listed companies only; private-company financials are not public.
