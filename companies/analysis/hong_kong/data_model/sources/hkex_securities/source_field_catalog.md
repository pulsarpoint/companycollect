# HKEX — List of Securities Field Catalog

## Source Summary

- Country: Hong Kong
- Source type: financial_disclosure
- Organization: Hong Kong Exchanges and Clearing (HKEX)
- URL: https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx
- License: HKEX website terms
- Access: **public via browser**; static xlsx returns a **template** (populated server-side)
- Freshness: daily
- Record shape: XLSX (template skeleton via static URL)
- Primary keys: stock_code
- Join keys: stock_code, isin, name_of_securities

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| stock_code | Stock Code | HKEX stock code | string | identifier |  | listed key |
| name_of_securities | Name of Securities | Security/company name | string | legal_name |  | |
| category | Category | Security category | string | metadata |  | Equity etc. |
| sub_category | Sub-Category | Sub-category | string | metadata |  | |
| board_lot | Board Lot | Board lot size | integer | metadata |  | |
| isin | ISIN | Securities id | string | identifier |  | listed-only |

## Interpretation Notes

- **HKEX List of Securities** is the daily list of listed stocks. The **static `.xlsx`
  URL returned a TEMPLATE skeleton** for an automated request (placeholders `<<Table
  Header>>`, `<<TableContent>>`, `<<nextTradeDate>>`; dimension A1:R8) — the populated list
  is generated **server-side**. So listed-security identity (stock code, name, ISIN) is
  **browser-public but not cleanly available** via this static URL for automation.
- Fields are documented from the known HKEX list structure with **medium/low confidence**;
  no populated values were captured.
- **Join**: stock code / ISIN keys the listed entity; join to the company register by name
  (HKEX does not publish the CR Company Number or BR Number in this list).
- **Scope**: listed companies only.
- No `sample_record.json`: only a template was retrieved (no populated data).
