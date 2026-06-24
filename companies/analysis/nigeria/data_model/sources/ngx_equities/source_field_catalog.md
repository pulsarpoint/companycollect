# NGX — Nigerian Exchange (listed equities + financials) Field Catalog

## Source Summary

- Country: Nigeria
- Source type: financial_disclosure
- Organization: Nigerian Exchange Group (NGX)
- URL: https://doclib.ngxgroup.com/REST/api/statistics/equities/
- License: public disclosure
- Access: **public, open** (JSON)
- Freshness: daily
- Record shape: JSON array, one object per listed equity
- Primary keys: Symbol
- Join keys: Symbol, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| [].Symbol | Symbol | NGX ticker | string | identifier | DANGCEM, MTNN | listed key |
| [].Sector | Sector | NGX sector | string | activity | FINANCIAL SERVICES | |
| [].Market | Market | Listing board | string | metadata | Premium Board | |
| [].ClosePrice | ClosePrice | Close price | decimal | financial | 963.0 | NGN |
| [].OpeningPrice | Open/High/Low | Daily prices | decimal | financial |  | NGN |
| [].Change | Change/PercChange | Day change | decimal | financial |  | |
| [].Trades | Trades/Volume/Value | Trading activity | decimal | financial |  | Value NGN |
| [].TradeDate | TradeDate | Trade date | date | date |  | |
| financial_statements | Financial Statements | Listed financials | array | financial |  | NGX disclosures; listed only |

## Interpretation Notes

- **NGX EDGE / market-data API** is the one **open** Nigerian company source. `GET
  doclib.ngxgroup.com/REST/api/statistics/equities/?pageSize=500&pageNo=0` returns a
  **JSON array** of listed equities — **verified live**: 146 equities incl.
  **DANGCEM** (Industrial Goods, Premium Board, ₦963), **MTNN** (ICT), **GTCO** /
  **ZENITHBANK** / **ACCESSCORP** (Financial Services), **SEPLAT** (Oil & Gas),
  **NESTLE** / **NB** / **DANGSUGAR** (Consumer Goods), **BUACEMENT**.
- The `Symbol` field is the listed-entity key; the directory also covers
  delisted companies and listed bonds. **Full financial statements** are in the NGX
  issuer-disclosure section (per listed company, NGN).
- **Scope**: listed companies only (~150). Join to CAC by company name (RC number is
  not on NGX — that is CAC, gated/paid). No personal data in the market-data rows.
- Note: in this endpoint the `Company2` field can echo the symbol; resolve full legal
  names from the issuer disclosure pages.
