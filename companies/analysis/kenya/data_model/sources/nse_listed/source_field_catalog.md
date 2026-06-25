# NSE — Nairobi Securities Exchange (listed companies + financials) Field Catalog

## Source Summary

- Country: Kenya
- Source type: financial_disclosure
- Organization: Nairobi Securities Exchange PLC (NSE)
- URL: https://www.nse.co.ke/listed-companies/
- License: public disclosure
- Access: **public, open** (HTML directory)
- Freshness: event-driven / quarterly
- Record shape: HTML directory, one row per listed company
- Primary keys: ticker
- Join keys: ticker, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | Absa Bank Kenya PLC | verified on page |
| ticker | Ticker / Symbol | NSE ticker | string | identifier | ABSA, SBIC, SASN | listed key |
| sector_segment | Sector / Market Segment | NSE sector | string | activity | Banking, Agricultural | |
| announcements | Listed Company Announcements | Announcements | array | filing |  | financial results, notices |
| financial_results | Financial Results | Financials | array | financial |  | KES; listed only |

## Interpretation Notes

- **NSE** is the one **open** Kenyan company source. `nse.co.ke/listed-companies/`
  publishes the **listed-company directory** (name, sector/segment) — **verified
  live**: Absa Bank Kenya PLC, Stanbic Holdings Plc, Standard Chartered Bank Ltd,
  Diamond Trust Bank Kenya Ltd, Sasini Ltd, Williamson Tea Kenya Ltd, Car and General
  (K) Ltd, Kapchorua/Limuru/Eaagads.
- NSE also publishes **listed-company announcements** and **financial results**
  (`/listed-company-announcements/`) and **market statistics**. The page is HTML;
  parse the directory table. The WordPress REST root (`/wp-json/`) is reachable for
  structured content.
- **Scope**: listed companies only (~60). Join to BRS by company name (registration
  number / KRA PIN are not on NSE — those are BRS, paid).
- **Note**: NSE's **real-time market-data feed** is a **paid** product (published
  pricelist) — use the public directory/announcements, not the paid feed. No personal
  data in the directory rows.
