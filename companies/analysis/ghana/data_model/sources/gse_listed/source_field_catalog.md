# GSE — Ghana Stock Exchange (listed companies + financials) Field Catalog

## Source Summary

- Country: Ghana
- Source type: financial_disclosure
- Organization: Ghana Stock Exchange (GSE)
- URL: https://gse.com.gh/listed-companies/
- License: public disclosure
- Access: **public, open** (HTML)
- Freshness: event-driven / quarterly
- Record shape: HTML directory, one row per listed company
- Primary keys: ticker
- Join keys: ticker, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | Ecobank Ghana PLC | verified on page |
| ticker | Ticker / Symbol | GSE ticker | string | identifier | EGH, GCB, AGA | listed key |
| sector | Sector / Industry | GSE sector | string | activity | Banking, Mining | |
| profile | Company Profile | Profile | object | metadata |  | /profile-of-listed-companies/ |
| financial_statements | Financial Statements | Financials | array | financial |  | GHS; listed only |

## Interpretation Notes

- **GSE** is the one **open** Ghanaian company source. `gse.com.gh/listed-companies/`
  lists all listed companies (name, sector) — **verified live**: Access Bank Ghana
  Plc, Agricultural Development Bank, AngloGold Ashanti Plc, Benso Oil Palm Plantation
  Ltd, CalBank PLC, Camelot Ghana Ltd, Clydestone (Ghana) Ltd, Cocoa Processing
  Company, Ecobank Ghana PLC, Enterprise Group PLC, Fan Milk Limited, GCB / Ghana
  Commercial Bank, Guinness Ghana Breweries Plc, Mega African Capital Ltd, Standard
  Chartered Bank Ghana PLC.
- Per-company **profiles** (`/profile-of-listed-companies/`) and **financial
  statements** (`/financial-statements/`, GHS) are public. The site also has market
  reports and an OTC market.
- **Scope**: listed companies only (~35). Join to the ORC by company name
  (registration number / TIN are not on GSE — those are ORC, paid/firewalled).
- Pages are HTML (English); parse the directory table. No personal data at the
  directory level. Currency **GHS**.
