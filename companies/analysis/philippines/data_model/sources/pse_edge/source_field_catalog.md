# PSE EDGE — listed-company directory + disclosures Field Catalog

## Source Summary

- Country: Philippines
- Source type: financial_disclosure
- Organization: Philippine Stock Exchange (PSE)
- URL: https://edge.pse.com.ph/companyDirectory/search.ax
- License: public disclosure
- Access: **public** (POST search)
- Freshness: event-driven / quarterly
- Record shape: HTML table rows, one per listed company
- Primary keys: stock_symbol
- Join keys: stock_symbol, company_name, cmpy_id

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | PLDT Inc. | display name |
| stock_symbol | Stock Symbol | PSE ticker | string | identifier | TEL, JFC | listed key |
| cmpy_id | cmpy_id | PSE internal id | string | identifier |  | disclosure links |
| sector | Sector | PSE sector | string | activity | Services | |
| subsector | Subsector | PSE subsector | string | activity | Telecommunications | |
| listing_date | Listing Date | Listing date | date | date | Sep 17, 1953 | 'Mon dd, yyyy' |
| financial_reports | Disclosures / Financial Reports | Financials | array | financial |  | PHP; listed only |

## Interpretation Notes

- **PSE EDGE** is the one **open** Philippine company source. The company directory
  search (`POST /companyDirectory/search.ax`, form-encoded with `keyword`, `sector`,
  `subsector`, `pageNo`, `pageSize`, sort params) returns **HTML table rows**:
  company name, **stock symbol**, sector, subsector, listing date — **verified live**
  (PLDT Inc. / TEL / Services / Telecommunications / Sep 17 1953; Jollibee /
  Industrial / Food).
- Each company has an internal **cmpy_id** used to reach its **disclosures and
  financial reports** (PHP). **Listed companies only** (~280).
- **Join**: the **stock symbol** keys the listed entity; companies can be joined to
  SEC by name (SEC Registration Number / TIN are not on PSE — those are SEC, paid).
- Response is HTML (not JSON) — parse the table; be polite with request volume.
- No personal data in the directory rows (PDPA-safe at this level).
