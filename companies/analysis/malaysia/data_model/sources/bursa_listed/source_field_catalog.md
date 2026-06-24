# Bursa Malaysia — listed-company financials Field Catalog

## Source Summary

- Country: Malaysia
- Source type: financial_disclosure
- Organization: Bursa Malaysia Berhad
- URL: https://www.bursamalaysia.com/
- License: public disclosure
- Access: public via browser; **WAF-blocked (403)** for automation from this environment
- Freshness: quarterly / annual
- Record shape: per-listed-company profile + filings
- Primary keys: stock_code
- Join keys: stock_code, company_name, registration_number_new

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| stock_code | Stock Code | Bursa stock code | string | identifier | 1155, 5347, 5183 | listed-entity key |
| company_name | Company Name | Listed company name | string | legal_name | Malayan Banking Berhad | |
| sector | Sector | Bursa sector | string | activity |  | |
| financial_statements | Financial Statements | Financials | array | financial |  | MYR; listed only |
| registration_number_new | Registration No. | SSM 12-digit number | string | identifier |  | join to SSM |

## Interpretation Notes

- **Bursa Malaysia** publishes **listed-company** financial statements and
  announcements (public via browser). It complements the **paid SSM** financials for
  the listed subset (~900 companies). Real listed examples (public knowledge):
  **1155 MAYBANK**, **5347 TENAGA**, **5183 PCHEM**.
- **Access**: returned **HTTP 403 (WAF)** for automated requests from this
  environment — do not bypass; use a browser/clearance context.
- **Join**: the **stock code** keys the listed entity; announcements carry the SSM
  **12-digit registration number**, which joins to the SSM register. Currency **MYR**.
