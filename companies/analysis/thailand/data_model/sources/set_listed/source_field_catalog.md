# SET — Stock Exchange of Thailand (listed financials) Field Catalog

## Source Summary

- Country: Thailand
- Source type: financial_disclosure
- Organization: The Stock Exchange of Thailand (SET)
- URL: https://www.set.or.th/
- License: public disclosure
- Access: public (via browser)
- Freshness: quarterly / annual
- Record shape: per-listed-company profile + filings
- Primary keys: symbol
- Join keys: symbol, company_name, juristic_id

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| symbol | symbol | SET stock symbol | string | identifier | PTT, BBL, CPALL | listed-entity key |
| company_name | company_name | Listed company name | string | legal_name | PTT PUBLIC COMPANY LIMITED | |
| sector | sector / industry | SET sector | string | activity |  | SET industry groups |
| financial_statements | financial statements | Financial statements | array | financial |  | THB; listed only |
| juristic_id | juristic id (in filings) | 13-digit juristic id | string | identifier |  | join to DBD |

## Interpretation Notes

- **SET** publishes **listed-company** financial statements and disclosures (public
  via browser). It complements the **DBD OpenAPI** (which carries capital but not
  full statements) and the **DataWarehouse** (login) for the listed subset.
- **Join**: the SET **symbol** keys the listed entity; filings carry the **13-digit
  juristic ID**, which joins to the DBD OpenAPI for the full legal identity.
- Listed companies only (~800); currency **THB**.
