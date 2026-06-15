# Companies House — Free Company Data Product (basic data) Field Catalog

## Source Summary

- Country: United Kingdom
- Source type: official_registry
- Organization: Companies House
- URL: http://download.companieshouse.gov.uk/en_output.html
- License: Open Government Licence (OGL)
- Access: public
- Freshness: monthly (snapshot dated YYYY-MM-01)
- Record shape: CSV, one row per company, **55 columns**
- Primary keys: `CompanyNumber`
- Join keys: `CompanyNumber`

## Fields (key columns; 55 total)

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| CompanyNumber | CompanyNumber | 8-char id | string | identifier | 08209948 | join key |
| CompanyName | CompanyName | Name | string | legal_name | ! LTD | |
| CompanyCategory | CompanyCategory | Legal form | string | legal_form | Private Limited Company | |
| CompanyStatus | CompanyStatus | Status | string | status | Active | |
| IncorporationDate / DissolutionDate | (same) | Dates | date | date | 11/09/2012 | DD/MM/YYYY |
| RegAddress.* | RegAddress.* | Registered address | string | address | 9 PRINCES SQUARE, HARROGATE, HG1 1ND | split |
| SICCode.SicText_1..4 | (same) | SIC activity | string/array | activity | 99999 - Dormant Company | UK SIC 2007 |
| Accounts.AccountCategory | (same) | Accounts type | string | filing | DORMANT / SMALL / FULL | |
| Accounts.* / Returns.* / ConfStmt* | (same) | Filing dates | date | date | | |
| Mortgages.Num* | (same) | Charge counts | integer | metadata | | detail via API |
| PreviousName_1..10 | (same) | Former names | array | legal_name | | name history |
| URI | URI | data.gov.uk URI | string | metadata | | |

## Interpretation Notes

- The **full register** of live companies (~5.9M; part1 of 7 = 849,999 rows).
  **OGL**, monthly. Keyed on **CompanyNumber**.
- **No tax id / VAT** in the register (VAT is HMRC, separate).
- Dates are **DD/MM/YYYY**. Address is split across RegAddress.* — reassemble.
- `Accounts.AccountCategory` hints whether structured accounts exist (DORMANT/
  MICRO often have minimal data; SMALL/FULL have richer iXBRL in the accounts
  product).
- `sample_record.json` is a real register row (! LTD, 08209948), company-level.
