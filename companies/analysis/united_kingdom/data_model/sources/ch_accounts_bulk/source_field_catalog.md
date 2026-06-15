# Companies House — Accounts Bulk Data (iXBRL) Field Catalog

## Source Summary

- Country: United Kingdom
- Source type: official_financial
- Organization: Companies House
- URL: http://download.companieshouse.gov.uk/en_accountsdata.html (daily) + en_monthlyaccountsdata.html
- License: Open Government Licence (OGL)
- Access: public
- Freshness: daily (last 60 days) + monthly (rolling year)
- Record shape: ZIP of `Prod223_<run>_<companynumber>_<madeupto>.html` (iXBRL) + some `.xml` (XBRL)
- Primary keys: `company_number` + `made_up_to`
- Join keys: `company_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| bus:UKCompaniesHouseRegisteredNumber | (same) | Company number | string | identifier | 00009604 | join key (also in filename) |
| bus:EntityCurrentLegalOrRegisteredName | (same) | Filer name | string | legal_name | Hull & Humber Chamber… | |
| made_up_to | period end | Balance-sheet date | date | date | 2025-09-30 | filename + context |
| core:TurnoverRevenue | TurnoverRevenue | Turnover | decimal | financial | 1,615,243 | GBP |
| core:ProfitLoss | ProfitLoss | Profit/loss | decimal | financial | 221,523 | GBP |
| core:FixedAssets | FixedAssets | Fixed assets | decimal | financial | 1,619,290 | GBP |
| core:CashBankOnHand | CashBankOnHand | Cash | decimal | financial | 514,506 | GBP |
| core:NetCurrentAssetsLiabilities | (same) | Net current assets | decimal | financial | 4,163,394 | GBP |
| core:NetAssetsLiabilities | (same) | Net assets | decimal | financial | 5,782,684 | GBP |
| core:Equity | Equity | Equity | decimal | financial | 402,324 | GBP |

## Interpretation Notes

- **Structured financial statements** as **iXBRL** (HTML with embedded XBRL),
  tagged to the **FRC / UK GAAP taxonomy** (`core:` figures, `bus:` entity facts).
  OGL, free, daily + monthly. Join on **company number**.
- **Parsing**: extract `ix:nonFraction` (numeric) and `ix:nonNumeric` (text)
  elements by `@name`. Values are **GBP** with comma thousands. There are
  **multiple contexts** (current + prior period, instant vs duration) — select the
  fact for the reporting period via its `contextRef`.
- **Coverage**: **electronically-filed** accounts only (~60–75% of filings);
  paper/scanned PDFs excluded. The available tags depend on the accounts type
  (FULL/SMALL have more than MICRO/DORMANT).
- `sample_record.json` holds **real facts** for company 00009604 (Hull & Humber
  Chamber of Commerce), GBP.
