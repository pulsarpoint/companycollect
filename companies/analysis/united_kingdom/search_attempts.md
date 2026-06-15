# United Kingdom — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Companies House basic company data bulk download CSV BasicCompanyData URL Open Government Licence`
- Result: Free Company Data Product — CSV ZIP (one file or split parts), ~5M companies, OGL/CC-BY, at download.companieshouse.gov.uk.
- Decision: get the exact current filename/URL.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Companies House accounts data product bulk iXBRL financial statements download free open data`
- Result: Free Accounts Data Product — iXBRL/.html + XBRL/.xml, daily (60-day window) + monthly; electronically-filed accounts (~60–75%); free REST API (600 req/5min).
- Decision: download a daily accounts zip + a basic-data part.

## Attempt 3
- Date/time: 2026-06-15
- Source: curl
- Query: HEAD/GET BasicCompanyData-2026-06-01-part1_6.zip
- Result: **404** (the split is part1_7, not part1_6).
- Decision: fetch the index page for exact filenames.

## Attempt 4
- Date/time: 2026-06-15
- Source: WebFetch (en_output.html + en_accountsdata.html)
- Result: basic data = `BasicCompanyDataAsOneFile-2026-06-01.zip` (470 MB) or `BasicCompanyData-2026-06-01-part{1..7}_7.zip` (~70 MB). Accounts = `Accounts_Bulk_Data-2026-06-10.zip` (daily).
- Decision: download part1_7 + the 2026-06-10 accounts zip.

## Attempt 5
- Date/time: 2026-06-15
- Source: curl + python
- Query: download + inspect both
- Result: basic part1 = **849,999** rows, **55 columns** (identity, address, category, status, dates, SIC, accounts metadata, charges, previous names). Accounts zip = **9,717** iXBRL files named `Prod223_<run>_<companynumber>_<madeupto>.html`.
- Decision: confirm structured financials inside the iXBRL.

## Attempt 6
- Date/time: 2026-06-15
- Source: python (iXBRL parse)
- Query: company 00009604 accounts
- Result: real FRC-taxonomy facts — TurnoverRevenue 1,615,243; ProfitLoss 221,523; FixedAssets 1,619,290; NetAssets 5,782,684; Equity 402,324 (GBP); `bus:UKCompaniesHouseRegisteredNumber`=00009604. 212 numeric facts.
- Decision: Accounts Bulk Data = recommended financials; join on company number.
