# Latvia — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Latvia Uzņēmumu reģistrs open data data.gov.lv company register CSV download annual reports gada pārskati financial statements API`
- Language: English + Latvian
- Why this query was tried: Identify the register + any open bulk/API + financials.
- Top relevant URLs:
  - https://www.ur.gov.lv/en/get-information/free-of-charge-services/open-data/
  - https://data.gov.lv/
  - https://www.opensanctions.org/datasets/lv_business_register/
- Result: UR publishes open data (CSV/XLSX/JSON/Parquet/SQLite/Postgres), unrestricted reuse (commercial + non-commercial).
- Decision: Hit the data.gov.lv CKAN API to find the exact datasets.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — data.gov.lv CKAN
- Query: organization_list; package_search?q=uzņēmumu reģistrs
- Result: UR org slug = `ur`; 35 datasets incl. `uz` (register), `gada-parskatu-finansu-dati` (financials), `patiesie-labuma-guveji` (beneficial owners), `equity-capitals`, insolvency/liquidations/reorganizations/historical names.
- Decision: Inspect the register, financials, and BO packages.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — package_show
- Query: package_show?id=uz / gada-parskatu-finansu-dati / patiesie-labuma-guveji
- Result: All CC0-1.0. Register = register.csv. Financials = 4 CSVs (financial_statements, balance_sheets, income_statements, cash_flow_statements). BO = beneficial_owners.csv.
- Decision: Download register + financial_statements; sample balance/income.

## Attempt 4
- Date/time: 2026-06-14
- Source: curl (live) — bulk downloads
- Query: GET register.csv; financial_statements.csv; range-sample balance_sheets.csv / income_statements.csv
- Result:
  - register.csv → 128 MB, 485,134 entities (regcode, name, type SIA/AS/IK, dates, address, atvk, sepa).
  - financial_statements.csv → 204 MB, 1,970,094 reports (regcode, year, employees, currency).
  - balance_sheets / income_statements → structured figures (total_assets, equity, net_turnover, net_income, ...).
- Decision: Fully-structured open financials confirmed. Saved + SHA-256 metadata; built a real normalized record (SIA, regcode 40103550818) joined to its financials.
