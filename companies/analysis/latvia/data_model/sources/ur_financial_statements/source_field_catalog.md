# Gada pārskatu finanšu dati — Annual report financial data Field Catalog

## Source Summary

- Country: Latvia
- Source type: official_financial_disclosure
- Organization: Latvijas Republikas Uzņēmumu reģistrs (UR)
- URL: https://data.gov.lv/dati/lv/dataset/gada-parskatu-finansu-dati
- License: CC0-1.0 (public domain)
- Access: public (free)
- Freshness: annual filing
- Record shape: four joined CSVs (financial_statements + balance_sheets + income_statements + cash_flow_statements)
- Primary keys: `id` (statement)
- Join keys: `legal_entity_registration_number` (→ register), `id` / `file_id` (→ statement parts)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| financial_statements.id | id | Statement id | string | identifier | 709390 | = parts.statement_id |
| financial_statements.file_id | file_id | Submission file id | string | identifier | 16544390 | alt join |
| financial_statements.legal_entity_registration_number | legal_entity_registration_number | Filing company regcode | string | identifier | 40103504912 | → register |
| financial_statements.year | year | Report year | integer | date | 2016 | + period dates |
| financial_statements.employees | employees | Employees | integer | employment | 3 | distinctive |
| financial_statements.currency | currency | Currency | string | metadata | EUR | + rounded_to_nearest |
| balance_sheets.total_assets | total_assets | Total assets | decimal | financial | 5031 | |
| balance_sheets.equity | equity | Equity | decimal | financial | -15283 | can be negative |
| balance_sheets.total_current_assets | total_current_assets | Current assets | decimal | financial | 5031 | + liabilities/fixed/intangible |
| income_statements.net_turnover | net_turnover | Revenue | decimal | financial | 135 | |
| income_statements.income_before_income_taxes | income_before_income_taxes | Pre-tax result | decimal | financial | -3860 | |
| income_statements.net_income | net_income | Net income | decimal | financial | -3860 | can be negative |
| income_statements.by_function_gross_profit | by_function_gross_profit | Gross profit | decimal | financial | -2947 | by_function/by_nature |
| cash_flow_statements.* | cash_flow_statements | Cash flow | object | financial | — | optional 4th part |

## Interpretation Notes

- **Structured open financial data — rare and excellent.** Unlike most countries (PDF), Latvia publishes the
  financial-statement **line items** as open CSV under **CC0**. Four parts:
  1. **financial_statements** (verified: **1,970,094 reports**): report metadata — `id`, `file_id`,
     `legal_entity_registration_number` (= regcode), `year` + period, **`employees`**, `currency`,
     `rounded_to_nearest`.
  2. **balance_sheets**: cash, receivables, inventories, total_current_assets, fixed/intangible assets,
     total_non_current_assets, **total_assets**, current/non_current_liabilities, provisions, **equity**,
     total_equities.
  3. **income_statements**: **net_turnover**, `by_nature_*` and `by_function_*` expense breakdowns, gross_profit,
     interest, income_before/after_income_taxes, **net_income**.
  4. **cash_flow_statements**: operating/investing/financing flows.
- **Join model.** `balance_sheets.statement_id = financial_statements.id` (and `file_id`); then
  `financial_statements.legal_entity_registration_number` → `register.regcode`. Pivot the parts per report into a
  yearly statement.
- **Currency.** EUR; pre-2014 reports may be LVL. Apply `rounded_to_nearest` (e.g. ONES) to values. Employee
  counts are a distinctive open field. No raw `sample_record.json` (data is multi-file row-per-statement); the
  examples above are verbatim from the downloaded files.
