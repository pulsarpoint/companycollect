# CRO Open Data Portal — Financial Statements (filings index) Field Catalog

## Source Summary

- Country: Ireland
- Source type: official_financial_disclosure
- Organization: Companies Registration Office (CRO)
- URL: https://opendata.cro.ie/dataset/financial-statements (download: financial_statements_{year}.csv)
- License: Creative Commons Attribution 4.0 (CC-BY-4.0)
- Access: public (free)
- Freshness: annual files (2022, 2023, …)
- Record shape: one row per filed financial-statement submission
- Primary keys: `submission_num`
- Join keys: `company_num`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| file_name | file_name | PDF file name | string | document | 125210782.pdf | paid retrieval pointer |
| company_num | company_num | CRO number | string | identifier | 247182 | join key |
| company_name | company_name | Company name | string | legal_name | GUILFOYLE TRUCK SALES LIMITED | |
| submission_num | submission_num | Submission number | string | filing | SR1746845 | filing id |
| submission_rec_date | submission_rec_date | Received date | date | date | 2023-05-11 | |
| submission_eff_date | submission_eff_date | Effective date | date | date | 2023-05-09 | |
| submission_reg_date | submission_reg_date | Registered date | date | date | 2023-05-16 | |
| submissions_accounts_to_date | submissions_accounts_to_date | Accounts made up to | date | date | 2023-04-30 | fiscal-year end |

## Interpretation Notes

- **An open INDEX of filings, not the figures.** Verified: **121,387 filings (2023)**; per-year CSV files (2022,
  2023). Each row is one filed accounts submission — **which company** (`company_num`), **which submission**
  (`submission_num`), **which PDF** (`file_name`), the filing dates, and the **accounts-to date** (fiscal-year
  end). **CC-BY 4.0**, free.
- **The figures are paid.** The actual balance-sheet / income-statement numbers are inside the filed **PDF**,
  retrieved via the CRO document-retrieval feature **pay-per-call** (see `cro_document_retrieval`). Structured
  financials therefore need the paid PDF (OCR/parse) or a commercial provider. Small/micro companies file
  **abridged** accounts. Currency EUR.
- **Join** on `company_num` (→ Company Records). Use `submission_num` to dedupe and `file_name` to request the
  document. No raw `sample_record.json` needed — the field examples above are verbatim from the downloaded 2023
  file.
