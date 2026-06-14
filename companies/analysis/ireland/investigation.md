# Ireland — Company Open Data Investigation

## Conclusion

Ireland is a **fully-open** country for company identity, and **open for the financial-filings index**, via the
**CRO Open Data Portal** (`opendata.cro.ie`), launched in late 2024 under **CC-BY 4.0** (Open Data Directive).
Everything joins on the **CRO number** (`company_num`). The actual financial-statement **figures** are inside
filed PDFs retrieved **pay-per-call**; VAT is not in the CRO data; beneficial ownership (RBO) is restricted.

## What was verified (live, with real downloads)

- **CKAN** `opendata.cro.ie/api/3/action/package_list` → `["companies", "financial-statements"]`. Both packages
  licensed **CC-BY 4.0**.
- **Company Records** `companies.csv.zip` → HTTP 200, **46.7 MB zip / 193 MB CSV**, **817,068 companies**.
  Columns: `company_num`, `company_name`, `company_status_code`, `company_status`, `company_type_code`,
  `company_type`, `company_reg_date`, `last_ar_date`, `company_address_1..4`, `comp_dissolved_date`, `nard`
  (next annual return date), `last_accounts_date`, `company_status_date`, `nace_v2_code`, `eircode`,
  `company_name_eff_date`, `company_type_eff_date`, `princ_object_code`.
  - Real example: `784992 SILVACRAFT FURNITURE LIMITED`, type "LTD - Private Company Limited by Shares",
    status "Normal", reg 2025-03-31, eircode N39 D880, NACE 3101.0.
- **Financial Statements** `financial_statements_2023.csv` → HTTP 200, **121,387 filings** (per-year files 2022,
  2023). Columns: `file_name` (PDF), `company_num`, `company_name`, `submission_num`, `submission_rec_date`,
  `submission_eff_date`, `submission_reg_date`, `submissions_accounts_to_date`.
  - Real example: `125210782.pdf  247182  GUILFOYLE TRUCK SALES LIMITED  SR1746845  2023-05-11 … accounts-to 2023-04-30`.
- **Document retrieval**: the actual financial-statement PDFs are available to **registered account holders on a
  pay-per-call basis** (not in the open CSV).

## Identifiers

- **CRO number** (`company_num`) — the registration number and the universal join key (also keys the financial
  filings).
- **VAT number** — `IE` + 7 digits + 1–2 letters (Revenue); **not** in the CRO open data → source via VIES/Revenue.
- **NACE Rev.2** (`nace_v2_code`) — activity classification (note values appear with a trailing `.0`).
- **Eircode** — Irish postcode.

## Financial data model

Two layers, both CC-BY 4.0:

```
Company Records (company_num, ..., last_accounts_date)         <- identity + when accounts were last filed
Financial Statements index (company_num, submission_num, file_name.pdf, dates, accounts-to date)  <- filings index
   └─ document retrieval (PDF) PAY-PER-CALL                     <- the actual figures (balance sheet + P&L + notes)
```

The open data gives you **which** companies filed accounts and **for which period** (the index); the **figures**
require fetching the PDF (paid, OCR/parse) or a commercial provider. Small/micro companies file **abridged**
accounts. Currency **EUR**.

## Recommended ingestion

Bulk-first: load Company Records (keyed on company_num) + the per-year Financial-Statements index; fetch
financial PDFs pay-per-call for the figures (or use a commercial provider). Use the CKAN API for incremental
updates (daily snapshot). Validate VAT separately via VIES.

## Risks / open questions

- **Financial figures are paid** (document retrieval pay-per-call) — the open dataset is the filings **index**,
  not the figures.
- **VAT not in CRO data** — must be sourced from Revenue/VIES.
- **Beneficial ownership (RBO)** is access-restricted (post-CJEU) — not open.
- **GDPR**: officers/directors are not in the open Company Records (they're in filed documents) — treat any
  person data fetched from documents under GDPR.
- **NACE values** carry a trailing `.0` — normalize. Large CSV (193 MB) — stream/chunk.
