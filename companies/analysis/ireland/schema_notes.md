# Ireland — Schema Notes

One authoritative open source (CRO Open Data Portal, CC-BY 4.0) with two datasets, both keyed on the **CRO
number** (`company_num`). Company Records = full identity; Financial Statements = filings index (figures behind
pay-per-call PDFs).

## Identifiers
- **CRO number** (`company_num`) — registration number; the universal join key (numeric; keep as string).
- **VAT number** — `IE` + 7 digits + 1–2 letters (Revenue). **Not** in the CRO open data → source via VIES/Revenue.
- **Eircode** — Irish postcode (in Company Records).
- **NACE Rev.2** (`nace_v2_code`) — activity code (values carry a trailing `.0`, e.g. `3101.0`).

## Company Records (companies.csv) — observed fields
```
company_num            - CRO number (company id)
company_name           - legal name
company_status_code    - status code (e.g. 1151)
company_status         - status text (e.g. "Normal", "Dissolved", "Strike off ...")
company_type_code      - type code (e.g. 1153)
company_type           - type text (e.g. "LTD - Private Company Limited by Shares")
company_reg_date       - registration / incorporation date (YYYY-MM-DD)
last_ar_date           - last annual return date
company_address_1..4   - registered address lines
comp_dissolved_date    - dissolution date (if dissolved)
nard                   - next annual return date
last_accounts_date     - date accounts last made up to / filed
company_status_date    - date of the current status
nace_v2_code           - NACE Rev.2 activity code (trailing .0)
eircode                - Irish postcode
company_name_eff_date  - name effective date
company_type_eff_date  - type effective date
princ_object_code      - principal objects code
```
817,068 rows (current + dissolved). Daily snapshot.

## Financial Statements (financial_statements_{year}.csv) — observed fields
```
file_name                       - PDF file name of the filed accounts (e.g. 125210782.pdf)
company_num                     - CRO number (join key)
company_name                    - legal name
submission_num                  - submission number (e.g. SR1746845)
submission_rec_date             - date the submission was received
submission_eff_date             - effective date
submission_reg_date             - registered date
submissions_accounts_to_date    - accounting period end ("accounts made up to")
```
121,387 filings (2023). Per-year files (2022, 2023, ...). This is the FILINGS INDEX; the figures are in the PDFs
(pay-per-call document retrieval).

## Financial statement documents (PDF) — pay-per-call
```
balance sheet, profit & loss / income statement, notes, directors' report, auditor's report (where applicable)
abridged accounts for small/micro companies ; currency EUR
```
- Retrieved via the CRO document-retrieval feature (registered account, pay-per-call). Structured figures need
  OCR/parse or a commercial provider. Join via file_name / submission_num + company_num.

## Mapping to internal company model
```
company_id          <- company_num (CRO number)
registration_number <- company_num
tax_id              <- not in CRO open data (source VAT separately)
vat_id              <- IE + 7 digits + letters (VIES/Revenue; not in CRO data)
legal_name          <- company_name
company_type        <- company_type (+ company_type_code)
status              <- company_status (+ company_status_code, company_status_date)
incorporation_date  <- company_reg_date
dissolution_date    <- comp_dissolved_date
registered_address  <- company_address_1..4
eircode             <- eircode
activity_code       <- nace_v2_code (strip trailing .0)
last_accounts_date  <- last_accounts_date
next_annual_return  <- nard
financial_filings[] <- Financial Statements index (submission_num, file_name, accounts-to date)
financials[]        <- financial-statement PDFs (pay-per-call; OCR/parse) | commercial provider [EUR]
officers[]          <- not in open data (in filed documents) [PII]
beneficial_owners[] <- RBO (restricted) [PII]
country             <- "Ireland"
source_url/name/at, raw_record
```
See `companies/data/ireland/normalized/companies.sample.jsonl` (real record: CRO 784992, SILVACRAFT FURNITURE LIMITED).
