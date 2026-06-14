# Ireland — Search Attempts Log

## Attempt 1
- Date/time: 2026-06-14
- Search engine or source: WebSearch
- Query: `Ireland CRO Companies Registration Office open data API services.cro.ie company search free bulk download financial statements annual return data.gov.ie`
- Language: English
- Why this query was tried: Identify the authoritative register + any open bulk/API + financials.
- Top relevant URLs:
  - https://opendata.cro.ie/dataset/companies
  - https://cro.ie/the-companies-registration-office-cro-announces-the-launch-of-new-open-data-portal/
  - https://data.gov.ie/dataset/companies
- Result: CRO launched an Open Data Portal (opendata.cro.ie) in late 2024 under CC-BY 4.0, bulk + API, with Company Records + Financial Statements. Basic data free; document retrieval pay-per-call.
- Decision: Hit the CKAN API at opendata.cro.ie and download the datasets.

## Attempt 2
- Date/time: 2026-06-14
- Source: curl (live) — CKAN API
- Query: GET /api/3/action/package_list ; package_show?id=companies / financial-statements
- Result: packages ["companies","financial-statements"], both CC-BY 4.0. Company Records = companies.csv.zip; Financial Statements = financial_statements.csv + financial_statements_2023.csv.
- Decision: Download both.

## Attempt 3
- Date/time: 2026-06-14
- Source: curl (live) — bulk downloads
- Query: GET companies.csv.zip ; financial_statements_2023.csv
- Result:
  - companies.csv.zip → 46.7 MB (193 MB csv), 817,068 companies; rich fields incl. nace_v2_code, eircode, status, type, dates, nard.
  - financial_statements_2023.csv → 121,387 filings; fields file_name(PDF), company_num, submission_num, dates, accounts-to date (the FILINGS INDEX; figures are in the PDFs).
- Decision: Company Records = recommended spine; Financial Statements index = recommended; the PDFs are pay-per-call (blocked_by_payment). Saved + SHA-256 metadata; built a real normalized record (CRO 784992).

## Attempt 4
- Date/time: 2026-06-14
- Source: WebSearch + documentation
- Query: CRO document retrieval / RBO / VAT
- Result: financial-statement PDFs retrieved pay-per-call by registered account holders; RBO (beneficial ownership) restricted post-CJEU; VAT (IE…) not in CRO data → VIES/Revenue.
- Decision: cro_document_retrieval = blocked_by_payment; rbo_register = blocked_by_authentication; vies_vat = useful_secondary. data.gov.ie mirrors CRO.
