# SARAS Reporting Portal (reportal.ge) Field Catalog

## Source Summary

- Country: Georgia
- Source type: financial_disclosure
- Organization: Service for Accounting, Reporting and Auditing Supervision (SARAS), MoF
- URL: https://reportal.ge/en/Reports
- License: public disclosure (license unconfirmed)
- Access: **public via browser; anti-forgery-token-gated** for automation
- Freshness: annual
- Record shape: company with filed reports (browser-public)
- Primary keys: identification_code
- Join keys: identification_code, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| identification_code | Identification | 9-digit ID code | string | identifier | 110782780 | **join key to NAPR** |
| company_name | orgName | Entity name | string | legal_name |  | search param |
| legal_form | legalFormId | Legal form | string | legal_form | შპს (LLC) | filter |
| nace_codes | naceCodes | NACE activity | array | activity |  | NACE Rev.2 |
| reporting_year | year | Reporting year | integer | date | 2023 | filter |
| financial_statements_pdf | Financial Statements | Filed financials | string | financial |  | PDF; GEL |
| management_report_pdf | Management Report | Management report | string | document |  | PDF |

## Interpretation Notes

- **reportal.ge** is the official **SARAS Reporting Portal** ("ანგარიშგების პორტალი") where
  Georgian reporting entities (public-interest entities, large/medium companies) file
  **annual financial statements + management reports**, freely viewable. Search is by
  **identification code** or **name**.
- **Access**: browser-public, but **automation is gated** — the simple search
  `POST /en/Base/Search` requires an `__RequestVerificationToken` (anti-forgery), and the
  detailed search is a `GET` form to `/en/Reports/List` (params `orgName`, `year`,
  `legalFormId`, `catgoryId`, `naceCodes`) whose guessed URLs returned 404 in testing. An
  API host (`rms.reportal.ge`) exists but guessed endpoints 404'd. Implement with
  token/session handling. One identification code (`110782780`, a შპს/LLC) was observed on
  the Reports page; no full per-company record was captured.
- **Join**: the **9-digit identification code** keys to NAPR. **Financials** are inside the
  filed **PDF** (currency GEL), not structured fields. **Language** Georgian + English.
- No `sample_record.json`: only a search/landing page was retrieved (no structured per-company record).
