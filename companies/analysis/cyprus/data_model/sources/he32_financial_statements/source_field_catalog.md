# HE32 annual return + audited financial statements (DRCIP) Field Catalog

> **Planning-only.** Paid (EUR 10 per company, detailed search) and document-based (scanned PDF). All fields
> below are described from public documentation of the filing, not from copied records. No `sample_record.json`
> is provided. No observed example values are copied (paid/restricted access).

## Source Summary

- Country: Cyprus
- Source type: official_financial_disclosure
- Organization: Department of Registrar of Companies and Intellectual Property (DRCIP)
- URL: https://www.companies.gov.cy/en/company-lifecycle/search-for-company-information
- License: Public (filed in the register); detailed search EUR 10 per company; no bulk redistribution implied
- Access: paid
- Freshness: annual filing
- Record shape: scanned PDF documents (HE32 annual return + audited financial statements) per company per year
- Primary keys: `registration_number`, `fiscal_year`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| annual_return.registration_number | registration_number | Filing company id | string | identifier | (none — paid) | join key |
| annual_return.shareholders[] | shareholders | Shareholders on the HE32 | array | ownership | (none — paid) | not in open CSV; PII |
| annual_return.directors[] | directors | Directors/secretary | array | person | (none — paid) | overlaps open officers |
| annual_return.share_capital | share_capital | Share capital snapshot | decimal | financial | (none — paid) | EUR |
| financial_statements.fiscal_year | fiscal_year | Accounting reference date | string | date | (none — paid) | per-year key |
| financial_statements.balance_sheet | balance_sheet | Assets/equity/liabilities | object | financial | (none — paid) | OCR required, EUR |
| financial_statements.income_statement | income_statement | Revenue/profit-loss | object | financial | (none — paid) | OCR required, EUR |
| financial_statements.notes | notes | Notes to the statements | string | financial | (none — paid) | free text |
| financial_statements.auditor_report | auditor_report | Auditor's opinion | object | document | (none — paid) | statements are audited |

## Interpretation Notes

- **Public but paid + document-based.** The HE32 annual return is filed **with** audited financial statements.
  They are publicly accessible only via a **detailed search costing EUR 10 per company**, delivered as
  **scanned PDFs** — there is **no XBRL/CSV of figures**. Structured financials require OCR/parsing or a
  commercial provider (see `commercial_aggregators`).
- **Shareholders live here, not in the open register.** The open DRCIP CSV names officers but not shareholders;
  the HE32 annual return is the route to a shareholder snapshot short of the restricted UBO register.
- **GDPR.** Shareholders/directors are personal data — planning-only; do not persist without a lawful basis.
- **Currency EUR.** Cyprus statements are in euro.
- **No bulk rights.** Each document is individually paid; no bulk redistribution is implied. Treat the whole
  source as planning-only until a lawful, paid acquisition path is in place.
