# CRO document retrieval (financial-statement PDFs) Field Catalog

> **Paid / document-based.** The filed financial-statement PDFs are retrieved pay-per-call by registered account
> holders. Fields described from public documentation; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Ireland
- Source type: official_financial_disclosure
- Organization: Companies Registration Office (CRO)
- URL: https://opendata.cro.ie/ (document retrieval; registered account, pay-per-call)
- License: public document (per-document fee)
- Access: paid
- Freshness: per filing
- Record shape: PDF document per submission (retrieved via file_name / submission_num)
- Primary keys: `submission_num`
- Join keys: `company_num`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| balance_sheet | balance sheet | Assets/equity/liabilities | object | financial | (paid PDF) | OCR; EUR; abridged for small co. |
| profit_and_loss | P&L / income statement | Revenue/result | object | financial | (paid PDF) | often omitted for abridged |
| notes | notes to the accounts | Notes | string | financial | (paid PDF) | free text |
| directors_auditor_report | directors'/auditor's report | Reports | object | document | (paid PDF) | DIRECTORS = PII |

## Interpretation Notes

- **The figures live here, behind a fee.** The open Financial-Statements index points (via `file_name` /
  `submission_num`) at the filed **PDF**, which is retrieved **pay-per-call** by a registered account holder.
  Structured financials require OCR/parsing the paid PDF or a commercial provider. **Small/micro companies file
  abridged accounts** (balance sheet + notes, often no P&L). Currency **EUR**.
- **Directors** appear in the directors'/auditor's report — **personal data (GDPR)**; the only route to officers
  for Ireland (Company Records has no officers, RBO is restricted).
- Join on `company_num` / `submission_num`.
