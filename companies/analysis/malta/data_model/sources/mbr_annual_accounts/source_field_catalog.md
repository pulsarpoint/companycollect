# MBR annual accounts / annual return (financial statements) Field Catalog

> **Paid / document-based.** Annual accounts + annual return are obtained as paid documents (EUR 5–25) from the
> MBR. Fields described from public documentation; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Malta
- Source type: official_financial_disclosure
- Organization: Malta Business Registry (MBR)
- URL: https://mbr.mt/ (document purchase)
- License: public (filed; document purchase EUR 5–25)
- Access: paid
- Freshness: annual filing
- Record shape: PDF documents per company per fiscal year
- Primary keys: `registration_number`, `fiscal_year`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| balance_sheet | balance sheet | Assets/equity/liabilities | object | financial | (paid PDF) | OCR; EUR; IFRS/GAPSME |
| profit_and_loss | profit and loss account | Revenue/result | object | financial | (paid PDF) | abridged for small co. |
| notes | notes to the accounts | Notes | string | financial | (paid PDF) | free text |
| directors_auditor_report | directors'/auditor's report | Reports | object | document | (paid PDF) | directors = PII |
| annual_return | annual return | Capital/shareholders/officers snapshot | object | filing | (paid) | PII |
| fiscal_year | accounting period | Fiscal year | string | date | (paid) | per-year key |

## Interpretation Notes

- **Public but paid + document-based.** Companies file **annual accounts** (under the Companies Act; **IFRS** or
  **GAPSME** for small companies) plus an **annual return** to the MBR. They are public but obtained as **paid
  documents** (EUR 5–25), usually **PDF**. **Small companies file abridged accounts.** Currency **EUR**.
- **No open structured bulk.** Structured figures need OCR/parsing the paid PDFs, the **paid MBR API**, or a
  **commercial provider**.
- The **annual return** is a useful per-year snapshot of capital, shareholders and officers. Join on
  **registration_number** + fiscal year.
