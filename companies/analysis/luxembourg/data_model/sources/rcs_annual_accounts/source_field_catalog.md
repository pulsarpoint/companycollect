# RCS annual accounts (comptes annuels / eCDF) Field Catalog

> **Document-based but FREE.** Annual accounts are free to download per company on the RCS page (PDF / eCDF). No
> open structured bulk, no `sample_record.json` (figures are in the PDFs). Access via the captcha-gated RCS page.

## Source Summary

- Country: Luxembourg
- Source type: official_financial_disclosure
- Organization: Luxembourg Business Registers (LBR) / Centrale des bilans (STATEC/BCL)
- URL: https://www.lbr.lu/ (company page → comptes annuels)
- License: public (filed in the register; free to download per company)
- Access: public (free per company; via the captcha-gated RCS page)
- Freshness: annual filing
- Record shape: PDF / eCDF documents per fiscal year
- Primary keys: `rcs_number`, `fiscal_year`
- Join keys: `rcs_number`, `matricule`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| bilan | bilan | Balance sheet | object | financial | (PDF) | OCR; EUR |
| compte_profits_pertes | compte de profits et pertes | Income statement | object | financial | (PDF) | OCR; abridged for small co. |
| annexes | annexes | Notes | string | financial | (PDF) | free text |
| fiscal_year | exercice | Fiscal year | string | date | (PDF) | per-year key |
| filing_format | format (eCDF) | eCDF vs PDF | string | metadata | (PDF) | eCDF = structured |

## Interpretation Notes

- **Public, free, but document-based.** Companies file annual accounts (**comptes annuels**: bilan + compte de
  profits et pertes + annexes) via the structured **eCDF** format. They are **public and free to download** on
  the company's RCS page, usually as **PDF**. **Small companies file abridged accounts.** Currency **EUR**.
- **No open structured bulk.** The eCDF structured data is collected centrally (Centrale des bilans, STATEC/BCL)
  but **not published openly**. Structured financials at scale need OCR/parsing of the PDFs (eCDF filings parse
  more reliably) or a **commercial provider**.
- **Access** is via the same RCS page (captcha-gated for automation). Join on **rcs_number** / matricule + fiscal
  year.
