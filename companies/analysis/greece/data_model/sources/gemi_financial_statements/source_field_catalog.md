# GEMI financial statements (ισολογισμοί / οικονομικές καταστάσεις) Field Catalog

> **Document-based.** Financial statements are filed to GEMI and viewable on the company page as **PDF**. No
> structured open figures, no `sample_record.json`. Same portal access constraints (reCAPTCHA/rate limits).

## Source Summary

- Country: Greece
- Source type: official_financial_disclosure
- Organization: GEMI / Γενική Γραμματεία Εμπορίου
- URL: https://publicity.businessportal.gr/ (company page → filings)
- License: public (filed in the register); document-based
- Access: public
- Freshness: annual filing
- Record shape: PDF documents on the company page (per fiscal year)
- Primary keys: `gemi_number`, `fiscal_year`
- Join keys: `gemi_number`, `afm`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| isologismos.balance_sheet | ισολογισμός | Balance sheet | object | financial | (PDF) | OCR; EUR |
| oikonomikes_katastaseis.income_statement | κατάσταση αποτελεσμάτων | Income statement | object | financial | (PDF) | OCR; EUR |
| fiscal_year | χρήση | Fiscal year | string | date | (PDF) | per-year key |
| accounting_standard | πρότυπα (ΕΛΠ/IFRS) | Accounting standard | string | financial | (PDF) | ΕΛΠ or IFRS |
| ga_resolution | απόφαση ΓΣ έγκρισης | GA resolution approving accounts | document | filing | (PDF) | filing metadata |

## Interpretation Notes

- **Public but document-based.** Greek companies must publish annual financial statements (under **ΕΛΠ** Greek
  GAAP or **IFRS**) and balance sheets (**ισολογισμοί**) to GEMI. They appear on the company's GEMI page as
  **PDF documents** — there is **no XBRL/CSV** of the figures. Structured financials require OCR/parsing or a
  commercial provider.
- **Access.** Reached via the company's GEMI page → same reCAPTCHA/rate-limit constraints as `gemi_portal`.
- **Join** on GEMI number / ΑΦΜ + fiscal year. Currency **EUR**.
