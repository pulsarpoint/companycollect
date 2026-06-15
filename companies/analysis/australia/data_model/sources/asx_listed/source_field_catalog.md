# ASX — Listed-Company Financials Field Catalog

> **DOCUMENTED-ONLY / LISTED ISSUERS ONLY.** Listed issuers' financial reports +
> announcements, publicly viewable per company (PDF). The only free route to
> Australian financials, but only the listed population. Cataloged from public
> docs; no records retrieved.

## Source Summary

- Country: Australia
- Source type: stock_exchange
- Organization: Australian Securities Exchange (ASX)
- URL: https://www.asx.com.au/
- License: issuer disclosure (open to view; redistribution per ASX/issuer terms)
- Access: public (per-issuer)
- Freshness: periodic (annual/half-year)
- Record shape: per-issuer reports (PDF)
- Primary keys: `asx_ticker`
- Join keys: `acn`, `abn`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| asx_ticker | ASX code | Listing code | string | identifier | listed only |
| financial_report | annual/half-year report | Financial statements | object | financial | AUD; AASB/IFRS |
| announcements[] | announcements | Market announcements | array | document | listed only |

## Interpretation Notes

- The **free, open route to Australian financials**, but **listed issuers only**
  (a few thousand). Per-issuer PDFs (AUD, AASB/IFRS). Link the **ASX ticker** to
  the **ACN/ABN** by issuer to join with the ABR. Non-listed financials require
  paid ASIC (and most small companies don't lodge at all).
