# ASIC Financial Reports Field Catalog

> **PLANNING-ONLY / PAID.** Company financial reports lodged with ASIC, bought per
> document via ASIC Connect. Cataloged from public docs; no records retrieved.

## Source Summary

- Country: Australia
- Source type: official_financial
- Organization: ASIC
- URL: https://www.asic.gov.au/for-business-and-companies/companies/company-financial-reports/
- License: paid (per document)
- Access: paid
- Freshness: annual
- Record shape: planning-only (PDF)
- Primary keys: `ACN`
- Join keys: `ACN`, `ABN`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| financial_report | balance sheet / income statement / cash flow | Audited annual report | object | financial | planning-only; AUD; AASB/IFRS |
| lodgement_meta | lodgement | Year + lodgement date | object | date | planning-only |

## Interpretation Notes

- The route to **company financials**, but **paid per document** and **only for
  lodging companies** — public companies, **large proprietary** companies,
  disclosing entities, registered schemes. **Small proprietary companies generally
  do not lodge** publicly, so most companies have no financials here. Join on ACN.
  Keep planning-only. For listed issuers, prefer the free **ASX** route.
