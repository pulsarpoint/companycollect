# SEDAR+ — Reporting-Issuer Financials Field Catalog

> **DOCUMENTED-ONLY / REPORTING ISSUERS ONLY.** Free access to public-company /
> fund filings incl. financial statements. The only open route to Canadian
> financials, but only the reporting-issuer population. Cataloged from public docs;
> no records retrieved.

## Source Summary

- Country: Canada
- Source type: official_financial
- Organization: Canadian Securities Administrators (CSA) — 13 provincial regulators
- URL: https://www.sedarplus.ca/
- License: public disclosure (open to view; redistribution per CSA terms)
- Access: public (per-issuer search)
- Freshness: annual/quarterly
- Record shape: per-issuer filing documents (PDF)
- Primary keys: `issuer` + `filing_date`
- Join keys: `business_number`, `corporation_number`, `name`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| financial_statements | financial statements | Audited/interim statements | object | financial | CAD; IFRS/ASPE; issuers only |
| mdna | MD&A | Management discussion | object | document | issuers only |
| annual_report | annual report / AIF | Annual report | document | document | issuers only |
| issuer_profile | reporting issuer | Issuer name + jurisdiction | object | identifier | Reporting Issuers List |

## Interpretation Notes

- The **free, open route to Canadian financials** — but only **reporting issuers**
  (public companies + investment funds), via the CSA's national SEDAR+ system.
  Per-issuer documents (PDF), CAD, IFRS/ASPE. **No clean open bulk API**; link the
  issuer to the **corporation number / BN** by name/jurisdiction.
- **Private-company financials are not public** in Canada — a structural gap for
  the vast majority of companies.
