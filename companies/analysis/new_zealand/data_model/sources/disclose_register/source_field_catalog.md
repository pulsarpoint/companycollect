# Disclose Register (FMA) Field Catalog

> **PLANNING-ONLY.** The FMA Disclose Register lists FMC offers and managed
> investment schemes under the Financial Markets Conduct Act 2013, with public
> document search (financial statements, PDS, fund updates). Covers only **FMC
> reporting entities/issuers** — not all companies. Cataloged from public docs.

## Source Summary

- Country: New Zealand
- Source type: financial_disclosure
- Organization: Companies Office / Financial Markets Authority
- URL: https://disclose-register.companiesoffice.govt.nz/
- License: public register (Crown copyright)
- Access: public search
- Freshness: filing-driven
- Record shape: per-offer / per-scheme with documents
- Primary keys: `offer_number`
- Join keys: `nzbn`, `company_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| offer.offer_name | Offer/Scheme name | Offer name | string | legal_name | |
| offer.issuer | Issuer | Issuer entity | string | legal_name | join to NZBN/company number |
| offer.scheme_type | Scheme type | Type | string | metadata | MIS/equity/debt/KiwiSaver |
| offer.financial_statements[] | Financial statements | Audited statements | array | financial | PDF/XBRL; NZD |
| offer.product_disclosure_statement | PDS | Disclosure doc | object | document | |
| offer.fund_updates[] | Fund updates | Fund updates | array | document | |
| offer.financial_year | Balance date | Period | string | date | often 31 Mar |

## Interpretation Notes

- This is the **open route to NZ company financials**, but only for the **FMC
  reporting** subset (issuers, managed investment schemes, large/public-interest
  entities). The vast majority of NZ companies file no public financials.
- **Join**: the issuer links to a company (NZBN / company number) → join to the
  NZBN API and Companies Register.
- Financial statements are documents (PDF; some XBRL), currency **NZD**. Balance
  dates vary (commonly 31 March).
- Public search; no documented free bulk/API. No raw sample record.
