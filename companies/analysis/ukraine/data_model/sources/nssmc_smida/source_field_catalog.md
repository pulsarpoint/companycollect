# NSSMC / SMIDA — Securities-Issuer Disclosure & Financials Field Catalog

> **DOCUMENTED.** Securities-issuer financial statements and disclosures
> (stockmarket.gov.ua / smida.gov.ua). Open; covers listed/issuer companies.
> Cataloged from public docs; no records pulled.

## Source Summary

- Country: Ukraine
- Source type: official_financial
- Organization: NSSMC / SMIDA
- URL: https://stockmarket.gov.ua ; https://smida.gov.ua
- License: open (disclosure)
- Access: public
- Freshness: annual/periodic
- Record shape: issuer disclosure documents
- Primary keys: `edrpou`
- Join keys: `edrpou`

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| edrpou | EDRPOU | Issuer code | string | identifier | join to EDR |
| financial_statements | financial statements | Balance sheet + income statement | object | financial | issuers only; UAH |
| disclosures | disclosures | Annual reports / material events | array | filing | |

## Interpretation Notes

- The disclosure layer for **securities issuers** — financial statements +
  regular/special disclosures, keyed on **EDRPOU**. Overlaps the XBRL FRS for IFRS
  filers; useful for issuer-specific reports and annual disclosures. Covers a
  small issuer population, not the broad company universe.
