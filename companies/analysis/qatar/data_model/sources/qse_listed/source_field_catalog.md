# Qatar Stock Exchange (QSE) — listed securities Field Catalog

## Source Summary

- Country: Qatar
- Source type: financial_disclosure
- Organization: Qatar Stock Exchange (QSE / Bourse de Doha)
- URL: https://www.qe.com.qa/listed-securities
- License: public disclosure
- Access: **browser-public** (Liferay portal; portlet AJAX, no clean open JSON API)
- Freshness: event-driven / quarterly
- Record shape: listed-company profile (browser-public, AJAX)
- Primary keys: symbol
- Join keys: symbol, isin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | Qatar National Bank (Q.P.S.C.) | listed only |
| symbol | Symbol | QSE ticker | string | identifier | QNBK, IQCD, QIBK | listed key |
| isin | ISIN | Securities id | string | identifier | QA0006929238 | Qatari ISINs start QA |
| sector | Sector | QSE sector | string | activity | Banks & Financial Services | |
| financial_statements | Financial Statements | Financials | array | financial |  | QAR; listed only |
| disclosures | Disclosures | Announcements | array | filing |  | AJAX-loaded |

## Interpretation Notes

- **Qatar Stock Exchange** publishes **listed-company** profiles, sector, disclosures, and
  financial statements (QAR). The directory is **browser-public** (Liferay portal), but the
  data is loaded via **portlet AJAX** — guessed JSON endpoints (`/api/markets/marketWatch`,
  `/api/markets/companies`) returned **404**, so there is **no clean open JSON API**.
- **Join**: the **QSE ticker symbol** / **ISIN** (`QA…`) keys the listed entity; join to
  the MoCI onshore registry by company name (no shared numeric key with the CR number).
- **Scope**: listed companies only; private-company financials are not public.
- Example values are **public-knowledge** QSE identities (QNBK = Qatar National Bank;
  IQCD = Industries Qatar; QIBK = Qatar Islamic Bank; ORDS = Ooredoo). ISINs listed are the
  widely-published Qatari ISINs for those issuers.
