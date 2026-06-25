# EGX — Egyptian Exchange (listed companies + financials) Field Catalog

## Source Summary

- Country: Egypt
- Source type: financial_disclosure
- Organization: The Egyptian Exchange (EGX)
- URL: https://www.egx.com.eg/en/ListedStocks.aspx
- License: public disclosure
- Access: **public via browser; WAF-gated** for automation
- Freshness: event-driven / quarterly
- Record shape: listed-company profile (browser-public; WAF-gated API)
- Primary keys: egx_symbol
- Join keys: egx_symbol, isin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Listed company name | string | legal_name | Commercial International Bank (Egypt) S.A.E. | listed only |
| egx_symbol | Symbol | EGX stock symbol | string | identifier | COMI, TMGH, SWDY | listed key |
| isin | ISIN | Securities id | string | identifier | EG... | Egyptian ISINs start EG |
| sector | Sector / Industry | EGX sector | string | activity | Banks, Real Estate | |
| disclosures | Disclosures | Filings | array | filing |  | WAF-gated |
| financial_statements | Financial Statements | Financials | array | financial |  | EGP; listed only |

## Interpretation Notes

- **EGX** publishes **listed-company** profiles, disclosures, and financial
  statements. The `ListedStocks.aspx` and `companiesprofilesearch.aspx` pages **load
  in a browser**, but the underlying data endpoints (`getinformation.aspx?type=…`)
  returned **"Request Rejected" (WAF)** to automated requests. So EGX is **public via
  the browser** but **WAF-gated** for automation — **not bypassed**; fields are
  documented from the page model + public knowledge (e.g. COMI = Commercial
  International Bank; TMGH = Talaat Moustafa Group; SWDY = Elsewedy Electric).
- **Join**: the **EGX symbol** / **ISIN** keys the listed entity; join to GAFI /
  Commercial Registry by company name (the commercial registry number / Tax ID are
  not on EGX — those are gated registry sources).
- **Scope**: listed companies only. Currency **EGP**. No personal data at the
  directory level.
