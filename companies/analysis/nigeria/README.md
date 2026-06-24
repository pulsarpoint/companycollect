# Company data sources for Nigeria

## Status

- Official bulk data: **not open** — CAC has no open bulk register
- Official API: **gated** — CAC search is Cloudflare-gated; documents are paid;
  the BO register API needs a token
- Open data portal: `data.gov.ng` was **unreachable** at investigation time
- License: CAC data is restricted/paid; NGX listed data is public
- Recommended ingestion path: **NGX equities API** for listed companies (open) +
  CAC documents (paid/per-company) for the rest

## Best source

The official registry is **CAC — Corporate Affairs Commission**, but it is not
openly accessible:

- **`cac.gov.ng`** and the public search **`search.cac.gov.ng`** are behind
  **Cloudflare** ("Just a moment…") — bot-gated.
- Company **documents** (status report, certified extract, annual returns) are
  obtained via the CAC portal — **paid per document**.
- The **Beneficial Ownership Register** (`bor.cac.gov.ng`, "Persons With Significant
  Control") is public via the browser, but its API (`borapp.cac.gov.ng/api/bor-search/
  get_psc`, POST) requires an **access token**.

The one genuinely **open** source is the **NGX (Nigerian Exchange) equities API**.

## Financial data

**NGX** (`ngxgroup.com`, data API `doclib.ngxgroup.com/REST/api/statistics/equities/`)
returns **open JSON** for **listed companies** — **verified live**: 146 listed
equities with **symbol, sector, market board, prices, volume, value** (e.g.
**DANGCEM**, **MTNN**, **GTCO**, **ZENITHBANK**, **SEPLAT**, **NESTLE**, **BUACEMENT**).
NGX also hosts listed-company **financial statements / disclosures**. **Private-company
financials** (AFS filed with CAC) are **not open** (paid, per company).

## Identifiers & tax

- **RC number** — Registration of Company number (limited companies).
- **BN number** — Business Name (sole proprietors / partnerships).
- **IT number** — Incorporated Trustees (NGOs / associations).
- **TIN** — Tax Identification Number (FIRS); **VAT** registration.
- Currency **NGN**. Language: English.

## Next action

Use the **NGX equities API** for listed companies (open) and CAC documents
(paid/per-company, Cloudflare-gated search) for the rest. There is **no open bulk
register**. The CAC BO register's beneficial-owner data is personal data (NDPA 2023)
— redact. **Note:** a token-less CAC BO endpoint was observed leaking an individual
user's profile (a misconfiguration) — it was **not used or stored**.
