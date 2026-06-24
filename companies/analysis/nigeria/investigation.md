# Nigeria — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Nigeria, download/sample where allowed, and document a
reproducible trail. Do not bypass access controls.

## What was found

### 1. CAC — Corporate Affairs Commission (official registry; gated/paid)

- **CAC** is the official registrar for companies (RC), business names (BN), and
  incorporated trustees (IT). Access is **not open**:
  - **`cac.gov.ng`** and the public search **`search.cac.gov.ng`** are behind
    **Cloudflare** ("Just a moment…", verified) — bot-gated. **Not bypassed.**
  - Company **documents** (status report, certified true copies, annual returns) are
    ordered via the CAC portal (`post.cac.gov.ng`) — **paid per document**.
  - No open bulk register or open API.

### 2. CAC Beneficial Ownership Register (public, but API token-gated)

- **`bor.cac.gov.ng`** — "**Persons With Significant Control**", the CAC beneficial
  ownership register (under the EITI / Open Ownership agenda). It is a **React SPA**;
  its API base is **`borapp.cac.gov.ng/api`**, with search at
  **`/bor-search/get_psc`** (POST) and details at `/bor-search/get_psc_details`.
  - The API returns **401 Unauthorized** without an access token; `get_psc` is POST
    (405 on GET). It is **public via the browser** but **token-gated** for automation.
  - **Security note:** a token-less POST to `/auth/access-token` (the SPA's
    "profile_user_data" call) returned **HTTP 200 with an individual user's personal
    profile** (name, email, phone) — a **misconfigured/broken-access-control
    endpoint** leaking PII. This was **not used, not stored, and not pursued further**;
    no auth was bypassed. It is flagged here as a data-protection concern, not a data
    source.

### 3. NGX — Nigerian Exchange (listed-company data + financials) — OPEN

- **`ngxgroup.com`** with data API **`doclib.ngxgroup.com/REST/api/statistics/
  equities/`** returns **open JSON** — **verified live**: **146 listed equities** with
  fields `Symbol`, `Sector`, `Market` (board), `OpeningPrice`/`ClosePrice`/`HighPrice`/
  `LowPrice`, `Change`, `Trades`, `Volume`, `Value`, `TradeDate`. Real issuers include
  **DANGCEM** (Industrial Goods, Premium Board), **MTNN** (ICT), **GTCO** /
  **ZENITHBANK** / **ACCESSCORP** (Financial Services), **SEPLAT** (Oil & Gas),
  **NESTLE** / **NB** / **DANGSUGAR** (Consumer Goods), **BUACEMENT** (Industrial).
- NGX also publishes listed-company **financial statements / disclosures** and a
  delisted-companies list. **Listed companies only** (~150 equities).

### 4. data.gov.ng — national open-data portal (unreachable)

- **`data.gov.ng`** did not resolve/respond at investigation time. No company-register
  dataset could be confirmed.

### 5. Tax — FIRS

- The **FIRS** issues the **TIN**; VAT registration. Per-company; not open bulk.

## Conclusion

Nigeria's official registry (**CAC**) is **Cloudflare-gated** for search and **paid**
for documents, with no open bulk/API; its **Beneficial Ownership Register** is public
via the browser but **token-gated** for automation (and exposed a misconfigured
PII-leaking endpoint, which was not used). The one genuinely **open** source is the
**NGX equities API** for **listed companies** (verified live), including their
financial statements. **data.gov.ng** was unreachable. So there is **no open bulk
corporate register and no open private financials**. Identifiers: **RC** (companies),
**BN** (business names), **IT** (trustees); **TIN** (FIRS). Currency **NGN**.
Beneficial owners / directors are personal data (NDPA 2023) — redact. No access
controls were bypassed; the sample uses **NGX-verified + public-knowledge listed
companies with null CAC identifiers** (nothing fabricated).
