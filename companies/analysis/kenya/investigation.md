# Kenya — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Kenya, download/sample where allowed, and document a
reproducible trail.

## What was found

### 1. BRS — Business Registration Service (official registry; eCitizen, paid)

- The **BRS** (Office of the Registrar of Companies) is the official registrar for
  companies, business names, and other entities. `brs.go.ke` is the agency site;
  the **transactional** system is the **eCitizen** platform:
  - **`brs.ecitizen.go.ke`** (returned **403** to automated requests; login-gated)
    and `businessregistration.ecitizen.go.ke` — company/business-name **search** and
    **document** requests: **CR12** (directors & shareholders), **status report**,
    certified extracts, annual returns — **paid per transaction** (KES), behind an
    eCitizen account.
- There is **no open bulk register and no open API**. `brs.go.ke` has an
  "entities-registered" / research-publications section (aggregate stats), not a
  company-level dataset.

### 2. NSE — Nairobi Securities Exchange (listed companies + financials) — OPEN

- **`nse.co.ke/listed-companies/`** publishes the **listed-company directory**
  (public, HTML). **Verified live** — real issuers include **Absa Bank Kenya PLC**,
  **Stanbic Holdings Plc**, **Standard Chartered Bank Ltd**, **Diamond Trust Bank
  Kenya Ltd**, **Sasini Ltd**, **Williamson Tea Kenya Ltd**, **Car and General (K)
  Ltd**, **Kapchorua Tea Co. Ltd**, **Limuru Tea Co. Ltd**, **Eaagads Ltd**.
- NSE also publishes **listed-company announcements / financial results**, a
  market-data-overview, and market statistics (some market-data products are paid per
  a published pricelist). The WordPress REST root (`/wp-json/`) is reachable.
- **Listed companies only** (~60). Private-company financials are not here.

### 3. opendata.go.ke — Kenya Open Data (KODI) — no accessible company dataset

- **`www.opendata.go.ke`** (the Kenya Open Data Initiative) is reachable (HTTP 200)
  but is a small landing page. Standard **CKAN / DKAN / Socrata** catalog APIs
  returned **404** — no accessible **company-register dataset** could be confirmed.

### 4. Tax — KRA

- The **KRA** (Kenya Revenue Authority, `itax.kra.go.ke`) issues the **PIN** (tax id)
  for companies and individuals; VAT obligation is registered **under the PIN** (no
  separate VAT number). Per-company; not open bulk.

## Conclusion

Kenya's official registry (**BRS**) is delivered through **eCitizen** — company
search and documents (**CR12**, status reports, annual returns) are **login-gated and
paid**, with no open bulk/API. The one genuinely **open** source is the **NSE
listed-company directory** (verified live) plus listed announcements/financials.
**opendata.go.ke** has no accessible company dataset. So there is **no open bulk
register and no open private financials** — ingestion is `blocked_payment` (BRS) +
open-for-listed (NSE). Identifiers: **company registration number** (BRS), **BN**,
**KRA PIN** (tax). Currency **KES**. CR12 directors/shareholders are personal data
(Data Protection Act 2019) — redact. No access controls were bypassed; the sample
uses **NSE-verified + public-knowledge listed companies with null BRS identifiers**
(nothing fabricated).
