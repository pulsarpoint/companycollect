# Philippines — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in the Philippines, download/sample where allowed, and document
a reproducible trail.

## What was found

### 1. SEC — Securities and Exchange Commission (official registry; PAID documents)

- **SEC** is the registrar for corporations and partnerships. The main site
  `sec.gov.ph` is **WAF-blocked (HTTP 403)**. Company data flows through several
  systems:
  - **eFAST** (`efast.sec.gov.ph`, HTTP 200) — the login portal where companies file
    the **General Information Sheet (GIS)** (directors, officers, stockholders,
    capital structure) and **Audited Financial Statements (AFS)**. SPA / login.
  - **eSPARC** (`esparc.sec.gov.ph` → `/application`, HTTP 200) — company
    **registration** application (login), including the **One Person Corporation
    (OPC)**.
  - **SEC Express System** (`secexpress.ph`, HTTP 200) — order **GIS / AFS / Articles
    of Incorporation / certificates** for delivery — **paid per document** ("Fees and
    Charges"). This is the public route to company documents.
- There is **no open bulk company register** and **no open API**. SEC documents are
  **paid** via SEC Express.

### 2. PSE EDGE — listed-company data + financials (OPEN)

- **PSE EDGE** (`edge.pse.com.ph`) is open. The **company directory** search
  (`/companyDirectory/search.ax`, POST) returns real listed-company rows — **verified
  live**:
  - **PLDT Inc.**, symbol **TEL**, **Services / Telecommunications**, listed
    **Sep 17, 1953**.
  - Banks, holding firms, property, food/beverage issuers (e.g. Jollibee — Industrial
    / Food, Beverage & Tobacco, listed Jul 14, 1993).
- EDGE also hosts disclosures and **financial reports** for listed companies. **Listed
  companies only** (~280).

### 3. DTI BNRS — business name registration (sole proprietors)

- **DTI BNRS** (`bnrs.dti.gov.ph`, HTTP 200) registers **business names** for sole
  proprietorships, with a free **name search/verification**. Not a corporate register
  (that is SEC); useful for sole-proprietor existence checks.

### 4. data.gov.ph — national open-data portal (no accessible company dataset)

- **`data.gov.ph`** is now a **JS SPA** (Angular). Its landing page returns no static
  catalogue, and standard CKAN API paths returned the SPA shell (not JSON). No
  company-register dataset could be confirmed headless.

### 5. Tax — BIR

- The **BIR** issues the **TIN** (Tax Identification Number). VAT-registered
  businesses use the **TIN** (no separate VAT number). Per-company; not open bulk.

## Conclusion

The Philippines' official registry (**SEC**) is comprehensive but **commercially
distributed** — company documents (**GIS** with officers/stockholders/capital, **AFS**
financials, Articles) are **paid** via the **SEC Express System**, with eFAST/eSPARC
as login portals and the main site WAF-blocked. The one genuinely **open** source is
**PSE EDGE** for **listed companies** (verified live: PLDT/TEL) including their
financial reports. **data.gov.ph** has no accessible company dataset; **DTI BNRS**
covers sole-proprietor names (free search). So there is **no open bulk corporate
register and no open private financials**. Identifiers: **SEC Registration Number**,
**TIN** (BIR), **DTI BN** (sole props); no separate VAT number. Currency **PHP**.
GIS officers/stockholders are personal data (Data Privacy Act 2012) — redact. No
access controls were bypassed; the sample uses **PSE-verified + public-knowledge
listed companies with null SEC identifiers** (nothing fabricated).
