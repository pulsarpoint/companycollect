# Malaysia — company data investigation

## Goal

Find official/open sources for **company registry data** and **financial data** for
companies registered in Malaysia, download/sample where allowed, and document a
reproducible trail.

## What was found

### 1. SSM — Companies Commission of Malaysia (official registry; PAID)

- **SSM (Suruhanjaya Syarikat Malaysia)** is the official registrar for companies
  (Sdn. Bhd. / Bhd.), LLPs (PLT), and businesses (ROB). Its company and financial
  data are sold as **paid products** via two official channels:
  - **SSM e-Info** (`ssm-einfo.my`) — verified live (HTTP 200). Login via the SSM
    IDP (`idpro.ssm.com.my`, SAML SSO). Product catalogue (with published sample
    PDFs) includes **Company Profile (ROC)**, **Business Profile**, **LLP Profile**,
    **Audit Firm Profile**, **Financial Comparison** (2 / 3 / 5 / 10 years),
    **Financial Historical**, plus CTC (certified true copy) variants.
  - **MyData-SSM** (`mydata-ssm.com.my`) — verified live. "Buy SSM Report":
    **Company Profile** and **Company Financial Report**.
- A free **e-Search** exists for basic existence/name/number verification, but full
  **profiles and financials are paid** (per document, MYR). There is **no open
  bulk register and no open API**.

### 2. data.gov.my — national open-data portal (no company register)

- **`data.gov.my`** (the revamped OpenDOSM portal) works (HTTP 200) and exposes a
  JSON API (`api.data.gov.my/data-catalogue/?id=...`). But searches for `company`,
  `business`, `syarikat`, `registration` returned **no company register** — only
  **DOSM statistics** and generic datasets. The catalogue API returns 404 for
  company-style ids. So there is **no open company-level dataset**.

### 3. Bursa Malaysia — listed-company financials (WAF-blocked here)

- **`bursamalaysia.com`** publishes **listed-company** financial statements and
  announcements, but returned **HTTP 403** (WAF) for automated requests from this
  environment. Public via browser; listed companies only.

### 4. Tax — LHDN/HASIL & SST

- **HASIL/LHDN** (`mytax.hasil.gov.my`) administers the **TIN** (income tax number).
  Malaysia uses **SST** (Sales & Service Tax) since 2018 — **no VAT/GST**; taxable
  persons hold an **SST registration number**. Per-company; not open bulk.

## Conclusion

Malaysia's official registry (**SSM**) is comprehensive but **commercially
distributed** — company profiles and **financial statements** are **paid products**
on **e-Info** and **MyData-SSM**, with only a free **e-Search** for basic existence.
**data.gov.my** hosts **no company register** (DOSM statistics), and **Bursa**
(listed financials) is WAF-blocked here. There is **no open bulk register and no
open financials**. The realistic path is **paid SSM products per company**.
Identifiers: SSM **company registration number** (new 12-digit since 2019 / old
NNNNNNN-A), **TIN** (LHDN), **SST number** (no VAT). Currency **MYR**.
Directors/shareholders are personal data (PDPA 2010) — redact. No access controls
were bypassed; the sample uses **public-knowledge listed companies with null SSM
identifiers** (nothing fabricated).
