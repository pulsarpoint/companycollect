# SEC Express System — company documents Field Catalog

## Source Summary

- Country: Philippines
- Source type: official_registry
- Organization: Securities and Exchange Commission (SEC)
- URL: https://secexpress.ph/
- License: paid per document
- Access: **paid** (per document, PHP); companies file via eFAST (login)
- Freshness: live register
- Record shape: paid PDF documents (GIS / AFS / Articles / certificates)
- Primary keys: sec_registration_number
- Join keys: sec_registration_number, tin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Company name | string | legal_name |  | Inc./Corp./OPC/Partnership |
| sec_registration_number | SEC Registration Number | SEC reg number | string | identifier |  | primary key |
| tin | TIN | BIR tax id | string | identifier |  | tax join |
| company_type | Company Type | Type | string | legal_form |  | Stock/Non-stock/OPC |
| status | Status | Status | string | status |  | Active/Revoked/... |
| incorporation_date | Date of Incorporation | Incorporation date | date | date |  | |
| registered_address | Principal Office Address | Registered office | string | address |  | |
| primary_purpose | Primary Purpose | Line of business | string | activity |  | |
| authorized_capital | Authorized Capital Stock | Authorised capital | decimal | financial |  | PHP |
| paid_up_capital | Paid-up Capital | Paid-up capital | decimal | financial |  | PHP |
| directors | Directors / Trustees | Directors | array | person |  | PERSONAL DATA — redact |
| officers | Officers | Officers | array | person |  | PERSONAL DATA — redact |
| stockholders | Stockholders | Stockholders | array | ownership |  | PERSONAL DATA — redact |

## Interpretation Notes

- The **SEC** is the official registrar for corporations and partnerships. The
  public route to company documents is the **SEC Express System** (`secexpress.ph`),
  which sells, **per document (PHP)**:
  - **General Information Sheet (GIS)** — directors/trustees, officers, stockholders,
    and capital structure;
  - **Audited Financial Statements (AFS)** — annual financials (see notes below);
  - **Articles of Incorporation** and certificates.
  Companies file these via **eFAST** (`efast.sec.gov.ph`, login). The SEC main site
  (`sec.gov.ph`) is **WAF-blocked**.
- The field model above is from **public product descriptions**; **no real company
  values are copied**. There is **no open bulk register/API**.
- **Identifiers**: the **SEC Registration Number** is the corporate company id; the
  **TIN** (BIR) links tax.
- **Capital** (authorised / paid-up, PHP) comes from the GIS; full **financial
  statements** are the separate **AFS** (also paid).
- **Personal data**: directors, officers, and stockholders are personal data under
  the **Data Privacy Act of 2012 (RA 10173)** — redact.
- Implementation is **blocked on payment**; planning-only.
