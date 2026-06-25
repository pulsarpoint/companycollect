# BRS — Business Registration Service (eCitizen) Field Catalog

## Source Summary

- Country: Kenya
- Source type: official_registry
- Organization: Business Registration Service (Office of the Registrar of Companies)
- URL: https://brs.ecitizen.go.ke/
- License: paid per transaction
- Access: **eCitizen login + paid** (returned 403 to automation)
- Freshness: live register
- Record shape: per-company search + documents (CR12, status report)
- Primary keys: registration_number
- Join keys: registration_number, kra_pin, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| registration_number | Registration Number | BRS reg number | string | identifier |  | primary key; BN for business names |
| company_name | Company Name | Registered name | string | legal_name |  | |
| company_type | Company Type | Type | string | legal_form |  | Private/Public Ltd, CLG, BN, LLP |
| status | Status | Status | string | status |  | Active/Dormant/Dissolved/Struck off |
| registration_date | Registration Date | Registration date | date | date |  | |
| registered_address | Registered Office Address | Registered office | string | address |  | |
| nominal_capital | Nominal / Issued Capital | Capital | decimal | financial |  | KES |
| kra_pin | KRA PIN | Tax id | string | identifier |  | tax join |
| directors | Directors (CR12) | Directors | array | person |  | PERSONAL DATA — redact |
| shareholders | Shareholders (CR12) | Shareholders | array | ownership |  | PERSONAL DATA — redact |

## Interpretation Notes

- The **BRS** is the official registrar (companies, business names, LLPs). Its
  transactional system is **eCitizen**: company/business-name **search** and
  **documents** — **CR12** (the official extract of directors & shareholders),
  **status report**, certified extracts, **annual returns** — are **paid per
  transaction (KES)** behind an **eCitizen login**. `brs.ecitizen.go.ke` returned
  **403** to automated requests. **Not bypassed.**
- The field model above is from **public knowledge**; **no real company values are
  copied** (login-gated and paid).
- **Identifiers**: the **registration number** is the company id (old `C.`/`CPR`
  formats; new `PVT-XXXXXXX`); the **KRA PIN** links tax.
- **Capital / financials** (nominal capital, annual returns) come from paid documents;
  currency **KES**.
- **Personal data**: directors and shareholders on the **CR12** are personal data
  under the **Kenya Data Protection Act, 2019** — redact.
- Implementation is **blocked on payment** (eCitizen login); planning-only.
