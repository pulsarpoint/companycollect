# SSM e-Info — Company Profile + Financial Field Catalog

## Source Summary

- Country: Malaysia
- Source type: official_registry
- Organization: Suruhanjaya Syarikat Malaysia (SSM / Companies Commission)
- URL: https://www.ssm-einfo.my/
- License: commercial paid products
- Access: **paid** (per document, MYR), SAML login (idpro.ssm.com.my)
- Freshness: live register
- Record shape: paid PDF Company Profile + Financial Comparison/Historical
- Primary keys: registration_number_new (12-digit)
- Join keys: registration_number_new, registration_number_old, tin

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Company name | string | legal_name |  | Sdn. Bhd./Bhd./PLT |
| registration_number_new | Registration No. (new) | 12-digit reg number | string | identifier |  | primary key |
| registration_number_old | Registration No. (old) | Legacy NNNNNNN-A | string | identifier |  | cross-ref |
| incorporation_date | Incorporation Date | Incorporation date | date | date |  | |
| company_type | Company Type | Type | string | legal_form |  | private/public |
| status | Status | Status | string | status |  | Existing/Dissolved/... |
| registered_address | Registered Office Address | Registered office | string | address |  | |
| business_address | Business Address | Place of business | string | address |  | |
| nature_of_business | Nature of Business (MSIC) | Activity | string | activity |  | MSIC |
| paid_up_capital | Paid-up Capital | Paid-up capital | decimal | financial |  | MYR |
| directors | Directors | Directors | array | person |  | PERSONAL DATA — redact |
| shareholders | Shareholders | Shareholders | array | ownership |  | PERSONAL DATA — redact |
| financial_comparison | Financial Comparison/Historical | Annual financials | array | financial |  | separate paid product; MYR |

## Interpretation Notes

- **SSM e-Info** is the official SSM commercial portal. Company and financial data
  are **paid products** (per document, MYR) behind a **SAML login**
  (`idpro.ssm.com.my`): **Company Profile (ROC)**, **Business Profile**, **LLP
  Profile**, **Audit Firm Profile**, **Financial Comparison** (2/3/5/10 years),
  **Financial Historical**. A free **e-Search** gives only basic existence.
- The field model above is taken from SSM's **published sample templates** (e.g.
  `Company_Profile.pdf`); **no real company values are copied**.
- **Identifiers**: the **12-digit registration number** (new, since 2019) is the
  primary key; the **old NNNNNNN-A** number cross-references legacy records; **TIN**
  (LHDN) links to tax.
- **Financials** (revenue, profit, assets, equity over 2–10 years) are a **separate
  paid product**; currency **MYR**.
- **Personal data**: directors and shareholders (incl. NRIC) are personal data
  under the **PDPA 2010** — redact.
- Implementation is **blocked on payment**; planning-only.
