# GAFI — General Authority for Investment Field Catalog

## Source Summary

- Country: Egypt
- Source type: official_registry
- Organization: General Authority for Investment and Free Zones (GAFI)
- URL: https://www.gafi.gov.eg/
- License: restricted (login)
- Access: **login-gated** investor eServices
- Freshness: live register
- Record shape: per-company record (login-gated)
- Primary keys: commercial_registry_number
- Join keys: commercial_registry_number, tax_id, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_name | Company Name | Company name | string | legal_name |  | S.A.E./LLC/branch |
| commercial_registry_number | رقم السجل التجاري | Commercial registration number | string | identifier |  | primary key |
| tax_id | الرقم الضريبي | Tax ID (9-digit) | string | identifier |  | tax join |
| company_type | Company Type | Type | string | legal_form |  | S.A.E./LLC/one-person/branch |
| status | Status | Status | string | status |  | Active/Under liquidation/Struck off |
| capital | Authorised / Paid-up Capital | Capital | decimal | financial |  | EGP |
| activity | Activity / Purpose | Activity | string | activity |  | |
| registered_address | Registered Address | Registered office | string | address |  | |
| directors | Board / Managers | Board | array | person |  | PERSONAL DATA — redact |
| shareholders | Shareholders / Partners | Shareholders | array | ownership |  | PERSONAL DATA — redact |

## Interpretation Notes

- **GAFI** establishes companies (joint-stock **S.A.E.** / LLC under the Investment &
  Companies Law) and runs **investor eServices** (registration/incorporation,
  amendments). These are **login-gated**; there is **no public company search/register
  and no open API** on GAFI.
- The field model above is documented from **public knowledge** of GAFI/Commercial
  Registry company records; **no real values are copied** (login-gated).
- **Identifiers**: the **Commercial Registry number** is the company id; the **Tax
  ID** (9-digit) links tax.
- **Capital** is in **EGP**. **Personal data**: board members / shareholders are
  personal data under **PDP Law 151/2020** — redact.
- Implementation is **blocked on authentication** (login); planning-only.
