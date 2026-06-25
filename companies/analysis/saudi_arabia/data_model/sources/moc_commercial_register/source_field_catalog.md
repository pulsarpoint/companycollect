# Ministry of Commerce — Commercial Register Field Catalog

## Source Summary

- Country: Saudi Arabia
- Source type: official_registry
- Organization: Ministry of Commerce (MoC) / Saudi Business Center
- URL: https://mc.gov.sa/en/eservices/Pages/Commercial-data.aspx
- License: restricted (login)
- Access: **Nafath login-gated** (inquiry hosts firewalled here)
- Freshness: live register
- Record shape: per-company CR record (Nafath login-gated)
- Primary keys: cr_number
- Join keys: cr_number, unified_number_700, vat_number, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cr_number | Commercial Registration Number | 10-digit CR number | string | identifier |  | region prefix; primary key |
| unified_number_700 | Unified National Number (700) | Unified company id | string | identifier |  | cross-agency key |
| vat_number | VAT Number | 15-digit ZATCA | string | identifier |  | from ZATCA |
| company_name | Company Name (AR/EN) | Company name | string | legal_name |  | |
| company_type | Entity Type | Type | string | legal_form | JSC, LLC | |
| status | Status | Status | string | status |  | Active/Expired/Cancelled |
| issue_date | Issue Date | Issue date | date | date |  | Hijri |
| expiry_date | Expiry Date | Expiry date | date | date |  | CR renewed |
| capital | Capital | Capital | decimal | financial |  | SAR |
| activities | Activities | Activities | array | activity |  | ISIC |
| head_office | Head Office / Address | Head office | string | address |  | |
| managers | Managers / Owners / Partners | Managers/owners | array | person |  | PERSONAL DATA — redact |

## Interpretation Notes

- The **MoC Commercial Register (السجل التجاري)** is the official company register.
  The **CR inquiry/verification** e-service ("Commercial-data") requires **Nafath
  login** (national digital identity) to view CR data, and the inquiry sub-hosts
  (`eservices.mc.gov.sa`, `businesscenter.gov.sa`, `qaweem.mc.gov.sa`) were
  **NXDOMAIN/firewalled** from this environment. **Not bypassed** — field model from
  **public knowledge**, **no live values copied**.
- **Identifiers**: the **CR number** (10-digit, region-prefixed) is the company id;
  the **Unified National Number (700…)** is the cross-agency key; the **VAT number**
  (15-digit) links tax (held by ZATCA).
- **Dates** are primarily **Hijri** (with Gregorian). **Capital** is in **SAR**.
- **Personal data**: managers, owners, and partners are personal data under the
  **PDPL (Royal Decree M/19 of 1443H)** — redact.
- Implementation is **blocked on authentication** (Nafath login); planning-only.
