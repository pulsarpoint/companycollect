# Companies Registry — ICRIS e-Search (full register) Field Catalog

## Source Summary

- Country: Hong Kong
- Source type: official_registry
- Organization: Companies Registry (CR), HKSAR Government
- URL: https://www.e-services.cr.gov.hk/ICRIS3EP/
- License: restricted
- Access: **interactive, pay-per-use** (no open bulk/API)
- Freshness: live
- Record shape: per-company lookup (planning-only)
- Primary keys: cr_company_number
- Join keys: cr_company_number, br_number, company_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| cr_company_number | Company Number (CR No.) | Registry key | string | identifier |  | ≠ BR Number |
| company_name | Company Name (EN/ZH) | Registered name(s) | string | legal_name |  | |
| company_type | Company Type | Type/legal form | string | legal_form |  | private ltd by shares etc. |
| company_status | Company Status | Status | string | status |  | Live/Dissolved/Struck off |
| date_of_incorporation | Date of Incorporation/Registration | Inc./reg. date | date | date |  | |
| registered_office_address | Registered Office Address | HK address | string | address |  | |
| directors | Directors | Directors | array | person |  | **PERSONAL DATA — redact** |
| company_secretary | Company Secretary | Secretary | string | person |  | **PERSONAL DATA — redact** |
| charges | Charges / Documents | Charges, filed docs | array | document |  | document images pay-per-use |

## Interpretation Notes

- **ICRIS e-Search (ICRIS3EP)** is the **authoritative full register**, holding the **CR
  Company Number** plus full particulars (type, status, registered office, directors,
  secretary, charges, filed documents). It is an **interactive** portal and document /
  full-particulars search is **pay-per-use** — there is **no open bulk file or free API**.
- All fields here are **planning-only**, documented from public CR knowledge; **no values
  were captured** (the portal was not driven, paid, or scraped).
- **Two HK identifiers**: the **CR Company Number** (this source) vs the **BR Number** (the
  open RNC063 feed). Join on company name / BR Number where available.
- **Personal data**: directors and company secretary are natural persons under the
  **Personal Data (Privacy) Ordinance (PDPO, Cap. 486)** — redact; the CR also restricts
  some director particulars.
- No `sample_record.json`: restricted/paid source, nothing captured.
