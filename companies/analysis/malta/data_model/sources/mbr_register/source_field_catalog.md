# MBR — Malta Business Registry (company search) Field Catalog

> Field model documented from the MBR. **No `sample_record.json`**: the registry portals are WAF-blocked (HTTP
> 403) with no open bulk/API, so no per-company open record was lawfully downloadable in bulk; no real values
> copied. Officer/shareholder/financial fields are delivered via paid documents / the paid API.

## Source Summary

- Country: Malta
- Source type: official_registry
- Organization: Malta Business Registry (MBR)
- URL: https://mbr.mt/ (registry.mbr.mt / baros.mbr.mt WAF-blocked for automation)
- License: public register (free basic search; documents paid; reuse terms unclear)
- Access: public (manual); registry portals WAF-blocked, no open bulk/API
- Freshness: real-time
- Record shape: company page (HTML; documents PDF)
- Primary keys: `registration_number`
- Join keys: `registration_number`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| registration_number | Registration Number | Company id | string | identifier | (not copied) | prefix = entity class (C) |
| name | Company Name | Legal name | string | legal_name | (not copied) | |
| company_type | Company Type | Legal form | string | legal_form | (not copied) | Ltd/plc/partnership |
| status | Status | Status | string | status | (not copied) | Active/Struck off/Liquidated |
| registration_date | Registration Date | Incorporation date | date | date | (not copied) | |
| registered_address | Registered Office | Registered address | string | address | (not copied) | |
| officers[] | Officers | Directors/secretary | array | person | (paid) | **PII** |
| shareholders[] | Shareholders | Name/share type/control | array | ownership | (paid) | **PII**; registered owners |
| financial_info | Financial information | Annual accounts/return | object | financial | (paid) | links to paid financials |

## Interpretation Notes

- **Authoritative register, rich but mostly paid.** Free basic search gives identity + status; the deeper data —
  **officers**, **shareholders** (name, share type, degree of control), and **financial information** — is
  delivered via **paid documents** (EUR 5–25) or the **paid MBR API**. The online registry portals
  (registry.mbr.mt, baros.mbr.mt) return **HTTP 403** to non-browser clients (WAF) → automated/bulk access is
  **blocked and must not be bypassed**; there is **no open bulk export**.
- **Registered shareholders are in the register** — a Malta distinctive (distinct from beneficial owners, which
  are in the restricted UBO register).
- **Identifiers.** Registration number (prefix = entity class: C = companies). **VAT** = `MT` + 8 digits,
  separate. Income-tax TIN separate.
- **GDPR.** Officers/shareholders are personal data. **License.** Reuse/redistribution terms not clearly stated.
