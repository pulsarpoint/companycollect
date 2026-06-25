# FBR — Active Taxpayers List (ATL) / NTN verification Field Catalog

## Source Summary

- Country: Pakistan
- Source type: tax_register
- Organization: Federal Board of Revenue (FBR)
- URL: https://www.fbr.gov.pk/active-taxpayer-list-income-tax/51147/131210
- License: restricted
- Access: **per-NTN online verification** (no open bulk file located)
- Freshness: weekly
- Record shape: per-NTN verification (planning-only)
- Primary keys: ntn
- Join keys: ntn, registration_no, name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ntn | NTN | National Tax Number | string | identifier |  | tax key |
| registration_no | Registration No. | Inc./reg. number | string | identifier |  | bridge to SECP |
| name | Name | Taxpayer name | string | legal_name |  | **individuals = personal data** |
| atl_status | ATL Status | Active/inactive filer | string | status |  | tax-filing status |
| category | Category | Company / AOP / individual | string | legal_form |  | separate companies from individuals |

## Interpretation Notes

- The FBR **Active Taxpayers List (ATL)** records income-tax filer status. The ATL pages are
  public, but access is **per-NTN online verification** (plus the Tax Asaan app / SMS); a
  clean **open bulk ATL file was not located** here (the category pages are informational,
  with no direct `.zip`/`.txt`). All fields here are **planning-only**.
- **Identifier**: the **NTN (National Tax Number)** is the tax key; the **Registration No.**
  can bridge a company to its SECP registration. The ATL covers **companies and individuals**
  — use `category` to separate them; **individual names are personal data** — redact.
- **`atl_status`** is **tax-filing status**, not company registration status (don't conflate
  with SECP `status`).
- No `sample_record.json`: restricted/verification-only source, nothing captured.
