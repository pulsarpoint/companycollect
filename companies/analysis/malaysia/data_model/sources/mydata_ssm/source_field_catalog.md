# MyData-SSM — Buy SSM Report Field Catalog

## Source Summary

- Country: Malaysia
- Source type: official_registry
- Organization: Suruhanjaya Syarikat Malaysia (SSM)
- URL: https://www.mydata-ssm.com.my/
- License: commercial paid products
- Access: **paid** (per report, MYR)
- Freshness: live register
- Record shape: paid Company Profile + Company Financial Report
- Primary keys: registration_number_new
- Join keys: registration_number_new

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_profile | Company Profile | SSM company profile | object | metadata |  | same fields as e-Info; redact PII |
| company_financial_report | Company Financial Report | Annual financials | object | financial |  | MYR |
| registration_number_new | Registration No. | 12-digit reg number | string | identifier |  | join key |

## Interpretation Notes

- **MyData-SSM** is a second official SSM channel ("Buy SSM Report") selling the
  **Company Profile** and **Company Financial Report** — the **same underlying SSM
  register** as e-Info, **paid per report**.
- It does not add new fields beyond e-Info; it is an alternative purchase channel.
  Use the **e-Info field catalog** for the detailed Company Profile field model.
- **Join** on the **12-digit registration number**. **Paid**; planning-only.
  Directors/shareholders are personal data (PDPA 2010) — redact.
