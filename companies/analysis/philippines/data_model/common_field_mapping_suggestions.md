# Common field mapping suggestions — Philippines

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Philippines profile, which stays keyed on the SEC Registration
> Number (corporations) / DTI BN (sole proprietors).

| Common field | Philippines source path | Notes |
|---|---|---|
| company_id | `registration.sec_registration_number` (SEC) | corporations; DTI BN for sole props |
| registration_number | `registration.sec_registration_number` | SEC (paid) |
| tax_id | `tax_identifiers.tin` (BIR) | 9-digit + branch |
| vat_id | not_available_in_open_sources | TIN-based; no separate VAT number |
| legal_name | `legal_identity.legal_name` | SEC (paid) / PSE (listed) |
| status | `status.status_text` | Active/Revoked/Suspended/Dissolved |
| legal_form | `legal_identity.company_type` | Stock/Non-stock/OPC/Partnership |
| incorporation_date | `status.incorporation_date` | SEC (paid) |
| dissolution_date | not_available_in_open_sources | status implies it (paid) |
| registered_address | `registered_location.registered_address` | SEC (paid) |
| activity_code | `activity.primary_purpose` / `activity.pse_sector` | PSIC (SEC) / PSE sector (listed) |
| financials | `financial_statements[]` | AFS via SEC Express (paid); PSE EDGE (listed) — PHP |
| officers | `officers[]` (Directors/Officers) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Stockholders) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == SEC Registration Number` (corporations);
  sole proprietors use the **DTI BN** number; `tax_id == TIN`.
- **No VAT number** — VAT-registered businesses use the **TIN**.
- The defining constraint is **paid SEC documents**: GIS/AFS bought via SEC Express
  (`blocked_payment`); the only **open** source is **PSE EDGE** (listed). data.gov.ph
  has no accessible company dataset. Currency **PHP**.
- Treat GIS directors/officers/stockholders as personal data (Data Privacy Act 2012)
  — redact.
