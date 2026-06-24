# Common field mapping suggestions — Malaysia

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Malaysia profile, which stays keyed on the SSM registration number.

| Common field | Malaysia source path | Notes |
|---|---|---|
| company_id | `registration.registration_number_new` (SSM) | new 12-digit (since 2019) |
| registration_number | `registration.registration_number_new` (+ old) | SSM |
| tax_id | `tax_identifiers.tin` (LHDN) | companies prefixed C |
| vat_id | not_available_in_open_sources | SST, no VAT/GST (use sst_number) |
| legal_name | `legal_identity.legal_name` | SSM (paid) / Bursa (listed) |
| status | `status.status_text` | Existing/Dissolved/Wound up/Struck off |
| legal_form | `legal_identity.company_type` | Sdn. Bhd./Bhd./PLT |
| incorporation_date | `status.incorporation_date` | SSM (paid) |
| dissolution_date | not_available_in_open_sources | status implies it (paid) |
| registered_address | `registered_location.registered_address` | SSM (paid) |
| activity_code | `activity.msic_code` | MSIC |
| financials | `financial_statements[]` | SSM Financial Comparison (paid) / Bursa (listed) — MYR |
| officers | `officers[]` (Directors) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Shareholders) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == SSM number` (12-digit new); `tax_id == TIN`.
- **No VAT** — Malaysia uses **SST** (no VAT/GST since 2018); `vat_id` not applicable.
- The defining constraint is **commercial distribution**: SSM sells profiles +
  financials (`blocked_payment`); only a free e-Search (existence) is open. data.gov.my
  has no register. Currency **MYR**.
- Treat directors/shareholders as personal data (PDPA 2010) — redact.
