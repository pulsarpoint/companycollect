# Common field mapping suggestions — Nigeria

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Nigeria profile, which stays keyed on the RC number (companies).

| Common field | Nigeria source path | Notes |
|---|---|---|
| company_id | `registration.rc_number` (CAC) | companies; BN / IT for other types |
| registration_number | `registration.rc_number` | CAC (gated/paid) |
| tax_id | `tax_identifiers.tin` (FIRS) |  |
| vat_id | `tax_identifiers.vat_id` (FIRS) | separate VAT registration |
| legal_name | `legal_identity.legal_name` | CAC (paid) / NGX (listed) |
| status | `status.status_text` | Active/Inactive/Dissolved/Delisted |
| legal_form | `legal_identity.company_type` | Plc/Ltd/Ltd-Gte/BN/IT |
| incorporation_date | `status.registration_date` | CAC (paid) |
| dissolution_date | not_available_in_open_sources | status implies it (paid) |
| registered_address | `registered_location.registered_address` | CAC (paid) |
| activity_code | `activity.nature_of_business` / `activity.ngx_sector` | CAC / NGX sector |
| financials | `financial_statements[]` | NGX (listed, open) / CAC AFS (paid) — NGN |
| officers | `officers[]` (Directors) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Shareholders / PSC) | PERSONAL DATA — redact; paid/token-gated |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == RC number` (companies); sole props/partnerships
  use **BN**, NGOs use **IT**; `tax_id == TIN`; `vat_id` is a separate FIRS VAT
  registration.
- The defining constraint is **gated/paid CAC** access (Cloudflare search + paid
  documents); the only **open** source is **NGX** (listed). data.gov.ng unreachable.
  Currency **NGN**.
- Treat directors/shareholders/beneficial owners as personal data (NDPA 2023) —
  redact; never store inadvertently-exposed PII.
