# Common field mapping suggestions — Ghana

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Ghana profile, which stays keyed on the ORC registration number.

| Common field | Ghana source path | Notes |
|---|---|---|
| company_id | `registration.registration_number` (ORC) | eServices/paid |
| registration_number | `registration.registration_number` |  |
| tax_id | `tax_identifiers.tin` (GRA) |  |
| vat_id | not_available_in_open_sources | VAT registration tied to the TIN |
| legal_name | `legal_identity.legal_name` | ORC (paid) / GSE (listed) |
| status | `status.status_text` | Active/Dissolved/Struck off/In receivership |
| legal_form | `legal_identity.company_type` | Ltd by shares (Ltd/PLC) / by guarantee / unlimited / external |
| incorporation_date | `status.incorporation_date` | ORC (paid) |
| dissolution_date | not_available_in_open_sources | status implies it (paid) |
| registered_address | `registered_location.registered_address` | ORC (paid) |
| activity_code | `activity.nature_of_business` / `activity.gse_sector` | ORC / GSE sector |
| financials | `financial_statements[]` | GSE (listed, open) / ORC annual returns (paid) — GHS |
| officers | `officers[]` (Directors) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Shareholders) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == ORC registration number`; `tax_id == TIN`;
  `vat_id` is not a separate number (VAT tied to the TIN).
- The defining constraint is **eServices-gated, paid ORC** access (and the hosts were
  firewalled from this environment); the only **open** source is the **GSE** (listed).
  data.gov.gh firewalled. Currency **GHS**; English.
- Treat directors/shareholders as personal data (Act 843) — redact (may include Ghana
  Card PIN).
