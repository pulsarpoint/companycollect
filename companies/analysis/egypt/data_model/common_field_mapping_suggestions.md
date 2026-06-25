# Common field mapping suggestions — Egypt

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Egypt profile, which stays keyed on the Commercial Registry number.

| Common field | Egypt source path | Notes |
|---|---|---|
| company_id | `registration.commercial_registry_number` | gated (GAFI / Commercial Registry) |
| registration_number | `registration.commercial_registry_number` |  |
| tax_id | `tax_identifiers.tax_id` (الرقم الضريبي) | 9-digit (ETA) |
| vat_id | not_available_in_open_sources | VAT under the Tax ID (no separate number) |
| legal_name | `legal_identity.legal_name` | GAFI (gated) / EGX (listed) |
| status | `status.status_text` | Active/Under liquidation/Struck off |
| legal_form | `legal_identity.company_type` | S.A.E./LLC/one-person/branch |
| incorporation_date | not_available_in_open_sources | in gated registry |
| dissolution_date | not_available_in_open_sources | status implies it |
| registered_address | `registered_location.registered_address` | gated |
| activity_code | `activity.activity_purpose` / `activity.egx_sector` | registry / EGX sector |
| financials | `financial_statements[]` | EGX (listed, WAF-gated) — EGP; private not open |
| officers | `officers[]` (Board) | PERSONAL DATA — redact; gated |
| owners | `owners[]` (Shareholders) | PERSONAL DATA — redact; gated |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == Commercial Registry number`; `tax_id == الرقم
  الضريبي`; no separate `vat_id` (VAT under the Tax ID).
- The defining constraint is that **all registry sources are gated** (GAFI login;
  Commercial Registry not openly searchable) and **EGX is WAF-gated**; there is **no
  open company register and no open programmatic financials**. data.gov.eg
  unreachable. Currency **EGP**; Arabic + English.
- Treat board/shareholders as personal data (PDP Law 151/2020) — redact.
