# Common field mapping suggestions — Kenya

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Kenya profile, which stays keyed on the BRS registration number.

| Common field | Kenya source path | Notes |
|---|---|---|
| company_id | `registration.registration_number` (BRS) | old C./CPR, new PVT-XXXXXXX; BN for business names |
| registration_number | `registration.registration_number` | BRS (eCitizen/paid) |
| tax_id | `tax_identifiers.kra_pin` (KRA) |  |
| vat_id | not_available_in_open_sources | VAT under the KRA PIN (no separate number) |
| legal_name | `legal_identity.legal_name` | BRS (paid) / NSE (listed) |
| status | `status.status_text` | Active/Dormant/Dissolved/Struck off |
| legal_form | `legal_identity.company_type` | Ltd/PLC/CLG/Business Name/LLP |
| incorporation_date | `status.registration_date` | BRS (paid) |
| dissolution_date | not_available_in_open_sources | status implies it (paid) |
| registered_address | `registered_location.registered_address` | BRS (paid) |
| activity_code | `activity.nse_sector` | NSE sector (listed); BRS not consistently coded |
| financials | `financial_statements[]` | NSE (listed, open) / BRS annual returns (paid) — KES |
| officers | `officers[]` (Directors, CR12) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Shareholders, CR12) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- `company_id == registration_number == BRS registration number`; `tax_id == KRA PIN`;
  `vat_id` is not a separate number (VAT under the PIN).
- The defining constraint is **eCitizen-gated, paid BRS** access; the only **open**
  source is **NSE** (listed). opendata.go.ke has no company dataset. Currency **KES**.
- Treat CR12 directors/shareholders as personal data (Data Protection Act 2019) —
  redact.
