# Iceland — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Iceland-specific profile. Identity is free per-company (no open
> bulk); financials are paid.

| Common field | Iceland mapping | Status |
|---|---|---|
| company_id | registration.kennitala (10-digit) | free per-company (no open bulk) |
| registration_number | registration.kennitala | free per-company |
| tax_id | registration.kennitala | free per-company (= kennitala) |
| vat_id | tax_identifiers.vat_id (VSK number) | separate registration; only status open |
| legal_name | legal_identity.legal_name (nafn) | free per-company |
| status | status.status (registered / afskráð) | free per-company |
| legal_form | legal_identity.legal_form (rekstrarform) | free per-company |
| incorporation_date | not_available_in_open_sources | not in the free overview (paid certificate) |
| dissolution_date | not_available_in_open_sources | status flag (afskráð) instead |
| registered_address | registered_location.registered_address (lögheimili) | free per-company |
| activity_code | activity.isat_code (ÍSAT, NACE-based) | free per-company |
| financials | financial_statements[] (Ársreikningaskrá) | paid per-document — planning-only |
| officers | officers[] (forráðamaður/chair) | free chair; full board paid; personal data (GDPR) |
| owners | not_available_in_open_sources | beneficial owners (raunverulegir eigendur) not openly published |
| source_provenance | source_provenance[] | available |

## Notes

- **Single anchor**: the **kennitala** is `company_id`, `registration_number`, and
  `tax_id` at once. The **VAT (VSK)** number is a separate registration; only the
  VSK status is open.
- **Access**: identity is free **per-company** (no open bulk / enumeration — bulk
  extracts and certified certificates are paid). Map cautiously: there is no open
  dataset to iterate; lookups need seed kennitalas.
- **Financials** are filed for public disclosure but **paid** per-document
  (Ársreikningaskrá), ISK.
- **Personal data**: the board chair (forráðamaður) is personal data (GDPR) —
  redact in any committed sample.
