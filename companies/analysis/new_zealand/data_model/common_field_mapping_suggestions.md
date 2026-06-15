# New Zealand — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the NZ-specific profile. Identity is via the NZBN API (free key);
> financials are FMC-reporting-only; officers are personal-data gated.

| Common field | New Zealand mapping | Status |
|---|---|---|
| company_id | registration.nzbn (13-digit NZBN) | open via NZBN API (free key) |
| registration_number | registration.company_number (Companies Register) | via NZBN sourceRegisterUniqueIdentifier |
| tax_id | not_available_in_open_sources | IRD number not public |
| vat_id | not_available_in_open_sources | NZ uses GST (not public); no VAT |
| legal_name | legal_identity.legal_name (entityName) | open (free key) |
| status | status.status (entityStatusDescription) | open (free key) |
| legal_form | legal_identity.entity_type (entityTypeDescription) | open (free key) |
| incorporation_date | incorporation.registration_date | open (free key) |
| dissolution_date | not_available_in_open_sources | status flag (Removed) instead |
| registered_address | registered_location.registered_address | open (free key) |
| activity_code | activity.industry_classifications (ANZSIC 2006) | open (free key) |
| financials | financial_statements[] (Disclose/Companies registers) | FMC reporting entities only — planning-only |
| officers | officers[] (Companies Register directors) | gated; personal data (Privacy Act) |
| owners | not_available_in_open_sources | shareholders on Companies Register UI; personal data |
| source_provenance | source_provenance[] | available |

## Notes

- **NZBN is the single anchor** for `company_id`; the **company number** is the
  `registration_number`. Do **not** expect a VAT id (NZ has GST), and neither the
  IRD nor GST number is public.
- **Access tier:** identity needs a free NZBN subscription key (no free bulk).
  Financials are limited to the FMC-reporting subset as documents; officers are on
  the Companies Register and are personal data.
- **Personal data:** directors/shareholders and any contact details are personal
  data under the **Privacy Act 2020** — redact in any committed sample.
