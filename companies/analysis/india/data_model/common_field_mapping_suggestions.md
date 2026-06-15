# India — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the India-specific profile. Identity + capital are open; financials
> and officers are paid/listed/personal-data gated.

| Common field | India mapping | Status |
|---|---|---|
| company_id | registration.cin (CIN, 21-char) | open (data.gov.in) |
| registration_number | registration.cin (RoC sequence = last 6 chars) | open |
| tax_id | not_available_in_open_sources | PAN (10-char) not in open data |
| vat_id | not_available_in_open_sources | India uses GST (GSTIN), not VAT; not in open data |
| legal_name | legal_identity.legal_name | open |
| status | status.status (ACTIVE->active, STRIKE OFF->struck_off, …) | open |
| legal_form | legal_identity.company_class + company_category + CIN type segment | open |
| incorporation_date | incorporation.date_of_registration | open (normalize 2 formats) |
| dissolution_date | not_available_in_open_sources | only a status flag (STRIKE OFF/DISSOLVED) |
| registered_address | registered_location.registered_office_address | open (free text) |
| activity_code | activity.industrial_class (2021) / CIN industry code (chars 2-6) | open; description in principal_business_activity |
| financials | financial_statements[] (MCA AOC-4/XBRL paid; BSE/NSE listed) | paid / listed-only — planning-only |
| officers | officers[] (MCA portal directors/DIN) | gated; DPDP personal data |
| owners | not_available_in_open_sources | SBO filed with MCA, not openly published |
| source_provenance | source_provenance[] | available |

## Notes

- **CIN is the single anchor** for `company_id` / `registration_number`. Do **not**
  expect a VAT id (India has GST), and neither PAN nor GSTIN is in the open data.
- **Capital ≠ financials:** the open data carries authorized/paid-up capital and
  latest-filing-year markers, but no P&L/balance-sheet figures. Map `financials`
  only from paid MCA documents or listed-company (BSE/NSE) disclosures.
- **Personal data:** company contact email and director/DIN data are personal data
  under the **DPDP Act 2023** — redact in any committed sample.
- **Freshness caveat:** the open data is point-in-time (latest 2021); the live
  register is the WAF-gated MCA portal.
