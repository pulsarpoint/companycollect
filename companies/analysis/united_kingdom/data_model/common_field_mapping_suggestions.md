# United Kingdom — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific UK profile, which is authoritative.

| Common field | UK source | UK path | Notes |
|---|---|---|---|
| company_id | ch_basic_company_data | registration.company_number | 8-char |
| registration_number | ch_basic_company_data | registration.company_number | same as company_id |
| tax_id | not_available_in_open_sources | — | CH has none |
| vat_id | not_available_in_open_sources | — | HMRC, separate |
| legal_name | ch_basic_company_data | legal_identity.legal_name | |
| status | ch_basic_company_data | status.status | Active/Dissolved/… |
| legal_form | ch_basic_company_data | legal_identity.legal_form | CompanyCategory |
| incorporation_date | ch_basic_company_data | incorporation.incorporation_date | DD/MM/YYYY |
| dissolution_date | ch_basic_company_data | incorporation.dissolution_date | |
| registered_address | ch_basic_company_data | registered_location | |
| activity_code | ch_basic_company_data | activity.sic_codes | UK SIC 2007 |
| financials | ch_accounts_bulk | financial_statements[] | iXBRL/FRC; GBP; ~60-75% coverage |
| officers | ch_rest_api | officers[] | free key; PII |
| owners | ch_psc_snapshot | owners[] | OPEN; PII |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Single key (company number)** across register, accounts, PSC, API. **No VAT/tax
  id** in the company register (unlike RO/PT/SK) — leave `vat_id`/`tax_id` empty
  for the company-register pipeline.
- **Financials are open and structured** (iXBRL/FRC taxonomy, GBP) — a strong
  point — but require **multi-context iXBRL parsing** and cover only e-filed
  accounts (~60–75%). Map `financials[]` by FRC tag (`core:TurnoverRevenue`, …).
- **Beneficial ownership is open** (PSC) — populate `owners` with control bands
  (redact individuals). **Officers** are open too but only via the free-key API.
