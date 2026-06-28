# Common Field Mapping Suggestions — Bangladesh

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Bangladesh profile, which is the source of truth.

Only the DSE listed source is directly implementable (`ready`); RJSC is paid (planning-only)
and NBR is per-ID verification.

| Common field | Bangladesh mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.rjsc_registration_number | rjsc_register | RJSC no. (paid); DSE trading code for listed |
| registration_number | registration.rjsc_registration_number | rjsc_register | RJSC no. (paid) |
| tax_id | registration.e_tin | nbr_tax | income-tax e-TIN (per-ID) |
| vat_id | registration.bin | nbr_tax | VAT BIN (per-ID) |
| legal_name | legal_identity.legal_name | dse_listed / rjsc_register | DSE open; RJSC authoritative |
| status | status.registration_status | rjsc_register | RJSC status (paid); NBR vat_status separate |
| legal_form | legal_identity.entity_type | rjsc_register | company/firm/society (paid) |
| incorporation_date | status.incorporation_date | rjsc_register | RJSC registration date (paid); DSE listing_year is NOT this |
| dissolution_date | not_available_in_open_sources | rjsc_register | struck-off status via RJSC (paid) |
| registered_address | registered_location.registered_address | rjsc_register | RJSC (paid) |
| activity_code | activity.sector | dse_listed | DSE sector (listed only) |
| financials | financial_statements | dse_listed | DSE authorized/paid-up capital (BDT mn); listed only |
| officers | officers | rjsc_register | **REDACT — personal data** (paid documents) |
| owners | not_available_in_open_sources | rjsc_register | shareholders via RJSC paid documents; personal data |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Bangladesh:

- Open data covers **listed companies only** (DSE). `company_id` / `registration_number`
  (RJSC no.), `legal_form`, `status`, `incorporation_date`, `registered_address`, `officers`,
  `owners` require the **paid RJSC** register.
- `tax_id`/`vat_id` (e-TIN/BIN) are via **per-ID NBR verification** (no bulk).
- Keep **DSE listing_year** distinct from **RJSC incorporation_date**.
- No single national identifier — join DSE↔RJSC↔NBR by **name**.
