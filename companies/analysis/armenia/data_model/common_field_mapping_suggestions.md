# Common Field Mapping Suggestions — Armenia

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Armenia profile, which is the source of truth.

Armenia's sources are gated (State Register = Radware bot-protected; SRC = per-TIN browser
search; AMX = JS SPA; data.gov.am unresolved), so these mappings are **planning-only** until
an access path is established.

| Common field | Armenia mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.tin_hvhh | src_taxpayer_search | 8-digit TIN; state reg. number alt |
| registration_number | registration.state_registration_number | state_register_eregister | bot-protected |
| tax_id | registration.tin_hvhh | src_taxpayer_search | TIN (ՀՎՀՀ) |
| vat_id | status.vat_status | src_taxpayer_search | VAT registration indicator (not a separate number) |
| legal_name | legal_identity.legal_name | src_taxpayer_search / state_register_eregister | register authoritative |
| status | status.registration_status | state_register_eregister | register status; SRC taxpayer_status separate |
| legal_form | legal_identity.legal_form | state_register_eregister | ՍՊԸ/ԲԲԸ/ՓԲԸ (planning-only) |
| incorporation_date | status.registration_date | state_register_eregister | planning-only |
| dissolution_date | not_available_in_open_sources | state_register_eregister | status only (bot-protected) |
| registered_address | registered_location.registered_address | state_register_eregister | bot-protected |
| activity_code | not_available_in_open_sources | — | not exposed in these sources |
| financials | not_available_in_open_sources | — | not openly published (AMX listed-only, SPA) |
| officers | officers | state_register_eregister | **REDACT — personal data** (bot-protected) |
| owners | owners | state_register_eregister | **REDACT — personal data** (bot-protected) |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Armenia:

- `company_id` / `tax_id` → the **TIN (ՀՎՀՀ)** (browser-public via SRC);
  `registration_number` (state reg. number), `legal_form`, `incorporation_date`,
  `registered_address`, `officers`, `owners` require the **bot-protected State Register**.
- `vat_id` is a **status** (registered/not) rather than a separate number.
- `activity_code`, `financials`, `dissolution_date` are `not_available_in_open_sources`.
- Keep **State Register registration status** and **SRC taxpayer status** distinct.
