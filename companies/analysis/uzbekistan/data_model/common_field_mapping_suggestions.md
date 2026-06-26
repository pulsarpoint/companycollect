# Common Field Mapping Suggestions — Uzbekistan

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Uzbekistan profile, which is the source of truth.

The EGRPO register and soliq tax are **firewalled from this environment** (planning-only,
re-run from an unblocked network); UZSE is browser-public (SPA).

| Common field | Uzbekistan mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.stir_inn | egrpo_register | 9-digit STIR/INN |
| registration_number | registration.stir_inn | egrpo_register | STIR/INN (or EGRPO code) |
| tax_id | registration.stir_inn | egrpo_register | the STIR is the tax id |
| vat_id | status.vat_status | soliq_taxpayer | VAT (QQS) status (not a separate number) |
| legal_name | legal_identity.legal_name | egrpo_register | UZ/RU; soliq alt |
| status | status.registration_status | egrpo_register | EGRPO status; soliq taxpayer_status separate |
| legal_form | legal_identity.legal_form | egrpo_register | MCHJ/AJ/YaTT |
| incorporation_date | status.registration_date | egrpo_register | EGRPO |
| dissolution_date | not_available_in_open_sources | egrpo_register | status only (firewalled here) |
| registered_address | registered_location.registered_address | egrpo_register | EGRPO |
| activity_code | activity.oked_activity | egrpo_register | OKED classifier |
| financials | not_available_in_open_sources | — | not openly published (UZSE listed-only, SPA) |
| officers | not_available_in_open_sources | egrpo_register | director/head field uncertain; redact if present |
| owners | not_available_in_open_sources | — | founders/shareholders not confirmed open |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Uzbekistan:

- `company_id` / `registration_number` / `tax_id` → the **STIR/INN** (EGRPO; firewalled here).
- `vat_id` is a **status** (VAT/QQS registered or not) via soliq, not a separate number.
- `financials`, `owners`, and a confident `officers` field are `not_available_in_open_sources`
  (UZSE is listed-only/SPA; the EGRPO director/head field is uncertain).
- Keep **EGRPO registration status** and **soliq tax status** distinct.
- All EGRPO/soliq mappings are **planning-only** until reachable from an unblocked network.
