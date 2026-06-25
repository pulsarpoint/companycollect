# Common Field Mapping Suggestions — Kazakhstan

This file is a **suggestion** for a future cross-country mapper. It does **not**
constrain the country-specific Kazakhstan profile, which is the source of truth.

The open register `gbd_ul` is directly implementable **once a free API key is obtained**; KGD
and KASE are browser-public.

| Common field | Kazakhstan mapping | Source | Notes |
|---|---|---|---|
| company_id | registration.bin | egov_gbd_ul | 12-digit BIN |
| registration_number | registration.bin | egov_gbd_ul | BIN |
| tax_id | registration.bin | egov_gbd_ul | the BIN is the tax id |
| vat_id | status.vat_registration | kgd_taxpayer | VAT (НДС) status (not a separate number) |
| legal_name | legal_identity.legal_name | egov_gbd_ul | RU/KZ; KGD alt |
| status | status.taxpayer_status | kgd_taxpayer | KGD tax status; gbd_ul = registration data |
| legal_form | not_available_in_open_sources | egov_gbd_ul | not in the described gbd_ul fields (inferable from name, e.g. ТОО = LLP) |
| incorporation_date | status.registration_date | egov_gbd_ul | gbd_ul |
| dissolution_date | not_available_in_open_sources | kgd_taxpayer | deregistration via KGD lists (per-list) |
| registered_address | registered_location.legal_address | egov_gbd_ul | at registration |
| activity_code | activity.oked_activity | egov_gbd_ul | OKED classifier |
| financials | not_available_in_open_sources | — | not openly published (KASE listed-only, SPA) |
| officers | officers | egov_gbd_ul | director name — **REDACT — personal data** |
| owners | not_available_in_open_sources | — | shareholders/founders not in gbd_ul described fields |
| source_provenance | source_provenance | all | per-section |

Concepts notes for Kazakhstan:

- `company_id` / `registration_number` / `tax_id` → the **BIN** (gbd_ul; free-API-key-gated).
- `vat_id` is a **status** (VAT-registered or not) via KGD, not a separate number.
- `legal_form`, `owners`, `financials`, `dissolution_date` are `not_available_in_open_sources`
  from the described gbd_ul fields (legal form is inferable from the name; deregistration via
  KGD lists).
- Keep **gbd_ul registration data** and **KGD tax status** distinct.
