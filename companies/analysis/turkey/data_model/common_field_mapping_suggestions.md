# Turkey — Common Field Mapping Suggestions

> Suggestion layer only for a future cross-country mapper. It does **not**
> constrain the Turkey-specific profile. Identity is free per-company (no open
> bulk); financials are listed-only (KAP).

| Common field | Turkey mapping | Status |
|---|---|---|
| company_id | registration.mersis_no (16-digit MERSIS no) | free per-company (no open bulk) |
| registration_number | registration.trade_registry_no (+ MERSIS no) | free per-company |
| tax_id | tax_identifiers.tax_id (VKN, 10-digit) | free per-company |
| vat_id | not_available_in_open_sources | Turkey has VAT (KDV); VKN serves as the VAT id |
| legal_name | legal_identity.legal_name (unvan) | free per-company (KAP for listed) |
| status | status.status (durum) | free per-company |
| legal_form | legal_identity.company_type (A.Ş./Ltd. Şti.) | free per-company |
| incorporation_date | not_available_in_open_sources | gazette incorporation announcement |
| dissolution_date | not_available_in_open_sources | gazette tasfiye announcement |
| registered_address | registered_location.registered_address | free per-company |
| activity_code | activity.nace_code (NACE) | free per-company |
| financials | financial_statements[] (KAP) | listed-only; TRY — public |
| officers | officers[] (gazette directors/shareholders) | gazette; personal data (KVKK) |
| owners | officers[] (gazette shareholders) | gazette; personal data |
| source_provenance | source_provenance[] | available |

## Notes

- **Two anchors**: `company_id` -> **MERSIS no**; `tax_id` -> **VKN**. Turkey has
  **VAT (KDV)** but **no separate VAT number** — mark `vat_id` not available.
- **Access**: identity is free **per-company** (MERSIS) with **no open bulk** — no
  enumeration; lookups need a seed (MERSIS no / VKN / title). Financials are
  **listed-only** via **KAP** (TRY) — a real, open source (~800 companies).
- **Personal data**: directors/shareholders (gazette) are personal data under
  **KVKK** — redact.
