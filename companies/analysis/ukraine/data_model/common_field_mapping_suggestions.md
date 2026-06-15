# Ukraine — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Ukraine profile, which is authoritative.

| Common field | Ukraine source | Ukraine path | Notes |
|---|---|---|---|
| company_id | edr_uo | registration.edrpou | 8-digit EDRPOU |
| registration_number | edr_uo | registration.edrpou | same as company_id |
| tax_id | edr_uo | registration.edrpou | EDRPOU = legal-entity tax code |
| vat_id | edr_uo | tax_registrations[] | not_available (no separate VAT number; tax-register flags only) |
| legal_name | edr_uo | legal_identity.legal_name | |
| status | edr_uo | status.status | registered/in-termination/terminated |
| legal_form | edr_uo | legal_identity.legal_form | OPF (ТОВ=LLC) |
| incorporation_date | edr_uo | incorporation.registration_date | first date in REGISTRATION |
| dissolution_date | edr_uo | incorporation.termination_date | first date in TERMINATED_INFO |
| registered_address | edr_full_restricted | registered_location | not_available_in_open_sources (wartime) |
| activity_code | edr_full_restricted | activity.kved_codes | not_available_in_open_sources (wartime) |
| financials | xbrl_frs / nssmc_smida | financial_statements[] | IFRS reporters/issuers only; UAH |
| officers | edr_uo | officers[] | OPEN but PII — redact |
| owners | edr_uo | owners.founders/beneficial_owners | OPEN UBO but PII — redact |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Single key (EDRPOU)** for both register and financials. **No VAT number** in
  Ukraine — a mapper must leave `vat_id` empty (unlike RO/PT/SK).
- **Open beneficial ownership** is a standout — `owners.beneficial_owners` is
  populated from open data (with PII redaction), unlike most countries where UBO is
  restricted/paid.
- **Two structural gaps from the war**: `registered_address` and `activity_code`
  are absent from the open export — mark `not_available_in_open_sources`.
- **Financials** are open but **partial coverage** (IFRS reporters/issuers) — do
  not assume every company has them.
