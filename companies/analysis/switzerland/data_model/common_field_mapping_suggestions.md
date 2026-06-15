# Switzerland — Common Field Mapping Suggestions

> Suggestions only for a future cross-country mapper. This does **not** constrain
> the country-specific Switzerland profile, which is authoritative.

| Common field | Switzerland source | Switzerland path | Notes |
|---|---|---|---|
| company_id | zefix_lindas | registration.uid | UID (CHE-…) |
| registration_number | zefix_lindas | registration.uid | UID (or CHID) |
| tax_id | zefix_lindas | registration.uid | UID is the tax id |
| vat_id | zefix_lindas | tax_identifiers.vat_id | UID + ' MWST'/'TVA'/'IVA' |
| legal_name | zefix_lindas | legal_identity.legal_name | multilingual |
| status | zefix_rest_api | status.status | free credentialed (LINDAS ≈ active) |
| legal_form | zefix_lindas | legal_identity.legal_form | eCH-0097 |
| incorporation_date | sogc_shab | register_events[] | free credentialed |
| dissolution_date | sogc_shab | register_events[] | free credentialed |
| registered_address | zefix_lindas | registered_location | structured |
| activity_code | not_available_in_open_sources | — | no per-company NOGA in the open set |
| financials | six_listed_financials | financial_statements[] | not_available for private companies (listed only) |
| officers | sogc_shab / handelsregister_extract | officers[] | credentialed/paid; PII |
| owners | not_available_in_open_sources | — | no public beneficial-ownership register |
| source_provenance | (all) | source_provenance[] | per-section provenance |

## Cross-country notes

- **Single key (UID).** `vat_id = UID + ' MWST'/'TVA'/'IVA'`; `tax_id = UID`. A
  mapper can derive these from `company_id` alone.
- **Financials are a structural gap** — unlike most EU countries, Switzerland does
  **not** publish private-company accounts (no filing obligation). Map `financials`
  only for listed issuers (SIX); mark everything else `not_available`.
- **Open identity is rich** (name, legal form, address, purpose, website) but
  **status/dates/officers/capital** require the free-credentialed REST/SOGC or a
  paid extract; **activity code** and **beneficial owners** are absent.
