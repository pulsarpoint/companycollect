# Cyprus — Common Field Mapping Suggestions

> **Suggestion layer only.** This does NOT constrain the country-specific Cyprus profile
> (`country_company_profile.schema.json`). It is a hint for a future cross-country mapper. Cyprus keeps three
> distinct identifiers (HE / TIC / VAT) and three distinct person/ownership layers (officers / shareholders /
> beneficial owners) that should not be flattened away.

| Common field | Cyprus source | Cyprus path | Notes |
|---|---|---|---|
| company_id | drcip_register | registration_number | HE… ; prefix encodes entity type |
| registration_number | drcip_register | registration_number | same as company_id |
| tax_id | tax_department | TIC | separate from HE and VAT; per-company lookup |
| vat_id | tax_department | VAT number | CY + 8 digits + letter; validate via VIES |
| legal_name | drcip_register | name | Greek and/or English |
| status | drcip_register | status | operational/struck-off/dissolved/… |
| legal_form | drcip_register | type (+ name) | company/business name/partnership/overseas; Ltd/Plc from name |
| incorporation_date | drcip_register | registration_date | normalise to ISO 8601 |
| dissolution_date | not_available_in_open_sources | — | only implied via status; no explicit open field |
| registered_address | drcip_register | registered_address | free-text; parse municipality/district |
| activity_code | not_available_in_open_sources | — | no public NACE/activity code in Cyprus open data |
| financials | he32_financial_statements (paid PDF) / commercial_aggregators (paid) | financial_statements.* / company.financials[] | not open; EUR; planning-only |
| officers | drcip_register | officers[] | OPEN (directors/secretary); PII |
| owners | he32_financial_statements (shareholders, paid) / ubo_register (beneficial, restricted) | annual_return.shareholders[] / beneficial_owners[] | not open; PII; planning-only |
| source_provenance | (all) | source_provenance[] | per-section provenance with access/license |

## Cross-Country Notes

- Cyprus is a **partial-open** country: identity + **officers** are open (a distinctive — many registers do not
  expose officers openly), but **financials are paid + document-based (scanned PDF)** and ownership beyond
  officers is paid/restricted.
- A cross-country `owners` field is ambiguous for Cyprus — map it explicitly to **shareholders** (paid HE32) or
  **beneficial owners** (restricted UBO), never to the open **officers** list.
- `activity_code` and `dissolution_date` are genuinely absent from Cyprus open data; mark
  `not_available_in_open_sources` rather than inventing empty fields.
