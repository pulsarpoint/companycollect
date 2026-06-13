# Germany — Common Field Mapping Suggestions

> **Suggestion only.** This file proposes how Germany's country-specific profile *could* map onto a
> future cross-country company schema. It does **not** constrain
> `country_company_profile.schema.json`. The country-specific model is authoritative.

| Common field | Germany source path | Notes |
|---|---|---|
| company_id | registration.company_number | Synthetic OpenCorporates id; stable only within this dataset. |
| registration_number | registration.register_number (+ register_type + registrar) | **Not unique alone** — must carry court + type. Use `registration.natural_key` or `native_company_number` as the meaningful id. |
| tax_id | not_available_in_open_sources | German Steuernummer/USt-IdNr not in open data. |
| vat_id | not_available_in_open_sources | USt-IdNr not listed; EU VIES only *validates* a given number. |
| legal_name | legal_identity.name | Includes legal-form suffix. |
| status | status.derived (from status.current_status) | Open data has only free text; no clean enum/date. |
| legal_form | legal_identity.company_type | Derived from name suffix / register_type (GmbH, AG, UG, KG, e.V., eG, …). |
| incorporation_date | not_available_in_open_sources | Not a clean field in OffeneRegister. |
| dissolution_date | not_available_in_open_sources | Infer (unreliably) from status text only. |
| registered_address | registered_location.registered_address (+ municipality, federal_state) | Address is unparsed free text. |
| activity_code | not_available_in_open_sources | No NACE/WZ code in the open dataset. |
| financials | financial_statements[] | **Planning-only** — no open/bulk source; paid API or per-company tool. revenue/net_income present only for medium/large filers. |
| officers | officers[] | Present in open data (PII; GDPR). |
| owners | not_available_in_open_sources | Beneficial ownership (Transparenzregister) not modeled here. |
| source_provenance | source_provenance[] | Per-source provenance retained. |

## Cross-country notes for a future mapper

- **Germany has no single national company number** like Norway's `organisasjonsnummer`. The register
  number is **court-scoped** — any cross-country `registration_number` mapping must keep the court and
  register type, or it is ambiguous. Expect **no `tax_id`/`vat_id`** from open data.
- **Financials are not open** in Germany (unlike Norway's fully open Regnskapsregisteret). A cross-country
  `financials` mapper must tolerate Germany's `financial_statements[]` being **empty or paid-sourced**, and
  must tolerate `revenue`/`net_income` being NULL for the majority (small/micro) of companies.
- **Currency** must be stored per figure (usually EUR for Germany, but do not hardcode).
- **Spine staleness**: Germany's open spine is 2017-2019; a cross-country freshness field should mark this.
- **No activity code** in the German open data — `activity_code` is `not_available_in_open_sources`.
