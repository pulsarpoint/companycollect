# Norway — Common Field Mapping Suggestions

> **This file is only a suggestion for a future cross-country mapper.** It does NOT constrain
> the country-specific Norway profile (`country_company_profile.schema.json`), which remains the
> source of truth. These mappings show how Norway's Brreg fields could feed a hypothetical
> shared/global company schema later.

| Common field | Norway source | Norway path | Notes |
|---|---|---|---|
| company_id | brregenhet | organisasjonsnummer | 9-digit org number; national PK |
| registration_number | brregenhet | organisasjonsnummer | same as company_id in NO |
| tax_id | — | not_available_in_open_sources | No separate tax id is published; org number serves tax purposes |
| vat_id | brregenhet | derived: `NO`+organisasjonsnummer+`MVA` | only when registrertIMvaregisteret |
| legal_name | brregenhet | navn | current registered name (uppercase) |
| status | brregenhet | derived from konkurs/underAvvikling/underTvangsavvikling... | map to active/liquidation/compulsory_liquidation/bankrupt |
| legal_form | brregenhet | organisasjonsform.kode (+ beskrivelse) | AS, ASA, ENK, NUF, ... |
| incorporation_date | brregenhet | stiftelsesdato (fallback registreringsdatoEnhetsregisteret) | |
| dissolution_date | brregenhet | konkursdato / underAvviklingDato (bulk CSV) | only in CSV when set |
| registered_address | brregenhet | forretningsadresse (join adresse[] + postnummer + poststed) | postadresse is mailing |
| activity_code | brregenhet | naeringskode1.kode | SN2007 = NACE Rev.2 |
| financials | brregregnskap | financial_statements[] (revenue, net_result, total_assets, equity, debt; per year × type) | currency varies — keep valuta |
| officers | brregroller | officers[] (DAGL/STYR/REVI; person or enhet) | **PII/GDPR** — minimize |
| owners | — | not_available_in_open_sources | Beneficial ownership register is not open bulk |
| source_provenance | all | source_provenance[] | Brreg, NLOD 2.0 |

## Cross-country notes

- Norway's identifier model is simple: a single 9-digit `organisasjonsnummer` is the company id,
  registration number, and (with `NO…MVA`) the VAT id. A future mapper should not expect separate
  tax/registration numbers for NO.
- Norway provides **establishment-level** data (`underenheter`) that many countries don't — a
  global schema may need an optional `establishments[]`/sites concept to use it.
- Financial figures come with a **currency that is not always NOK** — any cross-country financial
  normalization must carry currency, never assume the country currency.
- `owners`/beneficial ownership is a common concept that is **not available** from Norwegian open
  sources; mark `not_available_in_open_sources`.
